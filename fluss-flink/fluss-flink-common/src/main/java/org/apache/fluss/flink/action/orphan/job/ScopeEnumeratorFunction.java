/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.flink.action.orphan.job;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.DisconnectException;
import org.apache.fluss.exception.NetworkException;
import org.apache.fluss.exception.UnsupportedVersionException;
import org.apache.fluss.flink.action.orphan.OrphanCleanUtils;
import org.apache.fluss.flink.action.orphan.RpcErrorClassifier;
import org.apache.fluss.flink.action.orphan.audit.AuditLogger;
import org.apache.fluss.flink.action.orphan.build.ActiveRefsFetcher;
import org.apache.fluss.flink.action.orphan.build.KvActiveRefsFetchResult;
import org.apache.fluss.flink.action.orphan.build.KvSharedSstFetchResult;
import org.apache.fluss.flink.action.orphan.build.LogActiveRefsFetchResult;
import org.apache.fluss.flink.action.orphan.build.MaxKnownIdsTracker;
import org.apache.fluss.flink.action.orphan.config.OrphanCleanConfig;
import org.apache.fluss.flink.action.orphan.rule.OrphanDirDetector;
import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.shaded.guava32.com.google.common.util.concurrent.RateLimiter;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.FlussPaths;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.apache.fluss.flink.action.orphan.OrphanCleanUtils.enumerateBuckets;
import static org.apache.fluss.flink.action.orphan.OrphanCleanUtils.fetchClusterConfigMap;
import static org.apache.fluss.flink.action.orphan.OrphanCleanUtils.normalizeRoot;
import static org.apache.fluss.flink.action.orphan.OrphanCleanUtils.physicalPath;
import static org.apache.fluss.flink.action.orphan.OrphanCleanUtils.remoteSubDir;
import static org.apache.fluss.flink.action.orphan.OrphanCleanUtils.resolveClusterRemoteDataDir;
import static org.apache.fluss.flink.action.orphan.OrphanCleanUtils.resolveClusterRemoteDataDirs;
import static org.apache.fluss.flink.action.orphan.OrphanCleanUtils.resolveRemoteDataDir;

/**
 * Stage 1 of the orphan files cleanup job. Runs at parallelism=1 and concentrates all coordinator
 * RPC interaction in a single subtask.
 *
 * <p>For each live bucket, emits a {@link BucketCleanTask} containing the FS paths and manifest
 * locations needed for Stage 2 to execute cleanup without coordinator access. For each detected
 * orphan directory, emits an {@link OrphanDirCleanTask}.
 */
@Internal
public final class ScopeEnumeratorFunction extends ProcessFunction<Integer, CleanTask> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ScopeEnumeratorFunction.class);
    private static final String[] TOP_LEVEL_DIRS = {
        FlussPaths.REMOTE_LOG_DIR_NAME, FlussPaths.REMOTE_KV_DIR_NAME
    };

    private final OrphanCleanConfig config;

    public ScopeEnumeratorFunction(OrphanCleanConfig config) {
        this.config = config;
    }

    @Override
    public void processElement(Integer trigger, Context ctx, Collector<CleanTask> out)
            throws Exception {
        if (!config.extraConfigs().isEmpty()) {
            FileSystem.initialize(Configuration.fromMap(config.extraConfigs()), null);
        }

        Configuration flussConfig = new Configuration();
        flussConfig.setString(ConfigOptions.BOOTSTRAP_SERVERS.key(), config.bootstrapServer());
        // Pass through client-related extra configs (e.g. security/auth).
        for (Map.Entry<String, String> entry : config.extraConfigs().entrySet()) {
            if (entry.getKey().startsWith("client.")) {
                flussConfig.setString(entry.getKey(), entry.getValue());
            }
        }

        try (Connection connection = ConnectionFactory.createConnection(flussConfig);
                Admin admin = connection.getAdmin()) {
            // Fail fast on incompatible servers: the action jar may be deployed against an
            // older cluster that does not implement ListRemoteLogManifests / ListKvSnapshots.
            // Without this guard, every per-target fetch would degrade to skip_log_target /
            // skip_kv_target audit events and the job would exit "successfully" with
            // deleted=0, masking the incompatibility.
            verifyServerSupportsRequiredApis(admin);

            AuditLogger audit = new AuditLogger();
            audit.logCutoff(config.olderThanMillis());

            RateLimiter remoteFsOpRateLimiter =
                    RateLimiter.create((double) config.remoteFsOpRateLimitPerSecond());
            ActiveRefsFetcher fetcher = new ActiveRefsFetcher(admin, 3, remoteFsOpRateLimiter);
            MaxKnownIdsTracker tracker = new MaxKnownIdsTracker();
            Map<String, String> clusterConfigMap = fetchClusterConfigMap(admin);
            String clusterRemoteDataDir = resolveClusterRemoteDataDir(clusterConfigMap);
            List<String> clusterRoots =
                    normalizeRoots(resolveClusterRemoteDataDirs(clusterConfigMap));

            Map<String, DbScanState> dbStates = enumerateActiveScope(admin, audit, tracker);

            for (DbScanState dbState : dbStates.values()) {
                for (LiveTableScope liveTable : dbState.liveTables) {
                    emitBucketTasks(
                            liveTable, fetcher, audit, clusterRemoteDataDir, clusterRoots, out);
                    emitOrphanPartitionDirTasks(
                            liveTable, tracker, clusterRoots, audit, remoteFsOpRateLimiter, out);
                }
                emitOrphanTableDirTasks(
                        dbState, tracker, clusterRoots, audit, remoteFsOpRateLimiter, out);
            }
        }
    }

    /** Normalizes each root in the list and returns a deduplicated ordered list. */
    private static List<String> normalizeRoots(List<String> roots) {
        LinkedHashSet<String> normalized = new LinkedHashSet<String>();
        for (String root : roots) {
            normalized.add(normalizeRoot(root));
        }
        return new ArrayList<String>(normalized);
    }

    /**
     * Probes the two RPCs this action depends on and throws if the connected server does not
     * implement them. A sentinel {@code tableId} of {@link Long#MAX_VALUE} is used so that on a
     * compatible server the call simply fails with a benign error (typically table-not-found),
     * whereas an incompatible server raises {@link UnsupportedVersionException} during ApiVersions
     * negotiation. Any non-{@code UnsupportedVersionException} outcome is treated as proof that the
     * RPC is recognized.
     */
    private static void verifyServerSupportsRequiredApis(Admin admin) {
        long sentinelTableId = Long.MAX_VALUE;
        probeApi(
                "ListRemoteLogManifests",
                () -> admin.listRemoteLogManifests(sentinelTableId, null).get());
        probeApi("ListKvSnapshots", () -> admin.listKvSnapshots(sentinelTableId, null).get());
    }

    private static void probeApi(String apiName, ThrowingProbe probe) {
        try {
            probe.run();
        } catch (Throwable t) {
            if (isUnsupportedVersion(t)) {
                throw new UnsupportedOperationException(
                        "Orphan files cleanup requires the Fluss server to support the "
                                + apiName
                                + " RPC, which the connected cluster does not. Upgrade the"
                                + " cluster to a version that exposes this RPC, or run an"
                                + " older orphan-files-cleanup action that targets this server.",
                        t);
            }
            if (isConnectionFailure(t)) {
                throw new IllegalStateException(
                        "Failed to connect to Fluss cluster while probing "
                                + apiName
                                + " RPC. The bootstrap server may be unreachable.",
                        t);
            }
            // Any other failure means the RPC is recognized; the call merely failed because of
            // the sentinel target id. Compatibility is satisfied.
        }
    }

    private static boolean isConnectionFailure(Throwable t) {
        Throwable cause = ExceptionUtils.stripExecutionException(t);
        while (cause != null) {
            if (cause instanceof NetworkException
                    || cause instanceof DisconnectException
                    || cause instanceof IOException) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    private static boolean isUnsupportedVersion(Throwable t) {
        Throwable cause = t;
        while (cause != null) {
            if (cause instanceof UnsupportedVersionException) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    @FunctionalInterface
    private interface ThrowingProbe {
        void run() throws Exception;
    }

    // -------------------------------------------------------------------------
    // Scope enumeration (coordinator RPCs only)
    // -------------------------------------------------------------------------

    private Map<String, DbScanState> enumerateActiveScope(
            Admin admin, AuditLogger audit, MaxKnownIdsTracker tracker) {
        List<String> dbs = resolveDatabasesToScan(admin, audit);
        Map<String, DbScanState> result = new LinkedHashMap<String, DbScanState>();
        for (String dbName : dbs) {
            DbScanState dbState = new DbScanState(dbName);
            result.put(dbName, dbState);
            if (config.table().isPresent()) {
                dbState.tableInfosComplete = false;
                resolveTable(admin, audit, tracker, dbState, config.table().get(), true);
                continue;
            }
            List<String> tableNames;
            try {
                tableNames = admin.listTables(dbName).get();
            } catch (Exception e) {
                audit.logSkipDb(dbName, classifyName(e));
                dbState.tableInfosComplete = false;
                continue;
            }
            for (String tableName : tableNames) {
                resolveTable(admin, audit, tracker, dbState, tableName, false);
            }
        }
        return result;
    }

    private List<String> resolveDatabasesToScan(Admin admin, AuditLogger audit) {
        if (config.allDatabases()) {
            try {
                return admin.listDatabases().get();
            } catch (Exception e) {
                audit.logSkipDb("*", classifyName(e));
                throw new IllegalStateException(
                        "Failed to list databases from Fluss cluster. "
                                + "The coordinator server may be unreachable.",
                        e);
            }
        }
        String databaseName = config.database().get();
        try {
            if (admin.databaseExists(databaseName).get()) {
                return Collections.singletonList(databaseName);
            }
        } catch (Exception e) {
            audit.logSkipDb(databaseName, classifyName(e));
            throw new IllegalStateException(
                    "Failed to check existence of database '"
                            + databaseName
                            + "'. "
                            + "The coordinator server may be unreachable.",
                    e);
        }
        audit.logSkipDb(databaseName, RpcErrorClassifier.Category.NOT_FOUND.name());
        return Collections.emptyList();
    }

    private void resolveTable(
            Admin admin,
            AuditLogger audit,
            MaxKnownIdsTracker tracker,
            DbScanState dbState,
            String tableName,
            boolean explicitTableTarget) {
        TablePath tablePath = TablePath.of(dbState.dbName, tableName);
        TableInfo tableInfo;
        try {
            tableInfo = admin.getTableInfo(tablePath).get();
        } catch (Exception e) {
            RpcErrorClassifier.Category category = RpcErrorClassifier.classify(e);
            if (category != RpcErrorClassifier.Category.NOT_FOUND || explicitTableTarget) {
                audit.logSkipTable(dbState.dbName, tableName, category.name());
                dbState.tableInfosComplete = false;
            }
            return;
        }
        tracker.observeTableId(tableInfo.getTableId());
        dbState.activeTableIds.add(tableInfo.getTableId());

        LiveTableScope liveTable = new LiveTableScope(dbState.dbName, tableName, tableInfo);
        dbState.liveTables.add(liveTable);
        if (!tableInfo.isPartitioned()) {
            return;
        }
        try {
            List<PartitionInfo> partitions = admin.listPartitionInfos(tablePath).get();
            TableInfo confirm = admin.getTableInfo(tablePath).get();
            if (confirm.getTableId() != tableInfo.getTableId()) {
                audit.logSkipTable(dbState.dbName, tableName, "table-recreated-during-enumeration");
                liveTable.partitionInfosComplete = false;
                return;
            }
            for (PartitionInfo partition : partitions) {
                liveTable.partitions.add(partition);
                liveTable.activePartitionIds.add(partition.getPartitionId());
                tracker.observePartitionId(partition.getPartitionId());
            }
        } catch (Exception e) {
            audit.logSkipPartitionList(dbState.dbName, tableName, classifyName(e));
            liveTable.partitionInfosComplete = false;
        }
    }

    // -------------------------------------------------------------------------
    // Emit BucketCleanTasks (per-target RPC + per-bucket task emission)
    // -------------------------------------------------------------------------

    private void emitBucketTasks(
            LiveTableScope liveTable,
            ActiveRefsFetcher fetcher,
            AuditLogger audit,
            @Nullable String clusterRemoteDataDir,
            List<String> clusterRoots,
            Collector<CleanTask> out) {
        if (liveTable.partitioned && !liveTable.partitionInfosComplete) {
            return;
        }
        List<PartitionInfo> partitionTargets =
                liveTable.partitioned
                        ? liveTable.partitions
                        : Collections.<PartitionInfo>singletonList(null);
        for (PartitionInfo partitionInfo : partitionTargets) {
            emitBucketTasksForTarget(
                    liveTable,
                    partitionInfo,
                    fetcher,
                    audit,
                    clusterRemoteDataDir,
                    clusterRoots,
                    out);
        }
    }

    private void emitBucketTasksForTarget(
            LiveTableScope liveTable,
            @Nullable PartitionInfo partitionInfo,
            ActiveRefsFetcher fetcher,
            AuditLogger audit,
            @Nullable String clusterRemoteDataDir,
            List<String> clusterRoots,
            Collector<CleanTask> out) {
        Long partitionId = partitionInfo == null ? null : partitionInfo.getPartitionId();

        String remoteDataDir =
                resolveRemoteDataDir(liveTable.tableInfo, partitionInfo, clusterRemoteDataDir);

        // Scope guard: skip this target if its metadata-resolved root is not part of the
        // cluster's configured remote data directories.
        if (!clusterRoots.contains(normalizeRoot(remoteDataDir))) {
            audit.logSkipBucketOutOfScope(liveTable.tableId, partitionId, remoteDataDir);
            return;
        }

        LogActiveRefsFetchResult logResult =
                fetcher.fetchLogActiveRefsByBucket(liveTable.tableId, partitionId);
        if (!logResult.listOk()) {
            audit.logSkipLogTarget(liveTable.tableId, partitionId, logResult.listFailureReason());
        }

        Map<Integer, Set<String>> kvActiveByBucket = Collections.emptyMap();
        boolean kvTargetOk = false;
        if (liveTable.tableInfo.hasPrimaryKey()) {
            KvActiveRefsFetchResult kvResult =
                    fetcher.fetchKvActiveSnapDirs(liveTable.tableId, partitionId);
            if (kvResult.listOk()) {
                kvActiveByBucket = kvResult.activeSnapDirsByBucket();
                kvTargetOk = true;
            } else {
                audit.logSkipKvTarget(liveTable.tableId, partitionId, kvResult.listFailureReason());
            }
        }

        FsPath remoteLogDir = remoteSubDir(remoteDataDir, FlussPaths.REMOTE_LOG_DIR_NAME);
        FsPath remoteKvDir = remoteSubDir(remoteDataDir, FlussPaths.REMOTE_KV_DIR_NAME);

        for (TableBucket tableBucket : enumerateBuckets(liveTable.tableInfo, partitionInfo)) {
            int bucketId = tableBucket.getBucket();

            String logTabletDir = null;

            Set<String> logSegmentRelativePaths = Collections.emptySet();
            Set<String> logActiveManifestPaths = Collections.emptySet();

            if (logResult.listOk()) {
                switch (logResult.statusFor(bucketId)) {
                    case RESOLVED:
                        logTabletDir =
                                FlussPaths.remoteLogTabletDir(
                                                remoteLogDir,
                                                physicalPath(liveTable.tablePath, partitionInfo),
                                                tableBucket)
                                        .toString();
                        logSegmentRelativePaths =
                                logResult.activeRefsOf(bucketId).logSegmentRelativePaths();
                        logActiveManifestPaths =
                                logResult.activeRefsOf(bucketId).logActiveManifestPaths();
                        break;
                    case READ_FAILED:
                        audit.logBucketAborted(
                                OrphanCleanUtils.bucketScopeKey(
                                        liveTable.tableId, partitionId, bucketId),
                                logResult.readFailureReason(bucketId));
                        break;
                    case NOT_LISTED:
                        audit.logSkipLogBucket(
                                liveTable.tableId, partitionId, bucketId, "no_remote_manifest");
                        break;
                    default:
                        break;
                }
            }

            String kvTabletDir = null;
            Set<String> kvActiveSnaps = Collections.emptySet();
            Set<String> kvSharedSstFileNames = Collections.emptySet();
            if (kvTargetOk && kvActiveByBucket.containsKey(bucketId)) {
                kvTabletDir =
                        FlussPaths.remoteKvTabletDir(
                                        remoteKvDir,
                                        physicalPath(liveTable.tablePath, partitionInfo),
                                        tableBucket)
                                .toString();
                kvActiveSnaps = kvActiveByBucket.get(bucketId);
                KvSharedSstFetchResult sstResult =
                        fetcher.fetchKvSharedSstFileNames(new FsPath(kvTabletDir), kvActiveSnaps);
                if (sstResult.allMetadataReadOk()) {
                    kvSharedSstFileNames = sstResult.sharedSstFileNames();
                } else {
                    audit.logSkipKvSharedSst(
                            liveTable.tableId, partitionId, bucketId, sstResult.failureReason());
                }
            } else if (kvTargetOk) {
                audit.logSkipKvBucket(liveTable.tableId, partitionId, bucketId, "empty_active_set");
            }

            if (logTabletDir == null && kvTabletDir == null) {
                continue;
            }

            out.collect(
                    new BucketCleanTask(
                            logTabletDir,
                            kvTabletDir,
                            logSegmentRelativePaths,
                            logActiveManifestPaths,
                            kvActiveSnaps,
                            kvSharedSstFileNames,
                            config.olderThanMillis(),
                            config.dryRun(),
                            config.allowDeleteManifest()));
        }
    }

    // -------------------------------------------------------------------------
    // Emit OrphanDirCleanTasks
    // -------------------------------------------------------------------------

    private void emitOrphanTableDirTasks(
            DbScanState dbState,
            MaxKnownIdsTracker tracker,
            List<String> clusterRoots,
            AuditLogger audit,
            RateLimiter remoteFsOpRateLimiter,
            Collector<CleanTask> out)
            throws IOException {
        if (!dbState.tableInfosComplete) {
            audit.logSkipOrphanTableScan(dbState.dbName, "tableInfos-incomplete");
            return;
        }
        Set<Long> activeTableIds = dbState.activeTableIds;
        long maxKnownTableId = tracker.maxKnownTableId();
        boolean emit = config.allowCleanOrphanTables();
        for (String root : clusterRoots) {
            for (String topLevel : TOP_LEVEL_DIRS) {
                FsPath dbDir = remoteSubDir(root, topLevel + "/" + dbState.dbName);
                if (emit) {
                    forEachOrphanDirUnderParent(
                            dbDir,
                            dirName ->
                                    OrphanDirDetector.isOrphanTable(
                                            dirName, activeTableIds, maxKnownTableId),
                            remoteFsOpRateLimiter,
                            dir ->
                                    out.collect(
                                            new OrphanDirCleanTask(
                                                    dir.toString(),
                                                    config.olderThanMillis(),
                                                    config.dryRun(),
                                                    config.allowDeleteManifest())));
                } else {
                    forEachOrphanDirUnderParent(
                            dbDir,
                            dirName ->
                                    OrphanDirDetector.isOrphanTable(
                                            dirName, activeTableIds, maxKnownTableId),
                            remoteFsOpRateLimiter,
                            dir -> audit.logSkipOrphanTable(dir, "default-conservative"));
                }
            }
        }
    }

    private void emitOrphanPartitionDirTasks(
            LiveTableScope liveTable,
            MaxKnownIdsTracker tracker,
            List<String> clusterRoots,
            AuditLogger audit,
            RateLimiter remoteFsOpRateLimiter,
            Collector<CleanTask> out)
            throws IOException {
        if (!liveTable.partitioned || !liveTable.partitionInfosComplete) {
            return;
        }
        Set<Long> activePartitionIds = liveTable.activePartitionIds;
        long maxKnownPartitionId = tracker.maxKnownPartitionId();
        boolean emit = config.allowCleanOrphanPartitions();
        for (String root : clusterRoots) {
            for (String topLevel : TOP_LEVEL_DIRS) {
                FsPath tableDir =
                        FlussPaths.remoteTableDir(
                                remoteSubDir(root, topLevel),
                                liveTable.tablePath,
                                liveTable.tableId);
                if (emit) {
                    forEachOrphanDirUnderParent(
                            tableDir,
                            dirName ->
                                    OrphanDirDetector.isOrphanPartition(
                                            dirName, activePartitionIds, maxKnownPartitionId),
                            remoteFsOpRateLimiter,
                            dir ->
                                    out.collect(
                                            new OrphanDirCleanTask(
                                                    dir.toString(),
                                                    config.olderThanMillis(),
                                                    config.dryRun(),
                                                    config.allowDeleteManifest())));
                } else {
                    forEachOrphanDirUnderParent(
                            tableDir,
                            dirName ->
                                    OrphanDirDetector.isOrphanPartition(
                                            dirName, activePartitionIds, maxKnownPartitionId),
                            remoteFsOpRateLimiter,
                            dir -> audit.logSkipOrphanPartition(dir, "default-conservative"));
                }
            }
        }
    }

    private void forEachOrphanDirUnderParent(
            FsPath parentDir,
            Predicate<String> isOrphan,
            RateLimiter remoteFsOpRateLimiter,
            Consumer<FsPath> action)
            throws IOException {
        FileSystem fs = getFileSystemIfExists(parentDir, remoteFsOpRateLimiter);
        if (fs == null) {
            return;
        }
        FileStatus[] entries = listStatuses(fs, parentDir, remoteFsOpRateLimiter);
        if (entries == null) {
            return;
        }
        for (FileStatus entry : entries) {
            if (!entry.isDir()) {
                continue;
            }
            if (!isOrphan.test(entry.getPath().getName())) {
                continue;
            }
            action.accept(entry.getPath());
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static String classifyName(Throwable e) {
        return RpcErrorClassifier.classify(e).name();
    }

    @Nullable
    private static FileSystem getFileSystemIfExists(FsPath dir, RateLimiter remoteFsOpRateLimiter)
            throws IOException {
        FileSystem fs = dir.getFileSystem();
        remoteFsOpRateLimiter.acquire();
        return fs.exists(dir) ? fs : null;
    }

    @Nullable
    private static FileStatus[] listStatuses(
            FileSystem fs, FsPath dir, RateLimiter remoteFsOpRateLimiter) {
        try {
            remoteFsOpRateLimiter.acquire();
            return fs.listStatus(dir);
        } catch (IOException e) {
            LOG.warn("Failed to list directory: {}", dir, e);
            return null;
        }
    }

    // -------------------------------------------------------------------------
    // Internal state classes
    // -------------------------------------------------------------------------

    private static final class DbScanState {
        final String dbName;
        boolean tableInfosComplete = true;
        final Set<Long> activeTableIds = new LinkedHashSet<Long>();
        final List<LiveTableScope> liveTables = new ArrayList<LiveTableScope>();

        DbScanState(String dbName) {
            this.dbName = dbName;
        }
    }

    private static final class LiveTableScope {
        final String dbName;
        final String tableName;
        final TablePath tablePath;
        final long tableId;
        final TableInfo tableInfo;
        final boolean partitioned;
        boolean partitionInfosComplete = true;
        final List<PartitionInfo> partitions = new ArrayList<PartitionInfo>();
        final Set<Long> activePartitionIds = new LinkedHashSet<Long>();

        LiveTableScope(String dbName, String tableName, TableInfo tableInfo) {
            this.dbName = dbName;
            this.tableName = tableName;
            this.tablePath = tableInfo.getTablePath();
            this.tableId = tableInfo.getTableId();
            this.tableInfo = tableInfo;
            this.partitioned = tableInfo.isPartitioned();
        }
    }
}
