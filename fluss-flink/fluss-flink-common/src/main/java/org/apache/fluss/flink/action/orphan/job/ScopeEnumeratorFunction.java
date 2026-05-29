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
import org.apache.fluss.flink.action.orphan.OrphanCleanUtils;
import org.apache.fluss.flink.action.orphan.RpcErrorClassifier;
import org.apache.fluss.flink.action.orphan.audit.AuditLogger;
import org.apache.fluss.flink.action.orphan.build.ActiveRefsFetcher;
import org.apache.fluss.flink.action.orphan.build.KvActiveRefsFetchResult;
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
import java.util.function.Predicate;

import static org.apache.fluss.flink.action.orphan.OrphanCleanUtils.enumerateBuckets;
import static org.apache.fluss.flink.action.orphan.OrphanCleanUtils.getFileSystemIfExists;
import static org.apache.fluss.flink.action.orphan.OrphanCleanUtils.listStatuses;
import static org.apache.fluss.flink.action.orphan.OrphanCleanUtils.physicalPath;
import static org.apache.fluss.flink.action.orphan.OrphanCleanUtils.remoteSubDir;
import static org.apache.fluss.flink.action.orphan.OrphanCleanUtils.resolveClusterRemoteDataDir;
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

        try (Connection connection = ConnectionFactory.createConnection(flussConfig);
                Admin admin = connection.getAdmin()) {
            AuditLogger audit = new AuditLogger();
            audit.logCutoff(config.olderThanMillis());

            ActiveRefsFetcher fetcher = new ActiveRefsFetcher(admin, 3);
            MaxKnownIdsTracker tracker = new MaxKnownIdsTracker();
            String clusterRemoteDataDir = resolveClusterRemoteDataDir(admin);

            Map<String, DbScanState> dbStates = enumerateActiveScope(admin, audit, tracker);

            for (DbScanState dbState : dbStates.values()) {
                for (LiveTableScope liveTable : dbState.liveTables) {
                    emitBucketTasks(liveTable, fetcher, audit, clusterRemoteDataDir, out);
                    emitOrphanPartitionDirTasks(
                            liveTable, tracker, clusterRemoteDataDir, audit, out);
                }
                emitOrphanTableDirTasks(dbState, tracker, clusterRemoteDataDir, audit, out);
            }
        }
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
                return Collections.emptyList();
            }
        }
        String databaseName = config.database().get();
        try {
            if (admin.databaseExists(databaseName).get()) {
                return Collections.singletonList(databaseName);
            }
        } catch (Exception e) {
            audit.logSkipDb(databaseName, classifyName(e));
            return Collections.emptyList();
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
                audit.logSkipTable(dbState.dbName, tableName, "ABA");
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
                    liveTable, partitionInfo, fetcher, audit, clusterRemoteDataDir, out);
        }
    }

    private void emitBucketTasksForTarget(
            LiveTableScope liveTable,
            @Nullable PartitionInfo partitionInfo,
            ActiveRefsFetcher fetcher,
            AuditLogger audit,
            @Nullable String clusterRemoteDataDir,
            Collector<CleanTask> out) {
        Long partitionId = partitionInfo == null ? null : partitionInfo.getPartitionId();

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

        String remoteDataDir =
                resolveRemoteDataDir(liveTable.tableInfo, partitionInfo, clusterRemoteDataDir);
        if (remoteDataDir == null) {
            LOG.warn(
                    "Table {} partition {} has no resolvable remote.data.dir; skipping",
                    liveTable.tablePath,
                    partitionId);
            return;
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
            if (kvTargetOk && kvActiveByBucket.containsKey(bucketId)) {
                kvTabletDir =
                        FlussPaths.remoteKvTabletDir(
                                        remoteKvDir,
                                        physicalPath(liveTable.tablePath, partitionInfo),
                                        tableBucket)
                                .toString();
                kvActiveSnaps = kvActiveByBucket.get(bucketId);
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
            @Nullable String clusterRemoteDataDir,
            AuditLogger audit,
            Collector<CleanTask> out)
            throws IOException {
        if (!dbState.tableInfosComplete) {
            audit.logSkipOrphanTableScan(dbState.dbName, "tableInfos-incomplete");
            return;
        }
        Set<Long> activeTableIds = dbState.activeTableIds;
        long maxKnownTableId = tracker.maxKnownTableId();
        boolean emit = config.allowCleanOrphanTables();
        for (String root : rootsToScan(clusterRemoteDataDir)) {
            for (String topLevel : TOP_LEVEL_DIRS) {
                FsPath dbDir = remoteSubDir(root, topLevel + "/" + dbState.dbName);
                if (emit) {
                    emitOrphanDirsUnderParent(
                            dbDir,
                            dirName ->
                                    OrphanDirDetector.isOrphanTable(
                                            dirName, activeTableIds, maxKnownTableId),
                            out);
                } else {
                    logSkippedOrphanDirsUnderParent(
                            dbDir,
                            dirName ->
                                    OrphanDirDetector.isOrphanTable(
                                            dirName, activeTableIds, maxKnownTableId),
                            audit);
                }
            }
        }
    }

    private void emitOrphanPartitionDirTasks(
            LiveTableScope liveTable,
            MaxKnownIdsTracker tracker,
            @Nullable String clusterRemoteDataDir,
            AuditLogger audit,
            Collector<CleanTask> out)
            throws IOException {
        if (!liveTable.partitioned || !liveTable.partitionInfosComplete) {
            return;
        }
        Set<Long> activePartitionIds = liveTable.activePartitionIds;
        long maxKnownPartitionId = tracker.maxKnownPartitionId();
        boolean emit = config.allowCleanOrphanPartitions();
        for (String root : rootsForLiveTable(liveTable, clusterRemoteDataDir)) {
            for (String topLevel : TOP_LEVEL_DIRS) {
                FsPath tableDir =
                        FlussPaths.remoteTableDir(
                                remoteSubDir(root, topLevel),
                                liveTable.tablePath,
                                liveTable.tableId);
                if (emit) {
                    emitOrphanDirsUnderParent(
                            tableDir,
                            dirName ->
                                    OrphanDirDetector.isOrphanPartition(
                                            dirName, activePartitionIds, maxKnownPartitionId),
                            out);
                } else {
                    logSkippedOrphanPartitionDirsUnderParent(
                            tableDir,
                            dirName ->
                                    OrphanDirDetector.isOrphanPartition(
                                            dirName, activePartitionIds, maxKnownPartitionId),
                            audit);
                }
            }
        }
    }

    private void emitOrphanDirsUnderParent(
            FsPath parentDir, Predicate<String> isOrphan, Collector<CleanTask> out)
            throws IOException {
        FileSystem fs = getFileSystemIfExists(parentDir);
        if (fs == null) {
            return;
        }
        FileStatus[] entries = listStatuses(fs, parentDir);
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
            out.collect(
                    new OrphanDirCleanTask(
                            entry.getPath().toString(),
                            config.olderThanMillis(),
                            config.dryRun(),
                            config.allowDeleteManifest()));
        }
    }

    private void logSkippedOrphanDirsUnderParent(
            FsPath parentDir, Predicate<String> isOrphan, AuditLogger audit) throws IOException {
        FileSystem fs = getFileSystemIfExists(parentDir);
        if (fs == null) {
            return;
        }
        FileStatus[] entries = listStatuses(fs, parentDir);
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
            audit.logSkipOrphanTable(entry.getPath(), "default-conservative");
        }
    }

    private void logSkippedOrphanPartitionDirsUnderParent(
            FsPath parentDir, Predicate<String> isOrphan, AuditLogger audit) throws IOException {
        FileSystem fs = getFileSystemIfExists(parentDir);
        if (fs == null) {
            return;
        }
        FileStatus[] entries = listStatuses(fs, parentDir);
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
            audit.logSkipOrphanPartition(entry.getPath(), "default-conservative");
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private List<String> rootsToScan(@Nullable String clusterRemoteDataDir) {
        LinkedHashSet<String> roots = new LinkedHashSet<String>();
        if (clusterRemoteDataDir != null) {
            roots.add(clusterRemoteDataDir);
        }
        roots.addAll(config.scanRoots());
        return new ArrayList<String>(roots);
    }

    private List<String> rootsForLiveTable(
            LiveTableScope liveTable, @Nullable String clusterRemoteDataDir) {
        LinkedHashSet<String> roots = new LinkedHashSet<String>(rootsToScan(clusterRemoteDataDir));
        String tableRoot = resolveRemoteDataDir(liveTable.tableInfo, null, clusterRemoteDataDir);
        if (tableRoot != null) {
            roots.add(tableRoot);
        }
        for (PartitionInfo partitionInfo : liveTable.partitions) {
            String partitionRoot =
                    resolveRemoteDataDir(liveTable.tableInfo, partitionInfo, clusterRemoteDataDir);
            if (partitionRoot != null) {
                roots.add(partitionRoot);
            }
        }
        return new ArrayList<String>(roots);
    }

    private static String classifyName(Throwable e) {
        return RpcErrorClassifier.classify(e).name();
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
