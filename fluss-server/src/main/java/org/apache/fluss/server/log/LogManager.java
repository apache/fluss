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

package org.apache.fluss.server.log;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussException;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.LogStorageException;
import org.apache.fluss.exception.SchemaNotExistException;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.TabletManagerBase;
import org.apache.fluss.server.log.checkpoint.OffsetCheckpointFile;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.server.storage.LocalDiskManager;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.concurrent.Scheduler;
import org.apache.fluss.utils.types.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static org.apache.fluss.utils.concurrent.LockUtils.inLock;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * The entry point to the fluss log management subsystem. The log manager is responsible for log
 * creation, retrieval, and cleaning. All read and write operations are delegated to the individual
 * log instances.
 */
@ThreadSafe
public final class LogManager extends TabletManagerBase {
    private static final Logger LOG = LoggerFactory.getLogger(LogManager.class);

    @VisibleForTesting
    static final String RECOVERY_POINT_CHECKPOINT_FILE = "recovery-point-offset-checkpoint";

    /**
     * Clean shutdown file that indicates the tabletServer was cleanly shutdown in v0.7 and higher.
     * This is used to avoid unnecessary recovery operations after a clean shutdown like recovery
     * writer snapshot by scan all logs during loadLogs.
     *
     * <p>Note: for the previous cluster deploy by v0.6 and lower, there's no this file, we default
     * think its unclean shutdown.
     */
    static final String CLEAN_SHUTDOWN_FILE = ".fluss_cleanshutdown";

    private final ZooKeeperClient zkClient;
    private final Scheduler scheduler;
    private final Clock clock;
    private final TabletServerMetricGroup serverMetricGroup;
    private final LocalDiskManager localDiskManager;
    private final ReentrantLock logCreationOrDeletionLock = new ReentrantLock();

    private final Map<TableBucket, LogTablet> currentLogs = new ConcurrentHashMap<>();

    private volatile Map<File, OffsetCheckpointFile> recoveryPointCheckpoints;
    private boolean loadLogsCompletedFlag = false;

    private LogManager(
            LocalDiskManager localDiskManager,
            Configuration conf,
            ZooKeeperClient zkClient,
            int recoveryThreadsPerDataDir,
            Scheduler scheduler,
            Clock clock,
            TabletServerMetricGroup serverMetricGroup)
            throws Exception {
        super(TabletType.LOG, localDiskManager.dataDirs(), conf, recoveryThreadsPerDataDir);
        this.zkClient = zkClient;
        this.scheduler = scheduler;
        this.clock = clock;
        this.serverMetricGroup = serverMetricGroup;
        this.localDiskManager = localDiskManager;

        initializeCheckpointMaps();
    }

    public static LogManager create(
            Configuration conf,
            ZooKeeperClient zkClient,
            Scheduler scheduler,
            Clock clock,
            TabletServerMetricGroup serverMetricGroup,
            LocalDiskManager localDiskManager)
            throws Exception {
        return new LogManager(
                localDiskManager,
                conf,
                zkClient,
                conf.getInt(ConfigOptions.NETTY_SERVER_NUM_WORKER_THREADS),
                scheduler,
                clock,
                serverMetricGroup);
    }

    public void startup() {
        loadAllLogs();

        // TODO add more scheduler, like log-flusher etc.
    }

    private void initializeCheckpointMaps() throws IOException {
        recoveryPointCheckpoints = new HashMap<>();
        for (File dataDir : dataDirs) {
            recoveryPointCheckpoints.put(
                    dataDir,
                    new OffsetCheckpointFile(new File(dataDir, RECOVERY_POINT_CHECKPOINT_FILE)));
        }
    }

    /** Recover and load all logs in the given data directories. */
    private void loadAllLogs() {
        try {
            Map<File, List<File>> tabletsToLoadByDataDir = listTabletsToLoad();
            LOG.info("Loading logs from {} dirs", tabletsToLoadByDataDir.size());
            List<LogRecoveryTask> recoveryTasks = new ArrayList<>();
            for (Map.Entry<File, List<File>> entry : tabletsToLoadByDataDir.entrySet()) {
                recoveryTasks.add(loadLogsInDir(entry.getKey(), entry.getValue()));
            }
            try {
                for (LogRecoveryTask recoveryTask : recoveryTasks) {
                    waitForLoadLogsInDir(recoveryTask);
                }
                loadLogsCompletedFlag = true;
                LOG.info("Log loader complete.");
            } finally {
                for (LogRecoveryTask recoveryTask : recoveryTasks) {
                    recoveryTask.pool.shutdown();
                }
            }
        } catch (Throwable e) {
            throw new FlussRuntimeException("Failed to recover logs", e);
        }
    }

    private LogRecoveryTask loadLogsInDir(File dataDir, List<File> tabletsToLoad) throws Exception {
        LOG.info("Loading logs from dir {}", dataDir);

        String dataDirAbsolutePath = dataDir.getAbsolutePath();
        boolean isCleanShutdown = false;
        File cleanShutdownFile = new File(dataDir, CLEAN_SHUTDOWN_FILE);
        if (cleanShutdownFile.exists()) {
            // Cache the clean shutdown status marker and use that for rest of log loading
            // workflow. Delete the CleanShutdownFile so that if tabletServer crashes while
            // loading the log, it is considered hard shutdown during the next boot up.
            Files.deleteIfExists(cleanShutdownFile.toPath());
            isCleanShutdown = true;
        }

        Map<TableBucket, Long> recoveryPoints = new HashMap<>();
        try {
            recoveryPoints = recoveryPointCheckpoints.get(dataDir).read();
        } catch (Exception e) {
            LOG.warn(
                    "Error occurred while reading recovery-point-offset-checkpoint file of directory {}, "
                            + "resetting the recovery checkpoint to 0",
                    dataDirAbsolutePath,
                    e);
        }

        if (tabletsToLoad.isEmpty()) {
            LOG.info("No logs found to be loaded in {}", dataDirAbsolutePath);
        } else if (isCleanShutdown) {
            LOG.info("Skipping some recovery log process since clean shutdown file was found");
        } else {
            LOG.info("Recovering all local logs since no clean shutdown file was found");
        }

        final Map<TableBucket, Long> finalRecoveryPoints = recoveryPoints;
        final boolean cleanShutdown = isCleanShutdown;
        // set runnable job.
        Runnable[] jobsForDir =
                createLogLoadingJobs(
                        dataDir, tabletsToLoad, cleanShutdown, finalRecoveryPoints, conf, clock);

        long startTimeMillis = System.currentTimeMillis();
        List<Future<?>> jobsForDataDir = new ArrayList<>();
        ExecutorService pool = createThreadPool("log-recovery-" + dataDirAbsolutePath);
        for (Runnable job : jobsForDir) {
            jobsForDataDir.add(pool.submit(job));
        }
        return new LogRecoveryTask(dataDir, pool, jobsForDataDir, startTimeMillis);
    }

    private void waitForLoadLogsInDir(LogRecoveryTask recoveryTask) throws Throwable {
        int successCount = 0;
        for (Future<?> future : recoveryTask.jobs) {
            try {
                future.get();
                successCount++;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new FlussRuntimeException(
                        "Interrupted while waiting for log recovery tasks to finish.", e);
            } catch (ExecutionException e) {
                throw new FlussException(
                        "Failed while waiting for log recovery tasks to finish.",
                        ExceptionUtils.stripExecutionException(e));
            }
        }

        LOG.info(
                "Log loader complete for {}. Total success loaded log count is {}, Take {} ms",
                recoveryTask.dataDir.getAbsolutePath(),
                successCount,
                System.currentTimeMillis() - recoveryTask.startTimeMillis);
    }

    /**
     * Get or create log tablet for a given bucket of a table. If the log already exists, just
     * return a copy of the existing log. Otherwise, create a log for the given table and the given
     * bucket.
     *
     * @param dataDir the local data directory chosen for the bucket
     * @param tablePath the table path of the bucket belongs to
     * @param tableBucket the table bucket
     * @param logFormat the log format
     * @param tieredLogLocalSegments the number of segments to retain in local for tiered log
     * @param isChangelog whether the log is a changelog of primary key table
     */
    public LogTablet getOrCreateLog(
            File dataDir,
            PhysicalTablePath tablePath,
            TableBucket tableBucket,
            LogFormat logFormat,
            int tieredLogLocalSegments,
            boolean isChangelog)
            throws Exception {
        return inLock(
                logCreationOrDeletionLock,
                () -> {
                    if (currentLogs.containsKey(tableBucket)) {
                        return currentLogs.get(tableBucket);
                    }

                    File tabletDir = getOrCreateTabletDir(dataDir, tablePath, tableBucket);

                    LogTablet logTablet =
                            LogTablet.create(
                                    dataDir,
                                    tablePath,
                                    tabletDir,
                                    conf,
                                    serverMetricGroup,
                                    0L,
                                    scheduler,
                                    logFormat,
                                    tieredLogLocalSegments,
                                    isChangelog,
                                    clock,
                                    true);
                    currentLogs.put(tableBucket, logTablet);

                    LOG.info(
                            "Loaded log for bucket {} in dir {}",
                            tableBucket,
                            tabletDir.getAbsolutePath());

                    return logTablet;
                });
    }

    public Optional<LogTablet> getLog(TableBucket tableBucket) {
        return Optional.ofNullable(currentLogs.get(tableBucket));
    }

    public void dropLog(TableBucket tableBucket) {
        LogTablet dropLogTablet =
                inLock(logCreationOrDeletionLock, () -> currentLogs.remove(tableBucket));

        if (dropLogTablet != null) {
            TablePath tablePath = dropLogTablet.getTablePath();
            try {
                dropLogTablet.drop();
                if (dropLogTablet.getPartitionName() == null) {
                    LOG.info(
                            "Deleted log bucket {} for table {} in file path {}.",
                            tableBucket.getBucket(),
                            tablePath,
                            dropLogTablet.getLogDir().getAbsolutePath());
                } else {
                    LOG.info(
                            "Deleted log bucket {} for the partition {} of table {} in file path {}.",
                            tableBucket.getBucket(),
                            dropLogTablet.getPartitionName(),
                            tablePath,
                            dropLogTablet.getLogDir().getAbsolutePath());
                }
            } catch (Exception e) {
                throw new LogStorageException(
                        String.format(
                                "Error while deleting log for table %s, bucket %s in dir %s: %s",
                                tablePath,
                                tableBucket.getBucket(),
                                dropLogTablet.getLogDir().getAbsolutePath(),
                                e.getMessage()),
                        e);
            }
        } else {
            throw new LogStorageException(
                    String.format(
                            "Failed to delete log bucket %s as it does not exist.",
                            tableBucket.getBucket()));
        }
    }

    /**
     * Truncate the bucket's logs to the specified offsets and checkpoint the recovery point to this
     * offset.
     */
    public void truncateTo(TableBucket tableBucket, long offset) throws LogStorageException {
        LogTablet logTablet = currentLogs.get(tableBucket);
        // If the log tablet does not exist, skip it.
        if (logTablet != null && logTablet.truncateTo(offset)) {
            checkpointRecoveryOffsets(logTablet.getDataDir());
        }
    }

    public void truncateFullyAndStartAt(TableBucket tableBucket, long newOffset) {
        LogTablet logTablet = currentLogs.get(tableBucket);
        // If the log tablet does not exist, skip it.
        if (logTablet != null) {
            logTablet.truncateFullyAndStartAt(newOffset);
            checkpointRecoveryOffsets(logTablet.getDataDir());
        }
    }

    private LogTablet loadLog(
            File dataDir,
            File tabletDir,
            boolean isCleanShutdown,
            Map<TableBucket, Long> recoveryPoints,
            Configuration conf,
            Clock clock)
            throws Exception {
        Tuple2<PhysicalTablePath, TableBucket> pathAndBucket = FlussPaths.parseTabletDir(tabletDir);
        TableBucket tableBucket = pathAndBucket.f1;
        long logRecoveryPoint = recoveryPoints.getOrDefault(tableBucket, 0L);

        PhysicalTablePath physicalTablePath = pathAndBucket.f0;
        TablePath tablePath = physicalTablePath.getTablePath();
        TableInfo tableInfo = getTableInfo(zkClient, tablePath);
        LogTablet logTablet =
                LogTablet.create(
                        dataDir,
                        physicalTablePath,
                        tabletDir,
                        conf,
                        serverMetricGroup,
                        logRecoveryPoint,
                        scheduler,
                        tableInfo.getTableConfig().getLogFormat(),
                        tableInfo.getTableConfig().getTieredLogLocalSegments(),
                        tableInfo.hasPrimaryKey(),
                        clock,
                        isCleanShutdown);
        logTablet.updateIsDataLakeEnabled(tableInfo.getTableConfig().isDataLakeEnabled());

        if (currentLogs.containsKey(tableBucket)) {
            throw new IllegalStateException(
                    String.format(
                            "Duplicate log tablet directories for bucket %s are found in both %s and %s. "
                                    + "It is likely because tablet directory failure happened while server was "
                                    + "replacing current replica with future replica. Recover server from this "
                                    + "failure by manually deleting one of the two log directories for this bucket. "
                                    + "It is recommended to delete the bucket in the log tablet directory that is "
                                    + "known to have failed recently.",
                            tableBucket,
                            tabletDir.getAbsolutePath(),
                            currentLogs.get(tableBucket).getLogDir().getAbsolutePath()));
        }
        currentLogs.put(tableBucket, logTablet);
        localDiskManager.recordReplicaLoad(dataDir, tableInfo.hasPrimaryKey());

        return logTablet;
    }

    /** Close all the logs. */
    public void shutdown() {
        LOG.info("Shutting down LogManager.");

        Map<File, List<LogTablet>> logsByDataDir = new LinkedHashMap<>();
        for (File dataDir : dataDirs) {
            logsByDataDir.put(dataDir, new ArrayList<>());
        }
        for (LogTablet logTablet : currentLogs.values()) {
            File dataDir = logTablet.getDataDir();
            logsByDataDir.computeIfAbsent(dataDir, ignored -> new ArrayList<>()).add(logTablet);
        }

        List<LogShutdownTask> shutdownTasks = new ArrayList<>();
        for (Map.Entry<File, List<LogTablet>> entry : logsByDataDir.entrySet()) {
            shutdownTasks.add(shutdownLogsInDir(entry.getKey(), entry.getValue()));
        }

        try {
            for (LogShutdownTask shutdownTask : shutdownTasks) {
                waitForShutdownLogsInDir(shutdownTask);
            }
        } finally {
            for (LogShutdownTask shutdownTask : shutdownTasks) {
                shutdownTask.pool.shutdown();
            }
        }

        LOG.info("Shut down LogManager complete.");
    }

    private LogShutdownTask shutdownLogsInDir(File dataDir, List<LogTablet> logs) {
        String dataDirAbsolutePath = dataDir.getAbsolutePath();
        LOG.info("Shutting down {} logs in dir {}", logs.size(), dataDirAbsolutePath);

        List<Future<?>> jobsForTabletDir = new ArrayList<>();
        ExecutorService pool = createThreadPool("log-tablet-closing-" + dataDirAbsolutePath);
        for (LogTablet logTablet : logs) {
            Runnable runnable =
                    () -> {
                        try {
                            logTablet.flush(true);
                            logTablet.close();
                        } catch (IOException e) {
                            throw new FlussRuntimeException(e);
                        }
                    };
            jobsForTabletDir.add(pool.submit(runnable));
        }
        return new LogShutdownTask(dataDir, logs, pool, jobsForTabletDir);
    }

    private void waitForShutdownLogsInDir(LogShutdownTask shutdownTask) {
        for (Future<?> future : shutdownTask.jobs) {
            try {
                future.get();
            } catch (InterruptedException e) {
                LOG.warn("Interrupted while shutting down LogManager.");
            } catch (ExecutionException e) {
                LOG.warn("There was an error in one of the threads during LogManager shutdown", e);
            }
        }

        checkpointRecoveryOffsets(shutdownTask.dataDir, shutdownTask.logs);

        // mark that the shutdown was clean by creating marker file for log dirs that all logs have
        // been recovered at startup time.
        if (loadLogsCompletedFlag) {
            try {
                LOG.debug("Writing clean shutdown marker for directory {}.", shutdownTask.dataDir);
                Files.createFile(new File(shutdownTask.dataDir, CLEAN_SHUTDOWN_FILE).toPath());
            } catch (IOException e) {
                LOG.warn(
                        "Failed to write clean shutdown marker for directory {}.",
                        shutdownTask.dataDir,
                        e);
            }
        }
    }

    /** Create runnable jobs for loading logs from tablet directories. */
    private Runnable[] createLogLoadingJobs(
            File dataDir,
            List<File> tabletsToLoad,
            boolean cleanShutdown,
            Map<TableBucket, Long> recoveryPoints,
            Configuration conf,
            Clock clock) {
        Runnable[] jobs = new Runnable[tabletsToLoad.size()];
        for (int i = 0; i < tabletsToLoad.size(); i++) {
            final File tabletDir = tabletsToLoad.get(i);
            jobs[i] =
                    createLogLoadingJob(
                            dataDir, tabletDir, cleanShutdown, recoveryPoints, conf, clock);
        }
        return jobs;
    }

    /** Create a runnable job for loading log from a single tablet directory. */
    private Runnable createLogLoadingJob(
            File dataDir,
            File tabletDir,
            boolean cleanShutdown,
            Map<TableBucket, Long> recoveryPoints,
            Configuration conf,
            Clock clock) {
        return new Runnable() {
            @Override
            public void run() {
                LOG.debug("Loading log {}", tabletDir);
                try {
                    loadLog(dataDir, tabletDir, cleanShutdown, recoveryPoints, conf, clock);
                } catch (Exception e) {
                    LOG.error("Fail to loadLog from {}", tabletDir, e);
                    if (e instanceof SchemaNotExistException) {
                        LOG.error(
                                "schema not exist, table for {} has already been dropped, the residual data will be removed.",
                                tabletDir,
                                e);
                        FileUtils.deleteDirectoryQuietly(tabletDir);

                        // Also delete corresponding KV tablet directory if it exists
                        try {
                            Tuple2<PhysicalTablePath, TableBucket> pathAndBucket =
                                    FlussPaths.parseTabletDir(tabletDir);
                            File kvTabletDir =
                                    FlussPaths.kvTabletDir(
                                            dataDir, pathAndBucket.f0, pathAndBucket.f1);
                            if (kvTabletDir.exists()) {
                                LOG.info(
                                        "Also removing corresponding KV tablet directory: {}",
                                        kvTabletDir);
                                FileUtils.deleteDirectoryQuietly(kvTabletDir);
                            }
                        } catch (Exception kvDeleteException) {
                            LOG.warn(
                                    "Failed to delete corresponding KV tablet directory for log {}: {}",
                                    tabletDir,
                                    kvDeleteException.getMessage());
                        }
                        return;
                    }
                    throw new FlussRuntimeException(e);
                }
            }
        };
    }

    @VisibleForTesting
    void checkpointRecoveryOffsets(File dataDir) {
        checkpointRecoveryOffsets(
                dataDir,
                currentLogs.values().stream()
                        .filter(log -> log.getDataDir().equals(dataDir))
                        .collect(Collectors.toList()));
    }

    private void checkpointRecoveryOffsets(File dataDir, List<LogTablet> logs) {
        try {
            Map<TableBucket, Long> recoveryOffsets = new HashMap<>();
            for (LogTablet logTablet : logs) {
                recoveryOffsets.put(logTablet.getTableBucket(), logTablet.getRecoveryPoint());
            }
            LOG.debug("Writing recovery offsets checkpoint for directory {}.", dataDir);
            recoveryPointCheckpoints.get(dataDir).write(recoveryOffsets);
        } catch (Exception e) {
            throw new LogStorageException(
                    "Disk error while writing recovery offsets checkpoint", e);
        }
    }

    private static final class LogRecoveryTask {
        private final File dataDir;
        private final ExecutorService pool;
        private final List<Future<?>> jobs;
        private final long startTimeMillis;

        private LogRecoveryTask(
                File dataDir, ExecutorService pool, List<Future<?>> jobs, long startTimeMillis) {
            this.dataDir = dataDir;
            this.pool = pool;
            this.jobs = jobs;
            this.startTimeMillis = startTimeMillis;
        }
    }

    private static final class LogShutdownTask {
        private final File dataDir;
        private final List<LogTablet> logs;
        private final ExecutorService pool;
        private final List<Future<?>> jobs;

        private LogShutdownTask(
                File dataDir, List<LogTablet> logs, ExecutorService pool, List<Future<?>> jobs) {
            this.dataDir = dataDir;
            this.logs = logs;
            this.pool = pool;
            this.jobs = jobs;
        }
    }
}
