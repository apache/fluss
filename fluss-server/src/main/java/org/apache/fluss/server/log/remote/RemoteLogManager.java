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

package org.apache.fluss.server.log.remote;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.RemoteStorageException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.RemoteLogManifestHandle;
import org.apache.fluss.utils.IOUtils;
import org.apache.fluss.utils.MapUtils;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * The entry point for remote log management. The remote log manager is responsible for managing log
 * tiering from local log segments to remote log segments, expiring remote log segments, and
 * providing APIs to fetch log indexes and metadata about remote log segments.
 *
 * <p>This class is thread safe.
 */
@ThreadSafe
public class RemoteLogManager implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(RemoteLogManager.class);
    public static final String RLM_SCHEDULED_THREAD_PREFIX = "fluss-remote-log-manager-thread-pool";

    private final long taskInterval;
    private final RemoteLogIndexCache remoteLogIndexCache;
    private final RemoteLogStorage remoteLogStorage;
    private final CoordinatorGateway coordinatorGateway;
    private final ScheduledExecutorService rlManagerScheduledThreadPool;
    private final Clock clock;
    private final ZooKeeperClient zkClient;

    private final Map<TableBucket, TaskWithFuture> rlmTasks = MapUtils.newConcurrentHashMap();
    private final Map<TableBucket, RemoteLogTablet> remoteLogs = MapUtils.newConcurrentHashMap();

    public RemoteLogManager(
            Configuration conf,
            ZooKeeperClient zkClient,
            CoordinatorGateway coordinatorGateway,
            Clock clock)
            throws IOException {
        this(
                conf,
                zkClient,
                coordinatorGateway,
                new DefaultRemoteLogStorage(conf),
                Executors.newScheduledThreadPool(
                        conf.getInt(ConfigOptions.REMOTE_LOG_MANAGER_THREAD_POOL_SIZE),
                        new ExecutorThreadFactory(RLM_SCHEDULED_THREAD_PREFIX)),
                clock);
    }

    @VisibleForTesting
    public RemoteLogManager(
            Configuration conf,
            ZooKeeperClient zkClient,
            CoordinatorGateway coordinatorGateway,
            RemoteLogStorage remoteLogStorage,
            ScheduledExecutorService scheduledExecutor,
            Clock clock)
            throws IOException {
        this.remoteLogStorage = remoteLogStorage;
        this.zkClient = zkClient;
        this.coordinatorGateway = coordinatorGateway;

        File dataDir = new File(conf.getString(ConfigOptions.DATA_DIR));
        this.remoteLogIndexCache =
                new RemoteLogIndexCache(
                        (int) conf.get(ConfigOptions.REMOTE_LOG_INDEX_FILE_CACHE_SIZE).getBytes(),
                        remoteLogStorage,
                        dataDir);
        this.taskInterval = conf.get(ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION).toMillis();
        this.rlManagerScheduledThreadPool = scheduledExecutor;
        this.clock = clock;
    }

    public RemoteLogStorage getRemoteLogStorage() {
        return remoteLogStorage;
    }

    public FsPath remoteLogDir() {
        return remoteLogStorage.getRemoteLogDir();
    }

    /** Restore the remote log manifest and start the log tiering task for the given replica. */
    public void startLogTiering(Replica replica) throws Exception {
        if (remoteDisabled()) {
            return;
        }
        TableBucket tableBucket = replica.getTableBucket();
        PhysicalTablePath physicalTablePath = replica.getPhysicalTablePath();
        LogTablet log = replica.getLogTablet();
        RemoteLogTablet remoteLog =
                new RemoteLogTablet(physicalTablePath, tableBucket, replica.getLogTTLMs());
        Optional<RemoteLogManifestHandle> remoteLogManifestHandleOpt =
                zkClient.getRemoteLogManifestHandle(tableBucket);
        if (remoteLogManifestHandleOpt.isPresent()) {
            // If there is remote log manifest handle in remote, we will download
            // the manifest snapshot from remote storage and write to cache.
            RemoteLogManifest manifest =
                    remoteLogStorage.readRemoteLogManifestSnapshot(
                            remoteLogManifestHandleOpt.get().getRemoteLogManifestPath());
            remoteLog.loadRemoteLogManifest(manifest);
        }
        remoteLog.getRemoteLogEndOffset().ifPresent(log::updateRemoteLogEndOffset);
        log.updateRemoteLogStartOffset(remoteLog.getRemoteLogStartOffset());
        log.updateRemoteLogSize(remoteLog.getRemoteSizeInBytes());
        // leader needs to register the remote log metrics
        remoteLog.registerMetrics(replica.bucketMetrics());
        remoteLogs.put(tableBucket, remoteLog);

        doHandleLeaderReplica(replica, remoteLog, tableBucket);
        LOG.debug("Added the remote log tiering task for replica {}", tableBucket);
    }

    public long getRemoteLogSize() {
        return remoteLogs.values().stream().mapToLong(RemoteLogTablet::getRemoteSizeInBytes).sum();
    }

    /** Stop the log tiering task for the given replica. */
    public void stopLogTiering(Replica replica) {
        if (remoteDisabled()) {
            return;
        }
        TableBucket tb = replica.getTableBucket();
        RemoteLogTablet remoteLog = remoteLogs.remove(tb);

        if (remoteLog != null) {
            List<UUID> remoteLogSegmentIdList =
                    remoteLog.allRemoteLogSegments().stream()
                            .map(RemoteLogSegment::remoteLogSegmentId)
                            .collect(Collectors.toList());
            // remove cache.
            remoteLogIndexCache.removeAll(remoteLogSegmentIdList);
            // unregister the remote log metrics, only leader needs to report
            remoteLog.unregisterMetrics();
        }

        TaskWithFuture task = rlmTasks.remove(tb);
        if (task != null) {
            LOG.info("Cancelling the RLM task for table-bucket: {}", tb);
            task.cancel();
        }
        LOG.debug("Removed the remote log tiering task for replica {}", tb);
    }

    /**
     * Stop the log tiering task for the given replica, and maybe delete remote logs of the replica
     * in remote storage.
     */
    public void stopReplica(Replica replica, boolean deleteRemote) {
        // if the remote storage disabled, do nothing.
        if (remoteDisabled()) {
            return;
        }

        PhysicalTablePath physicalTablePath = replica.getPhysicalTablePath();
        TableBucket tb = replica.getTableBucket();
        // stop the log tiering task for the table bucket.
        stopLogTiering(replica);

        if (deleteRemote) {
            LOG.info("Deleting the remote log segments for table-bucket: {}", tb);
            // delete the remote log of the table bucket.
            deleteRemoteLog(physicalTablePath, tb);
        }
    }

    /** Get the position of the given offset in the remote log segment. */
    public int lookupPositionForOffset(RemoteLogSegment remoteLogSegment, long offset) {
        return remoteLogIndexCache.lookupPosition(remoteLogSegment, offset);
    }

    /**
     * Get the offset of the given timestamp in the remote log segment. If not found, -1L will
     * return.
     */
    public long lookupOffsetForTimestamp(TableBucket tableBucket, long timestamp) {
        if (remoteDisabled()) {
            return -1L;
        }

        RemoteLogTablet remoteLogTablet = remoteLogs.get(tableBucket);
        if (remoteLogTablet == null) {
            return -1L;
        }

        RemoteLogSegment segment = remoteLogTablet.findSegmentByTimestamp(timestamp);
        if (segment == null) {
            return -1L;
        } else {
            return remoteLogIndexCache.lookupOffsetForTimestamp(segment, timestamp);
        }
    }

    /**
     * Get all remote log segments relevant to the input offset, which including these segments
     * whose remote log start offset higher that or equal to this offset, and including another one
     * segment whose remote log start offset smaller than this offset (floor key).
     */
    public List<RemoteLogSegment> relevantRemoteLogSegments(TableBucket tableBucket, long offset) {
        return remoteLogTablet(tableBucket).relevantRemoteLogSegments(offset);
    }

    private boolean remoteDisabled() {
        return taskInterval <= 0L;
    }

    /**
     * Delete the remote log. This method will delete all the files in remote for this table bucket
     * and will delete the log segment manifests cache in local for leader replica.
     *
     * <p>Note: the zk path for {@link RemoteLogManifestHandle} will be deleted by coordinator while
     * table delete.
     */
    private void deleteRemoteLog(PhysicalTablePath physicalTablePath, TableBucket tableBucket) {
        // delete the file in remote storage.
        try {
            // TODO: maybe need to optimize to delete on specific file path
            remoteLogStorage.deleteTableBucket(physicalTablePath, tableBucket);
        } catch (RemoteStorageException e) {
            LOG.error(
                    "Error occurred while deleting remote log for table-bucket: {}",
                    tableBucket,
                    e);
        }
    }

    private void doHandleLeaderReplica(
            Replica replica, RemoteLogTablet remoteLog, TableBucket tableBucket) {
        rlmTasks.compute(
                tableBucket,
                (tb, prevTask) -> {
                    if (prevTask != null) {
                        LOG.info(
                                "Cancelling the remote log task for table-bucket: {}", tableBucket);
                        prevTask.cancel();
                    }
                    LogTieringTask task =
                            new LogTieringTask(
                                    replica,
                                    remoteLog,
                                    remoteLogStorage,
                                    coordinatorGateway,
                                    clock);
                    LOG.info("Created a new remote log task: {} and getting scheduled", task);
                    ScheduledFuture<?> future =
                            rlManagerScheduledThreadPool.scheduleWithFixedDelay(
                                    task,
                                    Math.abs(ThreadLocalRandom.current().nextLong(taskInterval)),
                                    taskInterval,
                                    TimeUnit.MILLISECONDS);
                    return new TaskWithFuture(task, future);
                });
    }

    @VisibleForTesting
    RemoteLogTablet remoteLogTablet(TableBucket tableBucket) {
        RemoteLogTablet remoteLog = remoteLogs.get(tableBucket);
        if (remoteLog == null) {
            throw new IllegalStateException(
                    "RemoteLogTablet can't be found for table-bucket " + tableBucket);
        }
        return remoteLog;
    }

    @Override
    public void close() throws IOException {
        rlmTasks.values().forEach(TaskWithFuture::cancel);
        IOUtils.closeQuietly(remoteLogStorage, "RemoteLogStorageManager");
        IOUtils.closeQuietly(remoteLogIndexCache, "RemoteIndexCache");

        shutdownAndAwaitTermination(
                rlManagerScheduledThreadPool, "RLMScheduledThreadPool", 10, TimeUnit.SECONDS);

        rlmTasks.clear();
    }

    static class TaskWithFuture {

        private final LogTieringTask task;
        private final Future<?> future;

        TaskWithFuture(LogTieringTask task, Future<?> future) {
            this.task = task;
            this.future = future;
        }

        public void cancel() {
            task.cancel();
            try {
                future.cancel(true);
            } catch (Exception ex) {
                LOG.error("Error occurred while canceling the task: {}", task, ex);
            }
        }
    }

    private static void shutdownAndAwaitTermination(
            ExecutorService pool, String poolName, long timeout, TimeUnit timeUnit) {
        // This pattern of shutting down thread pool is adopted from here:
        // https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/concurrent/ExecutorService.html
        LOG.info("Shutting down of thread pool {} is started", poolName);
        pool.shutdown(); // Disable new tasks from being submitted
        try {
            // Wait a while for existing tasks to terminate
            if (!pool.awaitTermination(timeout, timeUnit)) {
                LOG.info(
                        "Shutting down of thread pool {} could not be completed. It will retry cancelling "
                                + "the tasks using shutdownNow.",
                        poolName);
                // Cancel currently executing tasks.
                pool.shutdownNow();
                // Wait a while for tasks to respond to being cancelled
                if (!pool.awaitTermination(timeout, timeUnit)) {
                    LOG.warn(
                            "Shutting down of thread pool {} could not be completed even after retrying "
                                    + "cancellation of the tasks using shutdownNow.",
                            poolName);
                }
            }
        } catch (InterruptedException ex) {
            // (Re-)Cancel if current thread also interrupted.
            LOG.warn(
                    "Encountered InterruptedException while shutting down thread pool {}. It will retry "
                            + "cancelling the tasks using shutdownNow.",
                    poolName);
            pool.shutdownNow();
            // Preserve interrupt status.
            Thread.currentThread().interrupt();
        }

        LOG.info("Shutting down of thread pool {} is completed", poolName);
    }

    @VisibleForTesting
    public RemoteLogIndexCache getRemoteLogIndexCache() {
        return remoteLogIndexCache;
    }
}
