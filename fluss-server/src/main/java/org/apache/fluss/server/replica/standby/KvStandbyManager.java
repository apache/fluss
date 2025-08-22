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

package org.apache.fluss.server.replica.standby;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.remote.RemoteFileDownloader;
import org.apache.fluss.server.kv.KvApplyLogHelper;
import org.apache.fluss.server.log.remote.RemoteLogManager;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.utils.MapUtils;
import org.apache.fluss.utils.concurrent.Scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;

/** The manager to manage the kv standby replicas. */
public class KvStandbyManager {

    private static final Logger LOG = LoggerFactory.getLogger(KvStandbyManager.class);

    private final Scheduler scheduler;
    private final ZooKeeperClient zkClient;
    private final RemoteFileDownloader remoteFileDownloader;
    private final RemoteLogManager remoteLogManager;
    private final Path remoteLogTempDir;
    private final int maxFetchLogSizeWhenApplying;

    private final Map<TableBucket, TaskWithFuture> standbyTasks = MapUtils.newConcurrentHashMap();
    private final Map<TableBucket, KvApplyLogHelper> applyLogToKvHelpers =
            MapUtils.newConcurrentHashMap();

    public KvStandbyManager(
            Configuration conf,
            Scheduler scheduler,
            ZooKeeperClient zkClient,
            String dataDir,
            RemoteLogManager remoteLogManager)
            throws IOException {
        this.scheduler = scheduler;
        this.zkClient = zkClient;
        this.maxFetchLogSizeWhenApplying =
                (int) conf.get(ConfigOptions.KV_RECOVER_LOG_RECORD_BATCH_MAX_SIZE).getBytes();
        this.remoteLogManager = remoteLogManager;
        this.remoteFileDownloader = new RemoteFileDownloader(1);
        this.remoteLogTempDir = Paths.get(dataDir, "temp-remote-logs");
        prepareRemoteLogTempDir();
    }

    private void prepareRemoteLogTempDir() throws IOException {
        if (!Files.exists(remoteLogTempDir)) {
            Files.createDirectory(remoteLogTempDir);
        } else {
            // remove all temp file.
            Files.walk(remoteLogTempDir)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }

    public void shutdown() {
        // do nothing now.
    }

    public void startStandby(Replica replica) {
        TableBucket tableBucket = replica.getTableBucket();

        KvApplyLogHelper kvApplyLogHelper =
                applyLogToKvHelpers.computeIfAbsent(
                        tableBucket, tb -> new KvApplyLogHelper(replica, zkClient));

        standbyTasks.compute(
                tableBucket,
                (tb, prevTask) -> {
                    if (prevTask != null) {
                        LOG.info("Cancelling the standby task for table-bucket: {}", tableBucket);
                        prevTask.cancel();
                    }
                    BecomeHotStandbyTask task =
                            new BecomeHotStandbyTask(
                                    replica,
                                    remoteFileDownloader,
                                    remoteLogManager,
                                    remoteLogTempDir,
                                    kvApplyLogHelper,
                                    maxFetchLogSizeWhenApplying);
                    LOG.info("Created a new standby task: {} and getting scheduled one time", task);
                    ScheduledFuture<?> future =
                            scheduler.scheduleOnce("become-hot-standby-task", task);
                    return new TaskWithFuture(task, future);
                });
    }

    public KvApplyLogHelper getApplyLogToKvHelper(TableBucket tableBucket) {
        return applyLogToKvHelpers.get(tableBucket);
    }

    public boolean hasBecomeHotStandbyTask(TableBucket tableBucket) {
        return standbyTasks.containsKey(tableBucket);
    }

    public void cancelBecomeHotStandbyTask(TableBucket tableBucket) {
        standbyTasks.computeIfPresent(
                tableBucket,
                (tb, prevTask) -> {
                    prevTask.cancel();
                    return prevTask;
                });
    }

    public void stopStandby(TableBucket tableBucket) {
        standbyTasks.remove(tableBucket);
        applyLogToKvHelpers.remove(tableBucket);
    }

    static class TaskWithFuture {

        private final BecomeHotStandbyTask task;
        private final Future<?> future;

        TaskWithFuture(BecomeHotStandbyTask task, Future<?> future) {
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
}
