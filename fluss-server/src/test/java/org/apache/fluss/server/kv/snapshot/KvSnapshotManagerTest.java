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

package org.apache.fluss.server.kv.snapshot;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.kv.KvSnapshotResource;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.testutils.common.ManuallyTriggeredScheduledExecutorService;
import org.apache.fluss.utils.clock.ManualClock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.fluss.shaded.guava32.com.google.common.collect.Iterators.getOnlyElement;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link KvSnapshotManager} . */
class KvSnapshotManagerTest {
    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static final long periodicMaterializeDelay = 10_000L;
    private static ZooKeeperClient zkClient;
    private final TableBucket tableBucket = new TableBucket(1, 1);
    private ManuallyTriggeredScheduledExecutorService scheduledExecutorService;
    private ManuallyTriggeredScheduledExecutorService asyncSnapshotExecutorService;
    private KvSnapshotResource kvSnapshotResource;
    private KvSnapshotManager kvSnapshotManager;
    private DefaultSnapshotContext snapshotContext;
    private Configuration conf;
    private ManualClock manualClock;
    private @TempDir File tmpKvDir;

    @BeforeAll
    static void baseBeforeAll() {
        zkClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
    }

    @BeforeEach
    void before() {
        conf = new Configuration();
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofMillis(periodicMaterializeDelay));
        scheduledExecutorService = new ManuallyTriggeredScheduledExecutorService();
        asyncSnapshotExecutorService = new ManuallyTriggeredScheduledExecutorService();
        ExecutorService dataTransferThreadPool = Executors.newFixedThreadPool(1);
        kvSnapshotResource =
                new KvSnapshotResource(
                        scheduledExecutorService,
                        new KvSnapshotDataUploader(dataTransferThreadPool),
                        new KvSnapshotDataDownloader(dataTransferThreadPool),
                        asyncSnapshotExecutorService);
        snapshotContext =
                DefaultSnapshotContext.create(
                        zkClient,
                        new TestingCompletedKvSnapshotCommitter(),
                        kvSnapshotResource,
                        conf);
        manualClock = new ManualClock(System.currentTimeMillis());
    }

    @AfterEach
    void close() {
        if (kvSnapshotManager != null) {
            kvSnapshotManager.close();
        }
    }

    @Test
    void testInitialDelay() {
        kvSnapshotManager = createSnapshotManager(true);
        startPeriodicUploadSnapshot(NopUploadSnapshotTarget.INSTANCE);
        checkOnlyOneScheduledTasks();
    }

    @Test
    void testInitWithNonPositiveSnapshotInterval() {
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofMillis(0));
        snapshotContext =
                DefaultSnapshotContext.create(
                        zkClient,
                        new TestingCompletedKvSnapshotCommitter(),
                        kvSnapshotResource,
                        conf);
        kvSnapshotManager = createSnapshotManager(snapshotContext);
        startPeriodicUploadSnapshot(NopUploadSnapshotTarget.INSTANCE);
        // periodic snapshot is disabled when periodicMaterializeDelay is not positive
        Assertions.assertEquals(0, scheduledExecutorService.getAllScheduledTasks().size());
    }

    @Test
    void testPeriodicSnapshot() {
        kvSnapshotManager = createSnapshotManager(true);
        startPeriodicUploadSnapshot(NopUploadSnapshotTarget.INSTANCE);
        // check only one schedule task
        checkOnlyOneScheduledTasks();
        scheduledExecutorService.triggerNonPeriodicScheduledTasks();
        // after trigger, should still remain one task
        checkOnlyOneScheduledTasks();
    }

    @Test
    void testSnapshot() {
        // use local filesystem to make the FileSystem plugin happy
        String snapshotDir = "file:/test/snapshot1";
        TestUploadSnapshotTarget target = new TestUploadSnapshotTarget(new FsPath(snapshotDir));
        kvSnapshotManager = createSnapshotManager(true);
        startPeriodicUploadSnapshot(target);
        // trigger schedule
        scheduledExecutorService.triggerNonPeriodicScheduledTasks();
        // trigger async snapshot
        asyncSnapshotExecutorService.trigger();

        // now, check the result
        assertThat(target.getCollectedRemoteDirs())
                .isEqualTo(Collections.singletonList(snapshotDir));
    }

    @Test
    void testSnapshotWithException() {
        // use local filesystem to make the FileSystem plugin happy
        String remoteDir = "file:/test/snapshot1";
        String exceptionMessage = "Exception while initializing Materialization";
        TestUploadSnapshotTarget target =
                new TestUploadSnapshotTarget(new FsPath(remoteDir), exceptionMessage);
        kvSnapshotManager = createSnapshotManager(true);
        startPeriodicUploadSnapshot(target);
        // trigger schedule
        scheduledExecutorService.triggerNonPeriodicScheduledTasks();

        // trigger async snapshot
        asyncSnapshotExecutorService.trigger();

        assertThat(target.getCause())
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOf(FlussRuntimeException.class)
                .hasMessage(exceptionMessage);
    }

    private void checkOnlyOneScheduledTasks() {
        assertThat(
                        getOnlyElement(scheduledExecutorService.getAllScheduledTasks().iterator())
                                .getDelay(MILLISECONDS))
                .as(
                        String.format(
                                "task for initial materialization should be scheduled with a 0..%d delay",
                                periodicMaterializeDelay))
                .isLessThanOrEqualTo(periodicMaterializeDelay);
    }

    private KvSnapshotManager createSnapshotManager(boolean isLeader) {
        KvSnapshotManager snapshotManager = createSnapshotManager(snapshotContext);
        if (isLeader) {
            snapshotManager.becomeLeader();
        }
        return snapshotManager;
    }

    private KvSnapshotManager createSnapshotManager(SnapshotContext context) {
        return new KvSnapshotManager(tableBucket, tmpKvDir, context, manualClock);
    }

    private void startPeriodicUploadSnapshot(KvSnapshotManager.UploadSnapshotTarget target) {
        kvSnapshotManager.startPeriodicUploadSnapshot(
                org.apache.fluss.utils.concurrent.Executors.directExecutor(), target);
    }

    private static class NopUploadSnapshotTarget implements KvSnapshotManager.UploadSnapshotTarget {
        private static final NopUploadSnapshotTarget INSTANCE = new NopUploadSnapshotTarget();

        @Override
        public long currentSnapshotId() {
            return 0;
        }

        @Override
        public Optional<KvSnapshotManager.SnapshotRunnable> initSnapshot() {
            return Optional.empty();
        }

        @Override
        public void handleSnapshotResult(
                long snapshotId,
                int coordinatorEpoch,
                int bucketLeaderEpoch,
                SnapshotLocation snapshotLocation,
                SnapshotResult snapshotResult) {}

        @Override
        public void handleSnapshotFailure(
                long snapshotId, SnapshotLocation snapshotLocation, Throwable cause) {}

        @Override
        public long getSnapshotSize() {
            return 0L;
        }
    }

    private static class TestUploadSnapshotTarget
            implements KvSnapshotManager.UploadSnapshotTarget {

        private final FsPath snapshotPath;
        private final SnapshotLocation snapshotLocation;
        private final List<String> collectedRemoteDirs;
        private final String exceptionMessage;
        private Throwable cause;

        public TestUploadSnapshotTarget(FsPath snapshotPath) {
            this(snapshotPath, null);
        }

        public TestUploadSnapshotTarget(FsPath snapshotPath, String exceptionMessage) {
            this.snapshotPath = snapshotPath;
            this.collectedRemoteDirs = new ArrayList<>();
            this.exceptionMessage = exceptionMessage;
            try {
                this.snapshotLocation =
                        new SnapshotLocation(
                                snapshotPath.getFileSystem(), snapshotPath, snapshotPath, 1024);
            } catch (IOException e) {
                throw new FlussRuntimeException(e);
            }
        }

        @Override
        public long currentSnapshotId() {
            return 0;
        }

        @Override
        public Optional<KvSnapshotManager.SnapshotRunnable> initSnapshot() {
            RunnableFuture<SnapshotResult> runnableFuture =
                    new FutureTask<>(
                            () -> {
                                if (exceptionMessage != null) {
                                    throw new FlussRuntimeException(exceptionMessage);
                                } else {
                                    final long logOffset = 0;
                                    return new SnapshotResult(null, snapshotPath, logOffset);
                                }
                            });
            int snapshotId = 1;
            int coordinatorEpoch = 0;
            int leaderEpoch = 0;
            return Optional.of(
                    new KvSnapshotManager.SnapshotRunnable(
                            runnableFuture,
                            snapshotId,
                            coordinatorEpoch,
                            leaderEpoch,
                            snapshotLocation));
        }

        @Override
        public void handleSnapshotResult(
                long snapshotId,
                int coordinatorEpoch,
                int bucketLeaderEpoch,
                SnapshotLocation snapshotLocation,
                SnapshotResult snapshotResult) {
            collectedRemoteDirs.add(snapshotResult.getSnapshotPath().toString());
        }

        @Override
        public void handleSnapshotFailure(
                long snapshotId, SnapshotLocation snapshotLocation, Throwable cause) {
            this.cause = cause;
        }

        @Override
        public long getSnapshotSize() {
            return 0L;
        }

        private List<String> getCollectedRemoteDirs() {
            return collectedRemoteDirs;
        }

        private Throwable getCause() {
            return this.cause;
        }
    }
}
