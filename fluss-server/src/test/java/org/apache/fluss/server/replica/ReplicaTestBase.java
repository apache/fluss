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

package org.apache.fluss.server.replica;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.metrics.TestingClientMetricGroup;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.coordinator.TestCoordinatorGateway;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.kv.KvManager;
import org.apache.fluss.server.kv.snapshot.CompletedKvSnapshotCommitter;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.kv.snapshot.KvSnapshotDataDownloader;
import org.apache.fluss.server.kv.snapshot.KvSnapshotDataUploader;
import org.apache.fluss.server.kv.snapshot.SnapshotContext;
import org.apache.fluss.server.kv.snapshot.TestingCompletedKvSnapshotCommitter;
import org.apache.fluss.server.log.LogManager;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.checkpoint.OffsetCheckpointFile;
import org.apache.fluss.server.log.remote.RemoteLogManager;
import org.apache.fluss.server.log.remote.TestingRemoteLogStorage;
import org.apache.fluss.server.metadata.ClusterMetadata;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.server.metrics.group.BucketMetricGroup;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.testutils.common.ManuallyTriggeredScheduledExecutorService;
import org.apache.fluss.utils.CloseableRegistry;
import org.apache.fluss.utils.clock.ManualClock;
import org.apache.fluss.utils.concurrent.FlussScheduler;
import org.apache.fluss.utils.function.FunctionWithException;
import org.apache.fluss.utils.function.ThrowingRunnable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_PHYSICAL_TABLE_PATH;
import static org.apache.fluss.record.TestData.DATA1_PHYSICAL_TABLE_PATH_PA_2024;
import static org.apache.fluss.record.TestData.DATA1_PHYSICAL_TABLE_PATH_PK_PA_2024;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static org.apache.fluss.record.TestData.DATA2_SCHEMA;
import static org.apache.fluss.record.TestData.DATA2_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DATA2_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA2_TABLE_PATH;
import static org.apache.fluss.server.coordinator.CoordinatorContext.INITIAL_COORDINATOR_EPOCH;
import static org.apache.fluss.server.replica.ReplicaManager.HIGH_WATERMARK_CHECKPOINT_FILE_NAME;
import static org.apache.fluss.server.zk.data.LeaderAndIsr.INITIAL_BUCKET_EPOCH;
import static org.apache.fluss.server.zk.data.LeaderAndIsr.INITIAL_LEADER_EPOCH;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsWithWriterId;
import static org.apache.fluss.utils.FlussPaths.remoteLogDir;
import static org.apache.fluss.utils.FlussPaths.remoteLogTabletDir;

/**
 * Test base class for {@link Replica}, {@link ReplicaManager} and related operations related
 * function managed by {@link ReplicaManager}.
 */
public class ReplicaTestBase {
    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    protected static final int TABLET_SERVER_ID = 1;
    private static final String TABLET_SERVER_RACK = "rack1";
    protected static ZooKeeperClient zkClient;

    // to register all should be closed after each test
    private final CloseableRegistry closeableRegistry = new CloseableRegistry();

    protected @TempDir File tempDir;
    protected ManualClock manualClock;
    protected LogManager logManager;
    protected KvManager kvManager;
    protected ReplicaManager replicaManager;
    protected RpcClient rpcClient;
    protected Configuration conf;
    protected TabletServerMetadataCache serverMetadataCache;
    protected TestingCompletedKvSnapshotCommitter snapshotReporter;
    protected TestCoordinatorGateway testCoordinatorGateway;
    private FlussScheduler scheduler;

    // remote log related
    protected TestingRemoteLogStorage remoteLogStorage;
    protected RemoteLogManager remoteLogManager;
    protected ManuallyTriggeredScheduledExecutorService remoteLogTaskScheduler;

    protected Configuration getServerConf() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION, Duration.ofMillis(0L));
        return conf;
    }

    @BeforeAll
    static void baseBeforeAll() {
        zkClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
    }

    @BeforeEach
    public void setup() throws Exception {
        conf = getServerConf();
        conf.setString(ConfigOptions.DATA_DIR, tempDir.getAbsolutePath());
        conf.setString(ConfigOptions.COORDINATOR_HOST, "localhost");
        conf.set(ConfigOptions.REMOTE_DATA_DIR, tempDir.getAbsolutePath() + "/remote_data_dir");
        conf.set(ConfigOptions.REMOTE_LOG_DATA_TRANSFER_THREAD_NUM, 1);
        // set snapshot interval to 1 seconds for test purpose
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofSeconds(1));

        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE, MemorySize.parse("10kb"));
        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_PAGE_SIZE, MemorySize.parse("512b"));
        conf.set(ConfigOptions.CLIENT_WRITER_BATCH_SIZE, MemorySize.parse("1kb"));

        scheduler = new FlussScheduler(2);
        scheduler.startup();

        manualClock = new ManualClock(System.currentTimeMillis());
        logManager = LogManager.create(conf, zkClient, scheduler, manualClock);
        logManager.startup();

        kvManager = KvManager.create(conf, zkClient, logManager);
        kvManager.startup();

        serverMetadataCache =
                new TabletServerMetadataCache(new MetadataManager(zkClient, conf), zkClient);
        initMetadataCache(serverMetadataCache);

        rpcClient = RpcClient.create(conf, TestingClientMetricGroup.newInstance(), false);

        snapshotReporter = new TestingCompletedKvSnapshotCommitter();

        testCoordinatorGateway = new TestCoordinatorGateway(zkClient);
        initRemoteLogEnv();

        // init replica manager
        replicaManager = buildReplicaManager(testCoordinatorGateway);
        replicaManager.startup();

        // We will register all tables in TestData in zk client previously.
        registerTableInZkClient();
    }

    private void initMetadataCache(TabletServerMetadataCache metadataCache) {
        metadataCache.updateClusterMetadata(
                new ClusterMetadata(
                        new ServerInfo(
                                0,
                                null,
                                Endpoint.fromListenersString("CLIENT://localhost:1234"),
                                ServerType.COORDINATOR),
                        new HashSet<>(
                                Arrays.asList(
                                        new ServerInfo(
                                                TABLET_SERVER_ID,
                                                TABLET_SERVER_RACK,
                                                Endpoint.fromListenersString(
                                                        "CLIENT://localhost:90"),
                                                ServerType.TABLET_SERVER),
                                        new ServerInfo(
                                                2,
                                                "rack2",
                                                Endpoint.fromListenersString(
                                                        "CLIENT://localhost:91"),
                                                ServerType.TABLET_SERVER),
                                        new ServerInfo(
                                                3,
                                                "rack3",
                                                Endpoint.fromListenersString(
                                                        "CLIENT://localhost:92"),
                                                ServerType.TABLET_SERVER)))));
    }

    private void registerTableInZkClient() throws Exception {
        TableDescriptor data1NonPkTableDescriptor =
                TableDescriptor.builder().schema(DATA1_SCHEMA).distributedBy(3).build();
        zkClient.registerTable(
                DATA1_TABLE_PATH,
                TableRegistration.newTable(DATA1_TABLE_ID, data1NonPkTableDescriptor));
        zkClient.registerSchema(DATA1_TABLE_PATH, DATA1_SCHEMA);
        zkClient.registerTable(
                DATA1_TABLE_PATH_PK,
                TableRegistration.newTable(DATA1_TABLE_ID_PK, DATA1_TABLE_DESCRIPTOR_PK));
        zkClient.registerSchema(DATA1_TABLE_PATH_PK, DATA1_SCHEMA_PK);

        zkClient.registerTable(
                DATA2_TABLE_PATH,
                TableRegistration.newTable(DATA2_TABLE_ID, DATA2_TABLE_DESCRIPTOR));
        zkClient.registerSchema(DATA2_TABLE_PATH, DATA2_SCHEMA);
    }

    protected long registerTableInZkClient(
            TablePath tablePath,
            Schema schema,
            long tableId,
            List<String> bucketKeys,
            Map<String, String> properties)
            throws Exception {
        TableDescriptor.Builder builder =
                TableDescriptor.builder().schema(schema).distributedBy(3, bucketKeys);
        properties.forEach(builder::property);
        TableDescriptor tableDescriptor = builder.build();
        // if exists, drop it firstly
        if (zkClient.tableExist(tablePath)) {
            zkClient.deleteTable(tablePath);
        }
        zkClient.registerTable(tablePath, TableRegistration.newTable(tableId, tableDescriptor));
        zkClient.registerSchema(tablePath, schema);
        return tableId;
    }

    protected ReplicaManager buildReplicaManager(CoordinatorGateway coordinatorGateway)
            throws Exception {
        return new ReplicaManager(
                conf,
                scheduler,
                logManager,
                kvManager,
                zkClient,
                TABLET_SERVER_ID,
                serverMetadataCache,
                rpcClient,
                coordinatorGateway,
                snapshotReporter,
                NOPErrorHandler.INSTANCE,
                TestingMetricGroups.TABLET_SERVER_METRICS,
                remoteLogManager,
                manualClock);
    }

    @AfterEach
    void tearDown() throws Exception {
        closeableRegistry.close();

        if (logManager != null) {
            logManager.shutdown();
        }

        if (remoteLogStorage != null) {
            remoteLogStorage.close();
        }

        if (remoteLogManager != null) {
            remoteLogManager.close();
        }

        if (kvManager != null) {
            kvManager.shutdown();
        }

        if (replicaManager != null) {
            replicaManager.shutdown();
        }

        if (rpcClient != null) {
            rpcClient.close();
        }

        if (scheduler != null) {
            scheduler.shutdown();
        }

        // clear zk environment.
        ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().cleanupRoot();
    }

    // TODO this is only for single tablet server unit test.
    // TODO add more test cases for partition table which make leader by this method.
    protected void makeLogTableAsLeader(int bucketId) {
        makeLogTableAsLeader(new TableBucket(DATA1_TABLE_ID, bucketId), false);
    }

    /** If partitionTable is true, the partitionId of input TableBucket tb can not be null. */
    protected void makeLogTableAsLeader(TableBucket tb, boolean partitionTable) {
        makeLogTableAsLeader(
                tb,
                Collections.singletonList(TABLET_SERVER_ID),
                Collections.singletonList(TABLET_SERVER_ID),
                partitionTable);
    }

    protected void makeLogTableAsLeader(
            TableBucket tb, List<Integer> replicas, List<Integer> isr, boolean partitionTable) {
        makeLeaderAndFollower(
                Collections.singletonList(
                        new NotifyLeaderAndIsrData(
                                partitionTable
                                        ? DATA1_PHYSICAL_TABLE_PATH_PA_2024
                                        : DATA1_PHYSICAL_TABLE_PATH,
                                tb,
                                replicas,
                                new LeaderAndIsr(
                                        TABLET_SERVER_ID,
                                        INITIAL_LEADER_EPOCH,
                                        isr,
                                        INITIAL_COORDINATOR_EPOCH,
                                        INITIAL_BUCKET_EPOCH))));
    }

    // TODO this is only for single tablet server unit test.
    // TODO add more test cases for partition table which make leader by this method.
    protected void makeKvTableAsLeader(long tableId, TablePath tablePath, int bucketId) {
        makeKvTableAsLeader(
                new TableBucket(tableId, bucketId), tablePath, INITIAL_LEADER_EPOCH, false);
    }

    protected void makeKvTableAsLeader(
            TableBucket tb, TablePath tablePath, int leaderEpoch, boolean partitionTable) {
        makeKvTableAsLeader(
                tb,
                tablePath,
                Collections.singletonList(TABLET_SERVER_ID),
                Collections.singletonList(TABLET_SERVER_ID),
                leaderEpoch,
                partitionTable);
    }

    protected void makeKvTableAsLeader(
            TableBucket tb,
            TablePath tablePath,
            List<Integer> replicas,
            List<Integer> isr,
            int leaderEpoch,
            boolean partitionTable) {
        makeLeaderAndFollower(
                Collections.singletonList(
                        new NotifyLeaderAndIsrData(
                                partitionTable
                                        ? DATA1_PHYSICAL_TABLE_PATH_PK_PA_2024
                                        : PhysicalTablePath.of(tablePath),
                                tb,
                                replicas,
                                new LeaderAndIsr(
                                        TABLET_SERVER_ID,
                                        leaderEpoch,
                                        isr,
                                        INITIAL_COORDINATOR_EPOCH,
                                        // use leader epoch as bucket epoch
                                        leaderEpoch))));
    }

    protected void makeLeaderAndFollower(List<NotifyLeaderAndIsrData> notifyLeaderAndIsrDataList) {
        replicaManager.becomeLeaderOrFollower(0, notifyLeaderAndIsrDataList, result -> {});
    }

    protected Replica makeLogReplica(PhysicalTablePath physicalTablePath, TableBucket tableBucket)
            throws Exception {
        return makeReplica(physicalTablePath, tableBucket, false, null);
    }

    protected Replica makeKvReplica(
            PhysicalTablePath physicalTablePath,
            TableBucket tableBucket,
            SnapshotContext snapshotContext)
            throws Exception {
        return makeReplica(physicalTablePath, tableBucket, true, snapshotContext);
    }

    protected Replica makeKvReplica(PhysicalTablePath physicalTablePath, TableBucket tableBucket)
            throws Exception {
        return makeReplica(physicalTablePath, tableBucket, true, null);
    }

    private Replica makeReplica(
            PhysicalTablePath physicalTablePath,
            TableBucket tableBucket,
            boolean isPkTable,
            @Nullable SnapshotContext snapshotContext)
            throws Exception {
        if (snapshotContext == null) {
            snapshotContext =
                    new TestSnapshotContext(conf.getString(ConfigOptions.REMOTE_DATA_DIR));
        }
        BucketMetricGroup metricGroup =
                replicaManager
                        .getServerMetricGroup()
                        .addPhysicalTableBucketMetricGroup(
                                physicalTablePath, tableBucket.getBucket(), isPkTable);
        return new Replica(
                physicalTablePath,
                tableBucket,
                logManager,
                isPkTable ? kvManager : null,
                conf.get(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME).toMillis(),
                conf.get(ConfigOptions.LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER),
                TABLET_SERVER_ID,
                new OffsetCheckpointFile.LazyOffsetCheckpoints(
                        new OffsetCheckpointFile(
                                new File(
                                        conf.getString(ConfigOptions.DATA_DIR),
                                        HIGH_WATERMARK_CHECKPOINT_FILE_NAME))),
                replicaManager.getDelayedWriteManager(),
                replicaManager.getDelayedFetchLogManager(),
                replicaManager.getAdjustIsrManager(),
                snapshotContext,
                serverMetadataCache,
                NOPErrorHandler.INSTANCE,
                metricGroup,
                DATA1_TABLE_INFO,
                manualClock);
    }

    private void initRemoteLogEnv() throws Exception {
        remoteLogStorage = new TestingRemoteLogStorage(conf);
        remoteLogTaskScheduler = new ManuallyTriggeredScheduledExecutorService();
        remoteLogManager =
                new RemoteLogManager(
                        conf,
                        zkClient,
                        testCoordinatorGateway,
                        remoteLogStorage,
                        remoteLogTaskScheduler,
                        manualClock);
    }

    protected void addMultiSegmentsToLogTablet(LogTablet logTablet, int numSegments)
            throws Exception {
        addMultiSegmentsToLogTablet(logTablet, numSegments, true);
    }

    /**
     * Add multi segments to log tablet. The segments including four none-active segments and one
     * active segment.
     */
    protected void addMultiSegmentsToLogTablet(
            LogTablet logTablet, int numSegments, boolean advanceClock) throws Exception {
        if (logTablet.activeLogSegment().getSizeInBytes() > 0) {
            // roll active segment if it is not empty.
            logTablet.roll(Optional.empty());
        }

        int batchSequence = 0;
        for (int i = 0; i < numSegments; i++) {
            // write 10 batches per segment.
            for (int j = 0; j < DATA1.size(); j++) {
                // use clock to mock a unique writer id at a time.
                long writerId = manualClock.milliseconds();
                MemoryLogRecords records =
                        genMemoryLogRecordsWithWriterId(
                                Collections.singletonList(DATA1.get(j)),
                                writerId,
                                batchSequence++,
                                i * 10L + j);
                if (advanceClock) {
                    manualClock.advanceTime(10, TimeUnit.MILLISECONDS);
                    batchSequence = 0;
                }

                logTablet.appendAsLeader(records);
            }
            logTablet.flush(true);

            // For the last segment, do not roll to leave an active segment.
            if (i != numSegments - 1) {
                logTablet.roll(Optional.empty());
            }
        }
        logTablet.updateHighWatermark(logTablet.localLogEndOffset());
    }

    protected Set<String> listRemoteLogFiles(TableBucket tableBucket) throws IOException {
        FsPath dir =
                remoteLogTabletDir(
                        remoteLogDir(conf),
                        tableBucket.getPartitionId() == null
                                ? DATA1_PHYSICAL_TABLE_PATH
                                : DATA1_PHYSICAL_TABLE_PATH_PA_2024,
                        tableBucket);
        return Arrays.stream(dir.getFileSystem().listStatus(dir))
                .map(f -> f.getPath().getName())
                .filter(f -> !f.equals("metadata"))
                .collect(Collectors.toSet());
    }

    /** An implementation of {@link SnapshotContext} for test purpose. */
    protected class TestSnapshotContext implements SnapshotContext {

        private final FsPath remoteKvTabletDir;
        protected ManuallyTriggeredScheduledExecutorService scheduledExecutorService;
        protected final TestingCompletedKvSnapshotCommitter testKvSnapshotStore;
        private final ExecutorService executorService;

        public TestSnapshotContext(
                String remoteKvTabletDir, TestingCompletedKvSnapshotCommitter testKvSnapshotStore)
                throws Exception {
            this(
                    remoteKvTabletDir,
                    testKvSnapshotStore,
                    new ManuallyTriggeredScheduledExecutorService());
        }

        public TestSnapshotContext(String remoteKvTabletDir) throws Exception {
            this(remoteKvTabletDir, new TestingCompletedKvSnapshotCommitter());
        }

        public TestSnapshotContext(
                String remoteKvTabletDir,
                ManuallyTriggeredScheduledExecutorService manuallyTriggeredScheduledExecutorService)
                throws Exception {
            this(
                    remoteKvTabletDir,
                    new TestingCompletedKvSnapshotCommitter(),
                    manuallyTriggeredScheduledExecutorService);
        }

        private TestSnapshotContext(
                String remoteKvTabletDir,
                TestingCompletedKvSnapshotCommitter testKvSnapshotStore,
                ManuallyTriggeredScheduledExecutorService manuallyTriggeredScheduledExecutorService)
                throws Exception {
            this.remoteKvTabletDir = new FsPath(remoteKvTabletDir);
            this.testKvSnapshotStore = testKvSnapshotStore;
            this.executorService = Executors.newFixedThreadPool(1);
            this.scheduledExecutorService = manuallyTriggeredScheduledExecutorService;
            closeableRegistry.registerCloseable(
                    manuallyTriggeredScheduledExecutorService::shutdownNow);
        }

        @Override
        public ZooKeeperClient getZooKeeperClient() {
            return zkClient;
        }

        @Override
        public ExecutorService getAsyncOperationsThreadPool() {
            ExecutorService executorService = Executors.newFixedThreadPool(1);

            unchecked(() -> closeableRegistry.registerCloseable(executorService::shutdownNow));
            return executorService;
        }

        @Override
        public KvSnapshotDataUploader getSnapshotDataUploader() {
            return new KvSnapshotDataUploader(executorService);
        }

        @Override
        public KvSnapshotDataDownloader getSnapshotDataDownloader() {
            return new KvSnapshotDataDownloader(executorService);
        }

        @Override
        public ScheduledExecutorService getSnapshotScheduler() {
            return scheduledExecutorService;
        }

        @Override
        public CompletedKvSnapshotCommitter getCompletedSnapshotReporter() {
            return testKvSnapshotStore;
        }

        @Override
        public long getSnapshotIntervalMs() {
            return 100;
        }

        @Override
        public int getSnapshotFsWriteBufferSize() {
            return 1024;
        }

        @Override
        public FsPath getRemoteKvDir() {
            return remoteKvTabletDir;
        }

        @Override
        public FunctionWithException<TableBucket, CompletedSnapshot, Exception>
                getLatestCompletedSnapshotProvider() {
            return testKvSnapshotStore::getLatestCompletedSnapshot;
        }

        @Override
        public int maxFetchLogSizeInRecoverKv() {
            return 1024;
        }

        private void unchecked(ThrowingRunnable<?> throwingRunnable) {
            ThrowingRunnable.unchecked(throwingRunnable).run();
        }
    }
}
