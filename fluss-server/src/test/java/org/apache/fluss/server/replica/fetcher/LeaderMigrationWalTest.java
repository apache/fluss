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

package org.apache.fluss.server.replica.fetcher;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FencedLeaderEpochException;
import org.apache.fluss.metadata.LeaderEpochOffset;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.entity.FetchLogEpochInfo;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;
import org.apache.fluss.rpc.entity.ProduceLogResultForBucket;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.messages.FetchLogRequest;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.server.coordinator.LakeCatalogDynamicLoader;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.coordinator.TestCoordinatorGateway;
import org.apache.fluss.server.coordinator.statemachine.ReplicaLeaderElection.ReassignmentLeaderElection;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrResultForBucket;
import org.apache.fluss.server.kv.KvManager;
import org.apache.fluss.server.kv.scan.ScannerManager;
import org.apache.fluss.server.kv.snapshot.TestingCompletedKvSnapshotCommitter;
import org.apache.fluss.server.log.FetchIsolation;
import org.apache.fluss.server.log.LogManager;
import org.apache.fluss.server.metadata.ClusterMetadata;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.server.storage.LocalDiskManager;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.clock.ManualClock;
import org.apache.fluss.utils.concurrent.FlussScheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.record.TestData.ANOTHER_DATA1;
import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_PHYSICAL_TABLE_PATH;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeFetchLogResponse;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsWithWriterId;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests WAL convergence through clean leader migration with controlled RPC delivery. */
class LeaderMigrationWalTest {
    @RegisterExtension
    static final AllCallbackWrapper<ZooKeeperExtension> ZK =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static final TableBucket BUCKET = new TableBucket(DATA1_TABLE_ID, 0);
    private static final List<Integer> REPLICAS = Arrays.asList(1, 2, 3);
    private final Map<Integer, Server> servers = new HashMap<>();
    private final ManualClock clock = new ManualClock(System.currentTimeMillis());
    private @TempDir File tempDir;
    private ZooKeeperClient zkClient;
    private LeaderAndIsr leaderAndIsr;

    @BeforeEach
    void setUp() throws Exception {
        zkClient = ZK.getCustomExtension().getZooKeeperClient(NOPErrorHandler.INSTANCE);
        ZK.getCustomExtension().cleanupRoot();
        zkClient.registerTable(
                DATA1_TABLE_PATH,
                TableRegistration.newTable(
                        DATA1_TABLE_ID,
                        new File(tempDir, "remote").getAbsolutePath(),
                        DATA1_TABLE_DESCRIPTOR));
        zkClient.registerFirstSchema(DATA1_TABLE_PATH, DATA1_SCHEMA);
        for (int id : REPLICAS) {
            servers.put(id, new Server(id));
        }
        leaderAndIsr = new LeaderAndIsr(1, 0, REPLICAS, Collections.emptyList(), 0, 0);
        for (int id : REPLICAS) {
            notifyRole(id);
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        for (Server server : servers.values()) {
            server.rpc.pause();
        }
        for (Server server : servers.values()) {
            server.close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testCleanMigrationReplacesUncommittedFollowerTail(boolean enabled) throws Exception {
        Configuration mode = new Configuration();
        mode.set(ConfigOptions.LOG_REPLICATION_LEADER_EPOCH_ENABLED, enabled);
        for (Server server : servers.values()) {
            server.log.reconfigure(mode);
        }
        assertMigrationConverges(false, enabled);
    }

    @Test
    void testCleanMigrationWithCaughtUpTarget() throws Exception {
        assertMigrationConverges(true);
    }

    @Test
    void testDisablingPreventsEpochTruncationOfExistingTail() throws Exception {
        append(1, DATA1, 100, 1).get(5, TimeUnit.SECONDS);
        deliver(2, 1, 0);
        List<String> copied = readWal(2);
        Configuration mode = new Configuration();
        mode.set(ConfigOptions.LOG_REPLICATION_LEADER_EPOCH_ENABLED, false);
        servers.get(2).log.reconfigure(mode);
        assertThat(replica(2).truncateFollowerToEpochOffset(0, 1, 0)).isFalse();
        assertThat(replica(2).getLocalLogEndOffset()).isEqualTo(10);
        assertThat(readWal(2)).containsExactlyElementsOf(copied);
        assertThat(replica(2).getLogTablet().lastFetchedEpoch(10)).isEqualTo(-1);
    }

    @Test
    void testLateFetchCannotModifyReplicaAfterLeaderChange() throws Exception {
        FetchLogResultForBucket late =
                FetchLogResultForBucket.records(
                                BUCKET,
                                genMemoryLogRecordsWithWriterId(DATA1, 100, 0, 0),
                                10,
                                -1,
                                -1)
                        .withEpochInfo(
                                new FetchLogEpochInfo(
                                        0,
                                        null,
                                        Collections.singletonList(new LeaderEpochOffset(0, 0))));
        moveLeader(3);
        assertThatThrownBy(() -> replica(2).appendRecordsToFollower(late, 1, 0))
                .isInstanceOf(FencedLeaderEpochException.class);
        assertThat(replica(2).getLocalLogEndOffset()).isZero();
        assertThat(replica(2).getLogHighWatermark()).isZero();
        assertThat(replica(2).getLogTablet().lastFetchedEpoch(10)).isEqualTo(-1);
    }

    @Test
    void testConvergenceAcrossConsecutiveMigrationsAndEmptyEpoch() throws Exception {
        assertMigrationConverges(false);
        List<String> prefix = readWal(2);
        moveLeader(1);
        // No records are written in this epoch before the next clean migration.
        moveLeader(3);
        CompletableFuture<List<ProduceLogResultForBucket>> appended =
                append(3, DATA1.subList(0, 5), 103, -1);
        for (int round = 0; round < 6; round++) {
            deliver(1, 3, -1);
            deliver(2, 3, -1);
            if (appended.isDone()
                    && replica(1).getLogHighWatermark() == 20
                    && replica(2).getLogHighWatermark() == 20) {
                break;
            }
        }
        assertThat(appended.get(5, TimeUnit.SECONDS))
                .containsExactly(new ProduceLogResultForBucket(BUCKET, 15, 20));
        for (int i = 0; i < 5; i++) {
            prefix.add((15 + i) + ":" + DATA1.get(i)[0] + ":" + DATA1.get(i)[1]);
        }
        for (int id : REPLICAS) {
            assertThat(readWal(id)).containsExactlyElementsOf(prefix);
            assertThat(replica(id).getLogTablet().lastFetchedEpoch(20))
                    .isEqualTo(leaderAndIsr.leaderEpoch());
        }
    }

    private void moveLeader(int target) throws Exception {
        for (Server server : servers.values()) {
            server.rpc.pause();
        }
        List<Integer> preference = new ArrayList<>(REPLICAS);
        preference.remove(Integer.valueOf(target));
        preference.add(0, target);
        leaderAndIsr =
                new ReassignmentLeaderElection(preference)
                        .leaderElection(REPLICAS, leaderAndIsr, false)
                        .get()
                        .getLeaderAndIsr();
        notifyRole(target);
        for (int id : REPLICAS) {
            if (id != target) {
                notifyRole(id);
            }
        }
        for (Server server : servers.values()) {
            server.rpc.resume();
        }
    }

    @Test
    void testLegacyReplicationRemainsAvailableWithoutInventingHistory() throws Exception {
        CompletableFuture<List<ProduceLogResultForBucket>> first = append(1, DATA1, 100, -1);
        deliver(2, 1, 0, true);
        deliver(3, 1, 0);
        deliver(2, 1, 10, true);
        deliver(3, 1, 10);
        first.get(5, TimeUnit.SECONDS);
        assertThat(replica(2).getLogTablet().lastFetchedEpoch(10)).isEqualTo(-1);

        CompletableFuture<List<ProduceLogResultForBucket>> second =
                append(1, ANOTHER_DATA1.subList(0, 5), 101, -1);
        // The upgraded peer must not infer the identity of its old prefix from the source's
        // epoch boundary, which precedes this fetch.
        deliver(2, 1, 10);
        deliver(3, 1, 10);
        deliver(2, 1, 15);
        deliver(3, 1, 15);
        second.get(5, TimeUnit.SECONDS);
        deliver(2, 1, 15);
        assertThat(replica(2).getLogTablet().lastFetchedEpoch(10)).isEqualTo(-1);
        assertThat(replica(2).getLogTablet().lastFetchedEpoch(15)).isEqualTo(-1);
        assertThat(readWal(2)).containsExactlyElementsOf(readWal(1));
    }

    private void assertMigrationConverges(boolean catchUpTarget) throws Exception {
        assertMigrationConverges(catchUpTarget, true);
    }

    private void assertMigrationConverges(boolean catchUpTarget, boolean enabled) throws Exception {
        CompletableFuture<List<ProduceLogResultForBucket>> committed = append(1, DATA1, 100, -1);
        deliver(2, 1, 0);
        deliver(3, 1, 0);
        deliver(2, 1, 10);
        deliver(3, 1, 10);
        assertThat(committed.get(5, TimeUnit.SECONDS))
                .containsExactly(new ProduceLogResultForBucket(BUCKET, 0, 10));
        assertThat(replica(1).getLogHighWatermark()).isEqualTo(10);

        // Only C receives this uncommitted tail; B remains in the ISR at the committed prefix.
        append(1, DATA1.subList(0, 5), 101, 1).get(5, TimeUnit.SECONDS);
        deliver(3, 1, 10);
        assertThat(replica(2).getLocalLogEndOffset()).isEqualTo(10);
        assertThat(replica(3).getLocalLogEndOffset()).isEqualTo(15);
        assertThat(replica(1).getLogHighWatermark()).isEqualTo(10);

        if (catchUpTarget) {
            deliver(2, 1, 10);
        }
        long newLeaderStart = catchUpTarget ? 15 : 10;
        long newLeaderEnd = newLeaderStart + 5;
        assertThat(replica(1).getIsr()).containsExactlyInAnyOrderElementsOf(REPLICAS);

        // Reassignment elects B from the live ISR. No unclean election or file mutation is used.
        for (Server server : servers.values()) {
            server.rpc.pause();
        }
        leaderAndIsr =
                new ReassignmentLeaderElection(Arrays.asList(2, 1, 3))
                        .leaderElection(REPLICAS, leaderAndIsr, false)
                        .get()
                        .getLeaderAndIsr();
        assertThat(leaderAndIsr.leader()).isEqualTo(2);
        assertThat(leaderAndIsr.isr()).containsExactlyElementsOf(REPLICAS);
        notifyRole(2);
        notifyRole(1);
        notifyRole(3);
        for (Server server : servers.values()) {
            server.rpc.resume();
        }

        CompletableFuture<List<ProduceLogResultForBucket>> replacement =
                append(2, ANOTHER_DATA1.subList(0, 5), 102, -1);
        // Allow history reconciliation, data replication, and committed watermark propagation.
        for (int round = 0; round < 6; round++) {
            deliver(1, 2, -1);
            deliver(3, 2, -1);
            if (enabled && round == 0) {
                assertThat(replica(2).getLogHighWatermark()).isLessThanOrEqualTo(newLeaderStart);
                assertThat(replacement).isNotDone();
            }
            if (replacement.isDone()
                    && replica(1).getLogHighWatermark() == newLeaderEnd
                    && replica(3).getLogHighWatermark() == newLeaderEnd) {
                break;
            }
        }
        assertThat(replacement.get(5, TimeUnit.SECONDS))
                .containsExactly(
                        new ProduceLogResultForBucket(BUCKET, newLeaderStart, newLeaderEnd));
        assertThat(replica(2).getLogHighWatermark()).isEqualTo(newLeaderEnd);
        assertThat(replica(3).getLogHighWatermark()).isEqualTo(newLeaderEnd);
        assertThat(replica(2).getIsr()).containsExactlyInAnyOrderElementsOf(REPLICAS);

        List<String> expected = new ArrayList<>();
        for (int i = 0; i < DATA1.size(); i++) {
            expected.add(i + ":" + DATA1.get(i)[0] + ":" + DATA1.get(i)[1]);
        }
        if (catchUpTarget) {
            for (int i = 0; i < 5; i++) {
                expected.add((10 + i) + ":" + DATA1.get(i)[0] + ":" + DATA1.get(i)[1]);
            }
        }
        for (int i = 0; i < 5; i++) {
            expected.add(
                    (newLeaderStart + i)
                            + ":"
                            + ANOTHER_DATA1.get(i)[0]
                            + ":"
                            + ANOTHER_DATA1.get(i)[1]);
        }
        assertThat(readWal(2)).containsExactlyElementsOf(expected);
        assertThat(readWal(1)).containsExactlyElementsOf(expected);
        if (enabled) {
            assertThat(readWal(3))
                    .as("ISR replicas must contain the acknowledged records at the same offsets")
                    .containsExactlyElementsOf(expected);
        } else {
            List<String> divergent = new ArrayList<>(expected.subList(0, 10));
            for (int i = 0; i < 5; i++) {
                divergent.add((10 + i) + ":" + DATA1.get(i)[0] + ":" + DATA1.get(i)[1]);
            }
            assertThat(readWal(3))
                    .as("Legacy replication retains the old uncommitted tail")
                    .containsExactlyElementsOf(divergent)
                    .isNotEqualTo(readWal(2));
        }
    }

    private Replica replica(int id) {
        return servers.get(id).manager.getReplicaOrException(BUCKET);
    }

    private void notifyRole(int id) throws Exception {
        CompletableFuture<List<NotifyLeaderAndIsrResultForBucket>> result =
                new CompletableFuture<>();
        servers.get(id)
                .manager
                .becomeLeaderOrFollower(
                        0,
                        Collections.singletonList(
                                new NotifyLeaderAndIsrData(
                                        DATA1_PHYSICAL_TABLE_PATH, BUCKET, REPLICAS, leaderAndIsr)),
                        result::complete);
        assertThat(result.get(5, TimeUnit.SECONDS))
                .containsExactly(new NotifyLeaderAndIsrResultForBucket(BUCKET));
    }

    private CompletableFuture<List<ProduceLogResultForBucket>> append(
            int id, List<Object[]> rows, long writerId, int acks) throws Exception {
        CompletableFuture<List<ProduceLogResultForBucket>> result = new CompletableFuture<>();
        servers.get(id)
                .manager
                .appendRecordsToLog(
                        10000,
                        acks,
                        Collections.singletonMap(
                                BUCKET, genMemoryLogRecordsWithWriterId(rows, writerId, 0, 0)),
                        null,
                        result::complete);
        return result;
    }

    private void deliver(int follower, int leader, long offset) throws Exception {
        deliver(follower, leader, offset, false);
    }

    private void deliver(int follower, int leader, long offset, boolean legacy) throws Exception {
        ControlledRpcClient rpc = servers.get(follower).rpc;
        PendingRequest pending = rpc.requests.poll(5, TimeUnit.SECONDS);
        assertThat(pending).as("fetch from server %s", follower).isNotNull();
        assertThat(pending.destination).isEqualTo(leader);
        assertThat(pending.request).isInstanceOf(FetchLogRequest.class);
        FetchLogRequest request = (FetchLogRequest) pending.request;
        if (offset >= 0) {
            assertThat(
                            request.getTablesReqsList()
                                    .get(0)
                                    .getBucketsReqsList()
                                    .get(0)
                                    .getFetchOffset())
                    .isEqualTo(offset);
        }
        FetchLogRequest serverRequest = new FetchLogRequest().copyFrom(request);
        if (legacy) {
            serverRequest
                    .getTablesReqsList()
                    .forEach(
                            table ->
                                    table.getBucketsReqsList()
                                            .forEach(
                                                    bucket -> {
                                                        bucket.clearCurrentLeaderEpoch();
                                                        bucket.clearLastFetchedEpoch();
                                                    }));
        }
        TestingLeaderEndpoint endpoint =
                new TestingLeaderEndpoint(
                        servers.get(leader).conf,
                        servers.get(leader).manager,
                        new ServerNode(
                                follower, "localhost", 10000 + follower, ServerType.TABLET_SERVER),
                        leader);
        LeaderEndpoint.FetchData data =
                endpoint.fetchLog(
                                new FetchLogContext(
                                        Collections.singletonMap(DATA1_TABLE_ID, DATA1_TABLE_PATH),
                                        serverRequest))
                        .get(5, TimeUnit.SECONDS);
        pending.result.complete(makeFetchLogResponse(data.getFetchLogResultMap()));
        // The next request proves the actual background fetcher has consumed this response.
        retry(Duration.ofSeconds(5), () -> assertThat(rpc.requests).isNotEmpty());
    }

    private List<String> readWal(int id) throws Exception {
        List<String> records = new ArrayList<>();
        try (LogRecordReadContext context =
                LogRecordReadContext.createArrowReadContext(
                        DATA1_ROW_TYPE,
                        DEFAULT_SCHEMA_ID,
                        new TestingSchemaGetter(DEFAULT_SCHEMA_ID, DATA1_SCHEMA))) {
            for (LogRecordBatch batch :
                    replica(id)
                            .getLogTablet()
                            .read(0, Integer.MAX_VALUE, FetchIsolation.HIGH_WATERMARK, true)
                            .getRecords()
                            .batches()) {
                batch.ensureValid();
                try (CloseableIterator<LogRecord> iterator = batch.records(context)) {
                    while (iterator.hasNext()) {
                        LogRecord record = iterator.next();
                        records.add(
                                record.logOffset()
                                        + ":"
                                        + record.getRow().getInt(0)
                                        + ":"
                                        + record.getRow().getString(1));
                    }
                }
            }
        }
        return records;
    }

    private static final class PendingRequest {
        private final int destination;
        private final ApiMessage request;
        private final CompletableFuture<ApiMessage> result = new CompletableFuture<>();

        private PendingRequest(int destination, ApiMessage request) {
            this.destination = destination;
            this.request = request;
        }
    }

    /** Holds RPC requests until the test delivers them to the real destination ReplicaManager. */
    private static final class ControlledRpcClient implements RpcClient {
        private final BlockingQueue<PendingRequest> requests = new LinkedBlockingQueue<>();
        private boolean paused;

        @Override
        public synchronized CompletableFuture<ApiMessage> sendRequest(
                ServerNode node, ApiKeys apiKey, ApiMessage request) {
            PendingRequest pending = new PendingRequest(node.id(), request);
            if (paused) {
                pending.result.completeExceptionally(new IOException("Connection paused"));
            } else {
                requests.add(pending);
            }
            return pending.result;
        }

        synchronized void pause() {
            paused = true;
            PendingRequest request;
            while ((request = requests.poll()) != null) {
                request.result.completeExceptionally(new IOException("Connection paused"));
            }
        }

        synchronized void resume() {
            paused = false;
        }

        @Override
        public boolean connect(ServerNode node) {
            return true;
        }

        @Override
        public boolean isReady(String serverUid) {
            return true;
        }

        @Override
        public CompletableFuture<Void> disconnect(String serverUid) {
            pause();
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() {
            pause();
        }
    }

    private final class Server implements AutoCloseable {
        private final Configuration conf = new Configuration();
        private final ControlledRpcClient rpc = new ControlledRpcClient();
        private final FlussScheduler scheduler = new FlussScheduler(2);
        private final ExecutorService io = Executors.newSingleThreadExecutor();
        private final LocalDiskManager disk;
        private final LogManager log;
        private final KvManager kv;
        private final ScannerManager scanner;
        private final ReplicaManager manager;

        private Server(int id) throws Exception {
            conf.set(ConfigOptions.TABLET_SERVER_ID, id);
            conf.setString(
                    ConfigOptions.DATA_DIR, new File(tempDir, "server-" + id).getAbsolutePath());
            conf.set(ConfigOptions.REMOTE_DATA_DIR, new File(tempDir, "remote").getAbsolutePath());
            conf.set(ConfigOptions.SERVER_DATA_DISK_WRITE_LIMIT_RATIO, 1.0);
            conf.set(ConfigOptions.LOG_REPLICA_FETCH_BACKOFF_INTERVAL, Duration.ofMillis(10));
            conf.set(ConfigOptions.LOG_REPLICA_FETCH_WAIT_MAX_TIME, Duration.ZERO);
            scheduler.startup();
            disk = LocalDiskManager.create(conf);
            log =
                    LogManager.create(
                            conf,
                            zkClient,
                            scheduler,
                            clock,
                            TestingMetricGroups.TABLET_SERVER_METRICS,
                            disk);
            log.startup();
            kv =
                    KvManager.create(
                            conf,
                            zkClient,
                            log,
                            TestingMetricGroups.TABLET_SERVER_METRICS,
                            disk,
                            null,
                            clock);
            kv.startup();
            scanner = new ScannerManager(conf, scheduler);
            TabletServerMetadataCache metadata =
                    new TabletServerMetadataCache(
                            new MetadataManager(
                                    zkClient,
                                    conf,
                                    new LakeCatalogDynamicLoader(conf, null, true)));
            List<ServerInfo> members = new ArrayList<>();
            for (int member : REPLICAS) {
                members.add(
                        new ServerInfo(
                                member,
                                null,
                                Endpoint.fromListenersString(
                                        "FLUSS://localhost:" + (10000 + member)),
                                ServerType.TABLET_SERVER));
            }
            metadata.updateClusterMetadata(
                    new ClusterMetadata(
                            new ServerInfo(
                                    0,
                                    null,
                                    Endpoint.fromListenersString("FLUSS://localhost:9999"),
                                    ServerType.COORDINATOR),
                            new HashSet<>(members)));
            manager =
                    new ReplicaManager(
                            conf,
                            scheduler,
                            log,
                            kv,
                            zkClient,
                            id,
                            metadata,
                            rpc,
                            new TestCoordinatorGateway(),
                            new TestingCompletedKvSnapshotCommitter(),
                            NOPErrorHandler.INSTANCE,
                            TestingMetricGroups.TABLET_SERVER_METRICS,
                            TestingMetricGroups.USER_METRICS,
                            scanner,
                            clock,
                            io,
                            disk,
                            null);
            manager.startup();
        }

        @Override
        public void close() throws Exception {
            manager.shutdown();
            manager.getRemoteLogManager().close();
            scanner.close();
            kv.shutdown();
            log.shutdown();
            scheduler.shutdown();
            disk.close();
            io.shutdownNow();
        }
    }
}
