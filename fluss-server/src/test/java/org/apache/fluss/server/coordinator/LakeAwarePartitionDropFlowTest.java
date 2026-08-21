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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.FreezePartitionRequest;
import org.apache.fluss.rpc.messages.FreezePartitionResponse;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.server.coordinator.remote.RemoteDirDynamicLoader;
import org.apache.fluss.server.entity.FreezePartitionResultForBucket;
import org.apache.fluss.server.metadata.CoordinatorMetadataCache;
import org.apache.fluss.server.tablet.TestTabletServerGateway;
import org.apache.fluss.server.testutils.TestingServerMetadataCache;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.server.zk.data.ZkVersion;
import org.apache.fluss.server.zk.data.lake.LakeTableHelper;
import org.apache.fluss.server.zk.data.lake.LakeTableSnapshot;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.testutils.common.ManuallyTriggeredScheduledExecutorService;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.clock.ManualClock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getFreezePartitionData;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeFreezePartitionResponse;
import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end component test for lake-aware auto-partition dropping. */
class LakeAwarePartitionDropFlowTest {

    private static final String REMOTE_DATA_DIR = "/dir";
    private static final long TABLE_ID = 1L;
    private static final long FROZEN_OFFSET = 42L;
    private static final String EXPIRED_PARTITION = "2024091000";

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static ZooKeeperClient zooKeeperClient;

    @BeforeAll
    static void beforeAll() {
        zooKeeperClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
    }

    @AfterEach
    void afterEach() {
        ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().cleanupRoot();
    }

    @Test
    void testExpiredPartitionIsDroppedAfterFrozenOffsetsAreTiered() throws Exception {
        MetadataManager metadataManager =
                new MetadataManager(
                        zooKeeperClient,
                        new Configuration(),
                        new LakeCatalogDynamicLoader(new Configuration(), null, true));
        TableInfo tableInfo = createTable();
        PartitionRegistration partitionRegistration =
                createExpiredPartition(metadataManager, tableInfo);
        TableBucket tableBucket =
                new TableBucket(TABLE_ID, partitionRegistration.getPartitionId(), 0);
        registerLeader(tableBucket);

        StableFreezeGateway freezeGateway = new StableFreezeGateway(FROZEN_OFFSET);
        TestCoordinatorChannelManager channelManager =
                new TestCoordinatorChannelManager(Collections.singletonMap(0, freezeGateway));
        LakeAwarePartitionDropManager dropManager =
                new LakeAwarePartitionDropManager(
                        metadataManager, zooKeeperClient, channelManager, Runnable::run, 30_000L);
        ManuallyTriggeredScheduledExecutorService periodicExecutor =
                new ManuallyTriggeredScheduledExecutorService();
        AutoPartitionManager autoPartitionManager =
                new AutoPartitionManager(
                        new TestingServerMetadataCache(1),
                        metadataManager,
                        remoteDirDynamicLoader(),
                        new Configuration(),
                        disabledCapacityController(),
                        new ManualClock(
                                LocalDateTime.parse("2024-09-10T03:00:00")
                                        .atZone(ZoneId.systemDefault())
                                        .toInstant()
                                        .toEpochMilli()),
                        periodicExecutor,
                        dropManager);

        try {
            autoPartitionManager.start();
            autoPartitionManager.addAutoPartitionTable(tableInfo, true);
            periodicExecutor.triggerNonPeriodicScheduledTask();

            Optional<PartitionRegistration> frozenRegistration =
                    zooKeeperClient.getPartition(tableInfo.getTablePath(), EXPIRED_PARTITION);
            assertThat(frozenRegistration).isPresent();
            assertThat(frozenRegistration.get().isFrozen()).isTrue();
            assertThat(freezeGateway.getFreezeRequests()).hasSize(1);
            assertThat(getFreezePartitionData(freezeGateway.getFreezeRequests().get(0)))
                    .containsEntry(tableBucket, 1);

            LakeTableHelper lakeTableHelper = new LakeTableHelper(zooKeeperClient, REMOTE_DATA_DIR);
            lakeTableHelper.registerLakeTableSnapshotV1(
                    TABLE_ID,
                    new LakeTableSnapshot(
                            1L, Collections.singletonMap(tableBucket, FROZEN_OFFSET - 1)));
            periodicExecutor.triggerPeriodicScheduledTasks();
            assertThat(zooKeeperClient.getPartition(tableInfo.getTablePath(), EXPIRED_PARTITION))
                    .isPresent();

            lakeTableHelper.registerLakeTableSnapshotV1(
                    TABLE_ID,
                    new LakeTableSnapshot(
                            2L, Collections.singletonMap(tableBucket, FROZEN_OFFSET)));
            periodicExecutor.triggerPeriodicScheduledTasks();

            assertThat(zooKeeperClient.getPartition(tableInfo.getTablePath(), EXPIRED_PARTITION))
                    .isEmpty();
            assertThat(freezeGateway.getFreezeRequests()).hasSize(1);
        } finally {
            autoPartitionManager.close();
            channelManager.close();
        }
    }

    private TableInfo createTable() throws Exception {
        TablePath tablePath = TablePath.of("db", "lake_aware_partition_drop_flow");
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .column("dt", DataTypes.STRING())
                                        .primaryKey("id", "dt")
                                        .build())
                        .distributedBy(1)
                        .partitionedBy("dt")
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 1)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_KEY, "dt")
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                AutoPartitionTimeUnit.HOUR)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_NUM_RETENTION, 1)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE, 0)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_DROP_ENSURE_TIERED, true)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .build();
        long now = System.currentTimeMillis();
        TableInfo tableInfo =
                TableInfo.of(tablePath, TABLE_ID, 1, descriptor, REMOTE_DATA_DIR, now, now);
        zooKeeperClient.registerTable(
                tablePath, TableRegistration.newTable(TABLE_ID, REMOTE_DATA_DIR, descriptor));
        return tableInfo;
    }

    private PartitionRegistration createExpiredPartition(
            MetadataManager metadataManager, TableInfo tableInfo) throws Exception {
        Map<Integer, BucketAssignment> bucketAssignments = new HashMap<>();
        bucketAssignments.put(0, BucketAssignment.of(0));
        metadataManager.createPartition(
                tableInfo.getTablePath(),
                TABLE_ID,
                REMOTE_DATA_DIR,
                new PartitionAssignment(TABLE_ID, bucketAssignments),
                ResolvedPartitionSpec.fromPartitionName(
                        tableInfo.getPartitionKeys(), EXPIRED_PARTITION),
                false);
        return zooKeeperClient.getPartition(tableInfo.getTablePath(), EXPIRED_PARTITION).get();
    }

    private void registerLeader(TableBucket tableBucket) throws Exception {
        zooKeeperClient.registerLeaderAndIsr(
                tableBucket,
                new LeaderAndIsr(0, 1, Collections.singletonList(0), Collections.emptyList(), 0, 0),
                ZkVersion.MATCH_ANY_VERSION.getVersion());
    }

    private static RemoteDirDynamicLoader remoteDirDynamicLoader() {
        Configuration configuration = new Configuration();
        configuration.set(ConfigOptions.REMOTE_DATA_DIR, REMOTE_DATA_DIR);
        return new RemoteDirDynamicLoader(configuration);
    }

    private static ReplicaCapacityController disabledCapacityController() {
        Configuration configuration = new Configuration();
        configuration.set(ConfigOptions.KV_LEADER_REPLICA_MEMORY_RESERVED, MemorySize.ZERO);
        return new ReplicaCapacityController(configuration, new CoordinatorMetadataCache());
    }

    private static final class StableFreezeGateway extends TestTabletServerGateway {
        private final long frozenOffset;
        private final List<FreezePartitionRequest> freezeRequests = new ArrayList<>();

        private StableFreezeGateway(long frozenOffset) {
            super(false, Collections.<ApiKeys>emptySet());
            this.frozenOffset = frozenOffset;
        }

        @Override
        public CompletableFuture<FreezePartitionResponse> freezePartition(
                FreezePartitionRequest request) {
            freezeRequests.add(request);
            List<FreezePartitionResultForBucket> results = new ArrayList<>();
            for (TableBucket tableBucket : getFreezePartitionData(request).keySet()) {
                results.add(
                        new FreezePartitionResultForBucket(
                                tableBucket, frozenOffset, frozenOffset));
            }
            return CompletableFuture.completedFuture(makeFreezePartitionResponse(results));
        }

        private List<FreezePartitionRequest> getFreezeRequests() {
            return freezeRequests;
        }
    }
}
