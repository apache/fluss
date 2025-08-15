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

package com.alibaba.fluss.server.zk;

import com.alibaba.fluss.cluster.Endpoint;
import com.alibaba.fluss.cluster.TabletServerInfo;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.SchemaInfo;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePartition;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.server.entity.RegisterTableBucketLeadAndIsrInfo;
import com.alibaba.fluss.server.zk.data.BucketAssignment;
import com.alibaba.fluss.server.zk.data.BucketSnapshot;
import com.alibaba.fluss.server.zk.data.CoordinatorAddress;
import com.alibaba.fluss.server.zk.data.LeaderAndIsr;
import com.alibaba.fluss.server.zk.data.PartitionAssignment;
import com.alibaba.fluss.server.zk.data.TableAssignment;
import com.alibaba.fluss.server.zk.data.TableRegistration;
import com.alibaba.fluss.server.zk.data.TabletServerRegistration;
import com.alibaba.fluss.testutils.common.AllCallbackWrapper;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.utils.types.Tuple2;

import org.apache.fluss.shaded.curator5.org.apache.curator.CuratorZookeeperClient;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.ZooKeeper;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.client.ZKClientConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.fluss.server.utils.TableAssignmentUtils.generateAssignment;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link ZooKeeperClient}. */
class ZooKeeperClientTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static ZooKeeperClient zookeeperClient;

    @BeforeAll
    static void beforeAll() {
        zookeeperClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
    }

    @AfterEach
    void afterEach() {
        ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().cleanupRoot();
    }

    @AfterAll
    static void afterAll() {
        zookeeperClient.close();
    }

    @Test
    void testCoordinatorLeader() throws Exception {
        // try to get leader address, should return empty since node leader address stored in
        // zk
        assertThat(zookeeperClient.getCoordinatorAddress()).isEmpty();
        CoordinatorAddress coordinatorAddress =
                new CoordinatorAddress(
                        "2", Endpoint.fromListenersString("CLIENT://localhost1:10012"));
        // register leader address
        zookeeperClient.registerCoordinatorLeader(coordinatorAddress);
        // check get leader address
        CoordinatorAddress gottenAddress = zookeeperClient.getCoordinatorAddress().get();
        assertThat(gottenAddress).isEqualTo(coordinatorAddress);
    }

    @Test
    void testTabletServer() throws Exception {
        // try to get tablet server, should return empty
        assertThat(zookeeperClient.getTabletServer(1)).isEmpty();
        assertThat(zookeeperClient.getSortedTabletServerList()).isEmpty();
        // register two table servers
        TabletServerRegistration registration1 =
                new TabletServerRegistration(
                        "rack1",
                        Endpoint.fromListenersString("CLIENT://host1:3456"),
                        System.currentTimeMillis());
        TabletServerRegistration registration2 =
                new TabletServerRegistration(
                        "rack2",
                        Endpoint.fromListenersString("CLIENT://host2:3454"),
                        System.currentTimeMillis());
        zookeeperClient.registerTabletServer(2, registration2);
        zookeeperClient.registerTabletServer(1, registration1);
        // now get the tablet servers
        assertThat(zookeeperClient.getSortedTabletServerList()).isEqualTo(new int[] {1, 2});
        // get tablet server1
        assertThat(zookeeperClient.getTabletServer(1)).contains(registration1);
        assertThat(zookeeperClient.getTabletServer(2)).contains(registration2);
    }

    @Test
    void testTabletAssignments() throws Exception {
        long tableId = 1;
        // try to get tablet assignment, should return empty
        assertThat(zookeeperClient.getTableAssignment(tableId)).isEmpty();

        TableAssignment tableAssignment =
                TableAssignment.builder()
                        .add(0, BucketAssignment.of(1, 4, 5))
                        .add(1, BucketAssignment.of(2, 3))
                        .build();
        zookeeperClient.registerTableAssignment(tableId, tableAssignment);
        assertThat(zookeeperClient.getTableAssignment(tableId)).contains(tableAssignment);

        // test update
        TableAssignment tableAssignment2 =
                TableAssignment.builder().add(3, BucketAssignment.of(1, 5)).build();
        zookeeperClient.updateTableAssignment(tableId, tableAssignment2);
        assertThat(zookeeperClient.getTableAssignment(tableId)).contains(tableAssignment2);

        // test delete
        zookeeperClient.deleteTableAssignment(tableId);
        assertThat(zookeeperClient.getTableAssignment(tableId)).isEmpty();
    }

    @Test
    void testLeaderAndIsr() throws Exception {
        // try to get bucket leadership, should return empty
        TableBucket tableBucket = new TableBucket(1, 1);
        assertThat(zookeeperClient.getLeaderAndIsr(tableBucket)).isEmpty();

        // try to register bucket leaderAndIsr
        LeaderAndIsr leaderAndIsr = new LeaderAndIsr(1, 10, Arrays.asList(1, 2, 3), 100, 1000);
        zookeeperClient.registerLeaderAndIsr(tableBucket, leaderAndIsr);
        assertThat(zookeeperClient.getLeaderAndIsr(tableBucket)).hasValue(leaderAndIsr);

        // test update
        leaderAndIsr = new LeaderAndIsr(2, 20, Collections.emptyList(), 200, 2000);
        zookeeperClient.updateLeaderAndIsr(tableBucket, leaderAndIsr);
        assertThat(zookeeperClient.getLeaderAndIsr(tableBucket)).hasValue(leaderAndIsr);

        // test delete
        zookeeperClient.deleteLeaderAndIsr(tableBucket);
        assertThat(zookeeperClient.getLeaderAndIsr(tableBucket)).isEmpty();
    }

    @Test
    void testBatchCreateLeaderAndIsr() throws Exception {
        List<RegisterTableBucketLeadAndIsrInfo> noPartitionTableBucket = new ArrayList<>();
        // non-partition table
        List<LeaderAndIsr> noPartitionleaderAndIsrList = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            TableBucket tableBucket = new TableBucket(1, i);
            LeaderAndIsr leaderAndIsr =
                    new LeaderAndIsr(i, 10, Arrays.asList(i + 1, i + 2, i + 3), 100, 1000);
            noPartitionleaderAndIsrList.add(leaderAndIsr);
            noPartitionTableBucket.add(
                    new RegisterTableBucketLeadAndIsrInfo(tableBucket, leaderAndIsr, null, null));
        }
        zookeeperClient.batchRegisterLeaderAndIsrForTablePartition(noPartitionTableBucket);

        for (int i = 0; i < 100; i++) {
            Optional<LeaderAndIsr> optionalLeaderAndIsr =
                    zookeeperClient.getLeaderAndIsr(noPartitionTableBucket.get(i).getTableBucket());
            assertThat(optionalLeaderAndIsr.isPresent()).isTrue();
            assertThat(optionalLeaderAndIsr.get()).isIn(noPartitionleaderAndIsrList);
        }

        List<RegisterTableBucketLeadAndIsrInfo> partitionTableBucket = new ArrayList<>();
        // partition table
        List<LeaderAndIsr> partitionleaderAndIsrList = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            TableBucket tableBucket = new TableBucket(1, 2L, i);
            LeaderAndIsr leaderAndIsr =
                    new LeaderAndIsr(i, 10, Arrays.asList(i + 1, i + 2, i + 3), 100, 1000);
            partitionleaderAndIsrList.add(leaderAndIsr);
            partitionTableBucket.add(
                    new RegisterTableBucketLeadAndIsrInfo(
                            tableBucket, leaderAndIsr, "partition" + i, null));
        }

        zookeeperClient.batchRegisterLeaderAndIsrForTablePartition(partitionTableBucket);
        for (int i = 0; i < 100; i++) {
            Optional<LeaderAndIsr> optionalLeaderAndIsr =
                    zookeeperClient.getLeaderAndIsr(partitionTableBucket.get(i).getTableBucket());
            assertThat(optionalLeaderAndIsr.isPresent()).isTrue();
            assertThat(optionalLeaderAndIsr.get()).isIn(partitionleaderAndIsrList);
        }
    }

    @Test
    void testBatchUpdateLeaderAndIsr() throws Exception {
        int totalCount = 100;

        // try to register bucket leaderAndIsr
        Map<TableBucket, LeaderAndIsr> leaderAndIsrList = new HashMap<>();
        for (int i = 0; i < totalCount; i++) {
            TableBucket tableBucket = new TableBucket(1, i);
            LeaderAndIsr leaderAndIsr =
                    new LeaderAndIsr(i, 10, Arrays.asList(i + 1, i + 2, i + 3), 100, 1000);
            leaderAndIsrList.put(tableBucket, leaderAndIsr);
            zookeeperClient.registerLeaderAndIsr(tableBucket, leaderAndIsr);
        }

        // try to batch update
        Map<TableBucket, LeaderAndIsr> updateLeaderAndIsrList =
                leaderAndIsrList.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        entry -> {
                                            LeaderAndIsr old = entry.getValue();
                                            return new LeaderAndIsr(
                                                    old.leader() + 1,
                                                    old.leaderEpoch() + 1,
                                                    old.isr(),
                                                    old.coordinatorEpoch() + 1,
                                                    old.bucketEpoch() + 1);
                                        }));
        zookeeperClient.batchUpdateLeaderAndIsr(updateLeaderAndIsrList);
        for (Map.Entry<TableBucket, LeaderAndIsr> entry : updateLeaderAndIsrList.entrySet()) {
            TableBucket tableBucket = entry.getKey();
            LeaderAndIsr leaderAndIsr = entry.getValue();
            assertThat(zookeeperClient.getLeaderAndIsr(tableBucket)).hasValue(leaderAndIsr);
            zookeeperClient.deleteLeaderAndIsr(tableBucket);
            assertThat(zookeeperClient.getLeaderAndIsr(tableBucket)).isEmpty();
        }
    }

    @Test
    void testTable() throws Exception {
        TablePath tablePath = TablePath.of("db", "tb");
        assertThat(zookeeperClient.getTable(tablePath)).isEmpty();

        // register table.
        Map<String, String> options = new HashMap<>();
        options.put("option-1", "100");
        options.put("option-2", "200");
        long currentMillis = System.currentTimeMillis();
        TableRegistration tableReg =
                new TableRegistration(
                        11,
                        "first table",
                        Arrays.asList("a", "b"),
                        new TableDescriptor.TableDistribution(16, Collections.singletonList("a")),
                        options,
                        Collections.singletonMap("custom-1", "100"),
                        currentMillis,
                        currentMillis);

        zookeeperClient.registerTable(tablePath, tableReg);
        Optional<TableRegistration> optionalTable = zookeeperClient.getTable(tablePath);
        assertThat(optionalTable.isPresent()).isTrue();
        assertThat(optionalTable.get()).isEqualTo(tableReg);

        // update table.
        currentMillis = System.currentTimeMillis();
        tableReg =
                new TableRegistration(
                        22,
                        "second table",
                        Arrays.asList("a", "b"),
                        new TableDescriptor.TableDistribution(16, Collections.singletonList("a")),
                        options,
                        Collections.singletonMap("custom-2", "200"),
                        currentMillis,
                        currentMillis);
        zookeeperClient.updateTable(tablePath, tableReg);
        optionalTable = zookeeperClient.getTable(tablePath);
        assertThat(optionalTable.isPresent()).isTrue();
        assertThat(optionalTable.get()).isEqualTo(tableReg);

        // delete table.
        zookeeperClient.deleteTable(tablePath);
        assertThat(zookeeperClient.getTable(tablePath)).isEmpty();
    }

    @Test
    void testSchema() throws Exception {
        int schemaId = 1;
        TablePath tablePath = TablePath.of("db", "tb");
        assertThat(zookeeperClient.getSchemaById(tablePath, schemaId)).isEmpty();

        // register first version schema.
        Schema.Builder newBuilder = Schema.newBuilder();
        Schema schema =
                newBuilder
                        .column("a", DataTypes.INT())
                        .withComment("a is first column")
                        .column("b", DataTypes.STRING())
                        .withComment("b is second column")
                        .column("c", DataTypes.CHAR(10))
                        .withComment("c is third column")
                        .primaryKey("a")
                        .build();
        int registeredSchemaId = zookeeperClient.registerSchema(tablePath, schema);
        assertThat(registeredSchemaId).isEqualTo(schemaId);
        assertThat(zookeeperClient.getCurrentSchemaId(tablePath)).isEqualTo(schemaId);

        Optional<SchemaInfo> schemaInfo = zookeeperClient.getSchemaById(tablePath, schemaId);
        assertThat(schemaInfo.isPresent()).isTrue();
        assertThat(schemaInfo.get().getSchema()).isEqualTo(schema);
        assertThat(schemaInfo.get().getSchemaId()).isEqualTo(schemaId);

        // register second version schema.
        newBuilder = Schema.newBuilder();
        Schema schema2 =
                newBuilder
                        .column("a", DataTypes.INT())
                        .withComment("a is first column")
                        .column("b", DataTypes.STRING())
                        .withComment("b is second column")
                        .primaryKey("a")
                        .build();
        registeredSchemaId = zookeeperClient.registerSchema(tablePath, schema2);
        assertThat(registeredSchemaId).isEqualTo(2);
        assertThat(zookeeperClient.getCurrentSchemaId(tablePath)).isEqualTo(2);

        schemaInfo = zookeeperClient.getSchemaById(tablePath, 2);
        assertThat(schemaInfo.isPresent()).isTrue();
        assertThat(schemaInfo.get().getSchema()).isEqualTo(schema2);
        assertThat(schemaInfo.get().getSchemaId()).isEqualTo(2);
    }

    @Test
    void testGetTableIdAndIncrement() throws Exception {
        // init
        int firstN = 10;
        for (int i = 0; i < firstN; i++) {
            assertThat(zookeeperClient.getTableIdAndIncrement()).isEqualTo(i);
        }

        // restart to check we can still get the expected value
        ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().restart();
        for (int i = 0; i < 10; i++) {
            assertThat(zookeeperClient.getTableIdAndIncrement()).isEqualTo(i + firstN);
        }
    }

    @Test
    void testTableBucketSnapshot() throws Exception {
        TableBucket table1Bucket2 = new TableBucket(1, 2);
        // first register the assignment for table 1
        zookeeperClient.registerTableAssignment(
                table1Bucket2.getTableId(),
                TableAssignment.builder()
                        .add(table1Bucket2.getBucket(), BucketAssignment.of(0, 1, 2))
                        .build());
        BucketSnapshot snapshot1 = new BucketSnapshot(1L, 10L, "oss://test/cp1");
        BucketSnapshot snapshot2 = new BucketSnapshot(2L, 20L, "oss://test/cp2");
        zookeeperClient.registerTableBucketSnapshot(table1Bucket2, snapshot1);
        zookeeperClient.registerTableBucketSnapshot(table1Bucket2, snapshot2);
        assertThat(zookeeperClient.getTableBucketSnapshot(table1Bucket2, 1).get())
                .isEqualTo(snapshot1);
        assertThat(zookeeperClient.getTableBucketSnapshot(table1Bucket2, 2).get())
                .isEqualTo(snapshot2);
        TableBucket table2Bucket2 = new TableBucket(2, 2);
        BucketSnapshot snapshot21 = new BucketSnapshot(1L, 11L, "oss://test/cp21");
        zookeeperClient.registerTableBucketSnapshot(table2Bucket2, snapshot21);
        final List<Tuple2<BucketSnapshot, Long>> table1Bucket2AllSnapshotAndIds =
                zookeeperClient.getTableBucketAllSnapshotAndIds(table1Bucket2);
        assertThat(table1Bucket2AllSnapshotAndIds)
                .containsExactlyInAnyOrderElementsOf(
                        Arrays.asList(Tuple2.of(snapshot1, 1L), Tuple2.of(snapshot2, 2L)));

        // check snapshots for table2Bucket2
        final List<Tuple2<BucketSnapshot, Long>> table2Bucket2AllSnapshotAndIds =
                zookeeperClient.getTableBucketAllSnapshotAndIds(table2Bucket2);
        assertThat(table2Bucket2AllSnapshotAndIds)
                .containsExactlyInAnyOrderElementsOf(
                        Collections.singletonList(Tuple2.of(snapshot21, 1L)));

        // check all table buckets' snapshots for table 1;
        Map<Integer, Optional<BucketSnapshot>> tableBucketsLatestSnapshot =
                zookeeperClient.getTableLatestBucketSnapshot(table1Bucket2.getTableId());
        Map<Integer, Optional<BucketSnapshot>> expectedTableBucketsLatestSnapshot =
                Collections.singletonMap(table1Bucket2.getBucket(), Optional.of(snapshot2));
        assertThat(tableBucketsLatestSnapshot).isEqualTo(expectedTableBucketsLatestSnapshot);

        // now, delete snapshot1/snapshot2 for tableBucket
        zookeeperClient.deleteTableBucketSnapshot(table1Bucket2, 1);
        zookeeperClient.deleteTableBucketSnapshot(table1Bucket2, 2);
        assertThat(zookeeperClient.getTableBucketAllSnapshotAndIds(table1Bucket2)).isEmpty();
        assertThat(zookeeperClient.getTableBucketSnapshot(table1Bucket2, 1)).isEmpty();
    }

    @Test
    void testGetWriterIdAndIncrement() throws Exception {
        // init
        int firstN = 10;
        for (int i = 0; i < firstN; i++) {
            assertThat(zookeeperClient.getWriterIdAndIncrement()).isEqualTo(i);
        }

        // restart to check we can still get the expected value
        ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().restart();
        for (int i = 0; i < 10; i++) {
            assertThat(zookeeperClient.getWriterIdAndIncrement()).isEqualTo(i + firstN);
        }
    }

    @Test
    void testPartition() throws Exception {
        // first create a table
        TablePath tablePath = TablePath.of("db", "tb");
        long tableId = 12;
        long currentMillis = System.currentTimeMillis();
        TableRegistration tableReg =
                new TableRegistration(
                        tableId,
                        "partitioned table",
                        Arrays.asList("a", "b"),
                        new TableDescriptor.TableDistribution(16, Collections.singletonList("a")),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        currentMillis,
                        currentMillis);
        zookeeperClient.registerTable(tablePath, tableReg);

        Set<String> partitions = zookeeperClient.getPartitions(tablePath);
        assertThat(partitions).isEmpty();

        // test create new partitions
        PartitionAssignment partitionAssignment =
                new PartitionAssignment(
                        tableId,
                        generateAssignment(
                                        3,
                                        3,
                                        new TabletServerInfo[] {
                                            new TabletServerInfo(0, "rack0"),
                                            new TabletServerInfo(1, "rack1"),
                                            new TabletServerInfo(2, "rack2")
                                        })
                                .getBucketAssignments());
        zookeeperClient.registerPartitionAssignmentAndMetadata(
                1L, "p1", partitionAssignment, tablePath, tableId);
        zookeeperClient.registerPartitionAssignmentAndMetadata(
                2L, "p2", partitionAssignment, tablePath, tableId);

        // check created partitions
        partitions = zookeeperClient.getPartitions(tablePath);
        assertThat(partitions).containsExactly("p1", "p2");
        TablePartition partition = zookeeperClient.getPartition(tablePath, "p1").get();
        assertThat(partition.getPartitionId()).isEqualTo(1L);
        partition = zookeeperClient.getPartition(tablePath, "p2").get();
        assertThat(partition.getPartitionId()).isEqualTo(2L);

        // test delete partition
        zookeeperClient.deletePartition(tablePath, "p1");
        partitions = zookeeperClient.getPartitions(tablePath);
        assertThat(partitions).containsExactly("p2");
    }

    @Test
    void testZookeeperConfigPath() throws Exception {
        final Configuration config = new Configuration();
        config.setString(
                ConfigOptions.ZOOKEEPER_ADDRESS,
                ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().getConnectString());
        config.setString(ConfigOptions.ZOOKEEPER_CONFIG_PATH, "./no-file.properties");
        assertThatThrownBy(
                        () -> ZooKeeperUtils.startZookeeperClient(config, NOPErrorHandler.INSTANCE))
                .isExactlyInstanceOf(RuntimeException.class)
                .hasMessageContaining("Fail to load zookeeper client config from path");

        config.setString(
                ConfigOptions.ZOOKEEPER_CONFIG_PATH,
                getClass().getClassLoader().getResource("zk.properties").getPath());
        try (ZooKeeperClient zookeeperClient =
                        ZooKeeperUtils.startZookeeperClient(config, NOPErrorHandler.INSTANCE);
                CuratorFramework curatorClient = zookeeperClient.getCuratorClient();
                CuratorZookeeperClient curatorZookeeperClient = curatorClient.getZookeeperClient();
                ZooKeeper zooKeeper = curatorZookeeperClient.getZooKeeper()) {
            ZKClientConfig clientConfig = zooKeeper.getClientConfig();
            assertThat(clientConfig.getProperty(ZKClientConfig.ENABLE_CLIENT_SASL_KEY))
                    .isEqualTo("true");
            assertThat(clientConfig.getProperty(ZKClientConfig.LOGIN_CONTEXT_NAME_KEY))
                    .isEqualTo("ZookeeperClient");
            assertThat(clientConfig.getProperty(ZKClientConfig.ZK_SASL_CLIENT_USERNAME))
                    .isEqualTo("zookeeper2");
        }
    }
}
