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

package org.apache.fluss.client.table;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.KvRecordTestUtils;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.server.coordinator.CoordinatorServer;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.RpcMessageTestUtils;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.CoordinatorAddress;
import org.apache.fluss.server.zk.data.ZkData;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.util.Collections;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

class FirstRowEmptyWalITCase {
    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(clusterConfig())
                    .build();

    private static Configuration clusterConfig() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        return conf;
    }

    @ParameterizedTest
    @EnumSource(
            value = LogFormat.class,
            names = {"ARROW", "COMPACTED"})
    void testClientWritesAndFailover(LogFormat format) throws Exception {
        TablePath path = TablePath.of("empty_wal", "client_" + format.name().toLowerCase());
        long tableId = createTable(path, format);
        TableBucket bucket = new TableBucket(tableId, 0);
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Table table = connection.getTable(path)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(row(1, "first"));
            writer.flush();
            long end = CLUSTER.waitAndGetLeaderReplica(bucket).getLogTablet().localLogEndOffset();
            for (int i = 0; i < 6; i++) {
                writer.upsert(row(1, "ignored"));
                writer.flush();
            }
            assertThat(CLUSTER.waitAndGetLeaderReplica(bucket).getLogTablet().localLogEndOffset())
                    .isEqualTo(end);
            writer.upsert(row(2, "second"));
            writer.flush();
            int oldLeader = CLUSTER.waitAndGetLeader(bucket);
            CLUSTER.stopTabletServer(oldLeader);
            try {
                retry(
                        Duration.ofMinutes(1),
                        () -> assertThat(CLUSTER.waitAndGetLeader(bucket)).isNotEqualTo(oldLeader));
                writer.upsert(row(1, "ignored after failover"));
                writer.flush();
                writer.upsert(row(3, "third"));
                writer.flush();
                assertThat(
                                CLUSTER.waitAndGetLeaderReplica(bucket)
                                        .getLogTablet()
                                        .localLogEndOffset())
                        .isEqualTo(end + 2);
                assertThat(
                                table.newLookup()
                                        .createLookuper()
                                        .lookup(row(1))
                                        .get()
                                        .getSingletonRow()
                                        .getString(1)
                                        .toString())
                        .isEqualTo("first");
            } finally {
                CLUSTER.startTabletServer(oldLeader);
            }
        }
    }

    @Test
    void testLostNoopResponseAndLegacyRequest() throws Exception {
        long tableId = createTable(TablePath.of("empty_wal", "lost_response"), LogFormat.ARROW);
        TableBucket bucket = new TableBucket(tableId, 0);
        write(bucket, 0, -1, 1, "first", true);
        long end = CLUSTER.waitAndGetLeaderReplica(bucket).getLogTablet().localLogEndOffset();
        write(bucket, 1, 0, 1, "ignored", true);
        assertThat(CLUSTER.waitAndGetLeaderReplica(bucket).getLogTablet().localLogEndOffset())
                .isEqualTo(end);
        int oldLeader = CLUSTER.waitAndGetLeader(bucket);
        CLUSTER.stopTabletServer(oldLeader);
        try {
            retry(
                    Duration.ofMinutes(1),
                    () -> assertThat(CLUSTER.waitAndGetLeader(bucket)).isNotEqualTo(oldLeader));
            // The producer retained prefix 0 because the success for request 1 was lost.
            write(bucket, 1, 0, 1, "ignored", true);
            write(bucket, 2, 1, 2, "second", true);
            assertThat(CLUSTER.waitAndGetLeaderReplica(bucket).getLogTablet().localLogEndOffset())
                    .isEqualTo(end + 1);
            // A client without the prefix field retains the existing contiguous WAL protocol.
            write(bucket, 0, -1, 3, "legacy", false);
            long legacyEnd =
                    CLUSTER.waitAndGetLeaderReplica(bucket).getLogTablet().localLogEndOffset();
            write(bucket, 1, -1, 3, "ignored legacy", false);
            assertThat(CLUSTER.waitAndGetLeaderReplica(bucket).getLogTablet().localLogEndOffset())
                    .isEqualTo(legacyEnd + 1);
        } finally {
            CLUSTER.startTabletServer(oldLeader);
        }
    }

    @Test
    void testLegacyCoordinatorBlocksProtocolActivation() throws Exception {
        String legacyId = "legacy-protocol-test";
        ZooKeeperClient zk = CLUSTER.getZooKeeperClient();
        zk.registerCoordinatorServer(
                new CoordinatorAddress(
                        legacyId, Endpoint.fromListenersString("CLIENT://localhost:1")));
        try {
            retry(
                    Duration.ofMinutes(1),
                    () ->
                            CLUSTER.getTabletServers()
                                    .forEach(
                                            server ->
                                                    assertThat(
                                                                    server.getMetadataCache()
                                                                            .getSnapshot()
                                                                            .supportsFirstRowProgress())
                                                            .isFalse()));
            // Start fresh TabletServer processes while a legacy Coordinator is registered.
            for (int serverId = 0; serverId < 3; serverId++) {
                CLUSTER.stopTabletServer(serverId);
                CLUSTER.startTabletServer(serverId);
            }
            retry(
                    Duration.ofMinutes(1),
                    () ->
                            CLUSTER.getTabletServers()
                                    .forEach(
                                            server ->
                                                    assertThat(
                                                                    server.getMetadataCache()
                                                                            .supportsFirstRowProgress())
                                                            .isFalse()));
            TablePath path = TablePath.of("empty_wal", "rolling_upgrade");
            long id = createTable(path, LogFormat.ARROW);
            TableBucket bucket = new TableBucket(id, 0);
            try (Connection connection =
                            ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                    Table table = connection.getTable(path)) {
                UpsertWriter writer = table.newUpsert().createWriter();
                writer.upsert(row(1, "first"));
                writer.flush();
                long end =
                        CLUSTER.waitAndGetLeaderReplica(bucket).getLogTablet().localLogEndOffset();
                writer.upsert(row(1, "legacy duplicate"));
                writer.flush();
                assertThat(
                                CLUSTER.waitAndGetLeaderReplica(bucket)
                                        .getLogTablet()
                                        .localLogEndOffset())
                        .isEqualTo(end + 1);
                zk.getCuratorClient().delete().forPath(ZkData.CoordinatorIdZNode.path(legacyId));
                retry(
                        Duration.ofMinutes(1),
                        () ->
                                CLUSTER.getTabletServers()
                                        .forEach(
                                                server ->
                                                        assertThat(
                                                                        server.getMetadataCache()
                                                                                .getSnapshot()
                                                                                .supportsFirstRowProgress())
                                                                .isTrue()));
                writer.upsert(row(1, "modern duplicate"));
                writer.flush();
                assertThat(
                                CLUSTER.waitAndGetLeaderReplica(bucket)
                                        .getLogTablet()
                                        .localLogEndOffset())
                        .isEqualTo(end + 1);
                writer.upsert(row(2, "second"));
                writer.flush();
                assertThat(
                                CLUSTER.waitAndGetLeaderReplica(bucket)
                                        .getLogTablet()
                                        .localLogEndOffset())
                        .isEqualTo(end + 2);
            }
        } finally {
            String path = ZkData.CoordinatorIdZNode.path(legacyId);
            if (zk.getCuratorClient().checkExists().forPath(path) != null) {
                zk.getCuratorClient().delete().forPath(path);
            }
        }
    }

    @Test
    void testCoordinatorFailoverPreservesProtocolSupport() throws Exception {
        Configuration conf = clusterConfig();
        conf.set(
                ConfigOptions.ZOOKEEPER_ADDRESS,
                CLUSTER.getZooKeeperClient()
                        .getCuratorClient()
                        .getZookeeperClient()
                        .getCurrentConnectionString());
        conf.set(ConfigOptions.BIND_LISTENERS, "CLIENT://localhost:0,FLUSS://localhost:0");
        conf.set(ConfigOptions.REMOTE_DATA_DIR, CLUSTER.getRemoteDataDir());
        CoordinatorServer standby = new CoordinatorServer(conf);
        standby.start();
        try {
            long id =
                    createTable(TablePath.of("empty_wal", "coordinator_failover"), LogFormat.ARROW);
            TableBucket bucket = new TableBucket(id, 0);
            try (Connection connection =
                            ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                    Table table =
                            connection.getTable(
                                    TablePath.of("empty_wal", "coordinator_failover"))) {
                UpsertWriter writer = table.newUpsert().createWriter();
                writer.upsert(row(1, "first"));
                writer.flush();
                writer.upsert(row(1, "duplicate"));
                writer.flush();
                long end =
                        CLUSTER.waitAndGetLeaderReplica(bucket).getLogTablet().localLogEndOffset();
                CLUSTER.stopCoordinatorServer();
                retry(
                        Duration.ofMinutes(1),
                        () ->
                                assertThat(
                                                CLUSTER.getZooKeeperClient()
                                                        .getCoordinatorLeaderAddress())
                                        .hasValueSatisfying(
                                                address ->
                                                        assertThat(address.getId())
                                                                .isEqualTo(standby.getServerId())));
                retry(
                        Duration.ofMinutes(1),
                        () ->
                                CLUSTER.getTabletServers()
                                        .forEach(
                                                server ->
                                                        assertThat(
                                                                        server.getMetadataCache()
                                                                                .getSnapshot()
                                                                                .supportsFirstRowProgress())
                                                                .isTrue()));
                writer.upsert(row(1, "duplicate after coordinator failover"));
                writer.flush();
                writer.upsert(row(2, "second"));
                writer.flush();
                assertThat(
                                CLUSTER.waitAndGetLeaderReplica(bucket)
                                        .getLogTablet()
                                        .localLogEndOffset())
                        .isEqualTo(end + 1);
            }
        } finally {
            standby.close();
            if (CLUSTER.getCoordinatorServer() == null) {
                CLUSTER.startCoordinatorServer();
            }
        }
    }

    private long createTable(TablePath path, LogFormat format) throws Exception {
        return RpcMessageTestUtils.createTable(
                CLUSTER,
                path,
                TableDescriptor.builder()
                        .schema(DATA1_SCHEMA_PK)
                        .distributedBy(1, "a")
                        .logFormat(format)
                        .property(ConfigOptions.TABLE_MERGE_ENGINE, MergeEngineType.FIRST_ROW)
                        .build());
    }

    private void write(
            TableBucket bucket,
            int sequence,
            int acknowledged,
            int key,
            String value,
            boolean logicalProgress)
            throws Exception {
        KvRecordBatch records =
                KvRecordTestUtils.KvRecordBatchFactory.of(DEFAULT_SCHEMA_ID)
                        .ofRecords(
                                Collections.singletonList(
                                        KvRecordTestUtils.KvRecordFactory.of(
                                                        DATA1_SCHEMA_PK.getRowType())
                                                .ofRecord(
                                                        new byte[] {(byte) key},
                                                        new Object[] {key, value})),
                                logicalProgress ? 100L : 200L,
                                sequence);
        PutKvRequest request =
                RpcMessageTestUtils.newPutKvRequest(bucket.getTableId(), 0, -1, records);
        if (logicalProgress) {
            request.getBucketsReqAt(0).setLastAckedSequence(acknowledged);
        }
        PutKvResponse response =
                CLUSTER.newTabletServerClientForNode(CLUSTER.waitAndGetLeader(bucket))
                        .putKv(request)
                        .get();
        assertThat(response.getBucketsRespAt(0).hasErrorCode()).as(response.toString()).isFalse();
    }
}
