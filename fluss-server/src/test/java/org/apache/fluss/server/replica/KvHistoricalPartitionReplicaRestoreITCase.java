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

import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.DefaultKvRecordBatch;
import org.apache.fluss.record.KvRecord;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.KvRecordTestUtils;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.LookupRequest;
import org.apache.fluss.rpc.messages.PbLookupReqForBucket;
import org.apache.fluss.rpc.messages.PbLookupRespForBucket;
import org.apache.fluss.rpc.messages.PbPutKvReqForBucket;
import org.apache.fluss.rpc.messages.PbValue;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.server.zk.data.lake.LakeTable;
import org.apache.fluss.server.zk.data.lake.LakeTableSnapshot;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.apache.fluss.utils.PartitionUtils.HISTORICAL_PARTITION_VALUE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@code __historical__} partition recovery. Verifies composite key WAL
 * replay and tiered offset recovery for historical partitions.
 *
 * <p>This test class uses a separate cluster with {@code DATALAKE_FORMAT=PAIMON} enabled, so that
 * data-lake-enabled tables can be created without affecting non-lake tables in other test classes.
 */
class KvHistoricalPartitionReplicaRestoreITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(initConfig())
                    .build();

    /** Row type for the historical partition PK table: (a INT, b STRING, c STRING). */
    private static final RowType HISTORICAL_ROW_TYPE =
            RowType.of(
                    new org.apache.fluss.types.DataType[] {
                        DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING()
                    },
                    new String[] {"a", "b", "c"});

    /**
     * Key encoder for the historical PK table using PaimonKeyEncoder (because the table has
     * DATALAKE_FORMAT=PAIMON). Encodes just the physical PK column "a" from a full row. Must match
     * the encoder used by KvRecoverHelper during WAL replay.
     */
    private static final KeyEncoder HISTORICAL_KV_KEY_ENCODER =
            KeyEncoder.ofBucketKeyEncoder(
                    HISTORICAL_ROW_TYPE, Collections.singletonList("a"), DataLakeFormat.PAIMON);

    /**
     * Key-only row type for encoding lookup keys. Uses the same PaimonKeyEncoder format but with a
     * key-only row type so we can pass single-field rows for lookups.
     */
    private static final RowType HISTORICAL_KEY_ONLY_TYPE =
            RowType.of(new org.apache.fluss.types.DataType[] {DataTypes.INT()}, new String[] {"a"});

    private static final KeyEncoder HISTORICAL_LOOKUP_KEY_ENCODER =
            KeyEncoder.ofBucketKeyEncoder(
                    HISTORICAL_KEY_ONLY_TYPE,
                    Collections.singletonList("a"),
                    DataLakeFormat.PAIMON);

    /**
     * Verifies __historical__ partition recovery in two phases:
     *
     * <ol>
     *   <li>Full WAL replay: write data, failover, verify all records recovered via composite key
     *       encoding.
     *   <li>Tiered offset replay: simulate tiering, write more data, failover again, verify only
     *       post-tiered records are recovered (pre-tiered records skipped).
     * </ol>
     */
    @Test
    void testHistoricalPartitionRecovery() throws Exception {
        TablePath tablePath = TablePath.of("test_db", "test_hist_recovery");
        long tableId =
                createTable(
                        FLUSS_CLUSTER_EXTENSION, tablePath, createHistoricalPkTableDescriptor());

        // Create __historical__ partition directly (no need to wait for auto-partitions
        // since the default auto-partition check interval is 10 min)
        long historicalPartitionId = createHistoricalPartitionAndGetId(tablePath);
        TableBucket historicalBucket = new TableBucket(tableId, historicalPartitionId, 0);
        FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(historicalBucket);
        int leaderServer = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(historicalBucket);

        // --- Phase 1: full WAL replay recovery ---
        // Write records for two different expired partitions ("2000" and "2001") into the same
        // __historical__ bucket. This verifies composite key encoding preserves partition name
        // distinction — same PK values (id=0..4) in different partitions are separate entries.

        // Write 5 records for partition "2000" (id=0..4)
        KvRecordBatch batch2000 = genHistoricalKvRecordBatch(0, 5, "2000");
        putKvToPartition(historicalBucket, leaderServer, batch2000, "2000").join();

        // Write 5 records for partition "2001" (id=0..4)
        KvRecordBatch batch2001 = genHistoricalKvRecordBatch(0, 5, "2001");
        putKvToPartition(historicalBucket, leaderServer, batch2001, "2001").join();

        // Verify all records visible before failover
        TabletServerGateway leaderGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leaderServer);
        for (int i = 0; i < 5; i++) {
            byte[] key = HISTORICAL_LOOKUP_KEY_ENCODER.encodeKey(row(i));
            assertHistoricalLookup(
                    leaderGateway,
                    tableId,
                    historicalPartitionId,
                    0,
                    key,
                    "2000",
                    expectedValue(i, "val_" + i, "2000"));
            assertHistoricalLookup(
                    leaderGateway,
                    tableId,
                    historicalPartitionId,
                    0,
                    key,
                    "2001",
                    expectedValue(i, "val_" + i, "2001"));
        }

        // Failover: stop leader, wait for new leader with KV recovery complete
        FLUSS_CLUSTER_EXTENSION.stopTabletServer(leaderServer);
        FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(historicalBucket);
        int newLeaderServer = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(historicalBucket);
        TabletServerGateway newGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(newLeaderServer);

        // Verify all records recovered with correct composite key encoding for both partitions
        for (int i = 0; i < 5; i++) {
            byte[] key = HISTORICAL_LOOKUP_KEY_ENCODER.encodeKey(row(i));
            assertHistoricalLookup(
                    newGateway,
                    tableId,
                    historicalPartitionId,
                    0,
                    key,
                    "2000",
                    expectedValue(i, "val_" + i, "2000"));
            assertHistoricalLookup(
                    newGateway,
                    tableId,
                    historicalPartitionId,
                    0,
                    key,
                    "2001",
                    expectedValue(i, "val_" + i, "2001"));
        }

        // Restart stopped server to restore cluster health
        FLUSS_CLUSTER_EXTENSION.startTabletServer(leaderServer);
        FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(3);

        // --- Phase 2: tiered offset recovery ---

        // Record current log end offset as simulated tiered offset.
        // Only set lakeLogEndOffset on the leader (simulating real tiering — only the leader
        // performs tiering). Do NOT set it on followers. Instead, register a LakeTableSnapshot
        // to ZK so that when a follower becomes the new leader, updateWithLakeTableSnapshot
        // (which now runs before makeLeader) syncs the offset from ZK before initKvTablet.
        leaderServer = newLeaderServer;
        Replica leaderReplica = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(historicalBucket);
        LogTablet leaderLogTablet = leaderReplica.getLogTablet();
        long tieredOffset = leaderLogTablet.localLogEndOffset();
        leaderLogTablet.updateLakeLogEndOffset(tieredOffset);

        // Register LakeTableSnapshot to ZK (simulates what real tiering does)
        ZooKeeperClient zkClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
        LakeTableSnapshot lakeTableSnapshot =
                new LakeTableSnapshot(1L, Collections.singletonMap(historicalBucket, tieredOffset));
        zkClient.upsertLakeTable(tableId, new LakeTable(lakeTableSnapshot), /* isUpdate= */ false);

        // Write 5 more records for each partition (id=5..9)
        leaderGateway = FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leaderServer);
        KvRecordBatch phase2Batch2000 = genHistoricalKvRecordBatch(5, 5, "2000");
        putKvToPartition(historicalBucket, leaderServer, phase2Batch2000, "2000").join();
        KvRecordBatch phase2Batch2001 = genHistoricalKvRecordBatch(5, 5, "2001");
        putKvToPartition(historicalBucket, leaderServer, phase2Batch2001, "2001").join();

        // Verify phase 2 data visible on current leader
        for (int i = 5; i < 10; i++) {
            byte[] key = HISTORICAL_LOOKUP_KEY_ENCODER.encodeKey(row(i));
            assertHistoricalLookup(
                    leaderGateway,
                    tableId,
                    historicalPartitionId,
                    0,
                    key,
                    "2000",
                    expectedValue(i, "val_" + i, "2000"));
            assertHistoricalLookup(
                    leaderGateway,
                    tableId,
                    historicalPartitionId,
                    0,
                    key,
                    "2001",
                    expectedValue(i, "val_" + i, "2001"));
        }

        // Failover again: new leader re-initializes KV from tieredOffset
        FLUSS_CLUSTER_EXTENSION.stopTabletServer(leaderServer);
        FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(historicalBucket);
        newLeaderServer = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(historicalBucket);
        newGateway = FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(newLeaderServer);

        // Phase 2 records (id=5..9) should be found for both partitions
        for (int i = 5; i < 10; i++) {
            byte[] key = HISTORICAL_LOOKUP_KEY_ENCODER.encodeKey(row(i));
            assertHistoricalLookup(
                    newGateway,
                    tableId,
                    historicalPartitionId,
                    0,
                    key,
                    "2000",
                    expectedValue(i, "val_" + i, "2000"));
            assertHistoricalLookup(
                    newGateway,
                    tableId,
                    historicalPartitionId,
                    0,
                    key,
                    "2001",
                    expectedValue(i, "val_" + i, "2001"));
        }

        // Phase 1 records (id=0..4) should NOT be found for either partition
        for (int i = 0; i < 5; i++) {
            byte[] key = HISTORICAL_LOOKUP_KEY_ENCODER.encodeKey(row(i));
            assertHistoricalLookup(
                    newGateway, tableId, historicalPartitionId, 0, key, "2000", null);
            assertHistoricalLookup(
                    newGateway, tableId, historicalPartitionId, 0, key, "2001", null);
        }

        // Restart to restore cluster health
        FLUSS_CLUSTER_EXTENSION.startTabletServer(leaderServer);
        FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(3);
    }

    // ---- Helper methods ----

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        // set a shorter interval for test
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofSeconds(1));
        // set a shorter write buffer size to flush can write ssts
        conf.set(ConfigOptions.KV_WRITE_BUFFER_SIZE, MemorySize.parse("1b"));
        // set a shorter max lag time
        conf.set(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME, Duration.ofSeconds(5));
        // enable datalake for historical partition tests
        conf.set(ConfigOptions.DATALAKE_FORMAT, DataLakeFormat.PAIMON);
        return conf;
    }

    /**
     * Creates a data-lake-enabled, auto-partitioned PK table descriptor with schema (a INT, b
     * STRING, c STRING), PK(a, c), partitioned by c, 1 bucket.
     */
    private static TableDescriptor createHistoricalPkTableDescriptor() {
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column("c", DataTypes.STRING())
                        .primaryKey("a", "c")
                        .build();
        return TableDescriptor.builder()
                .schema(schema)
                .distributedBy(1, "a")
                .partitionedBy("c")
                .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                .property(ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT, AutoPartitionTimeUnit.YEAR)
                .property(ConfigOptions.TABLE_AUTO_PARTITION_NUM_RETENTION, 7)
                .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                .build();
    }

    /**
     * Creates the __historical__ partition via coordinator RPC and returns its partition ID. Waits
     * until the partition is registered in ZooKeeper.
     */
    private long createHistoricalPartitionAndGetId(TablePath tablePath) throws Exception {
        org.apache.fluss.rpc.gateway.CoordinatorGateway coordinatorGateway =
                FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        org.apache.fluss.rpc.messages.CreatePartitionRequest createPartitionRequest =
                new org.apache.fluss.rpc.messages.CreatePartitionRequest()
                        .setIgnoreIfNotExists(true);
        createPartitionRequest
                .setTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());
        org.apache.fluss.rpc.messages.PbKeyValue pbKv =
                new org.apache.fluss.rpc.messages.PbKeyValue()
                        .setKey("c")
                        .setValue(HISTORICAL_PARTITION_VALUE);
        createPartitionRequest
                .setPartitionSpec()
                .addAllPartitionKeyValues(Collections.singletonList(pbKv));
        coordinatorGateway.createPartition(createPartitionRequest).get();

        // Wait for the partition to be registered in ZK and return its ID
        ZooKeeperClient zkClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
        long[] partitionId = new long[1];
        waitUntil(
                () -> {
                    Map<String, PartitionRegistration> partitions =
                            zkClient.getPartitionRegistrations(tablePath);
                    PartitionRegistration reg = partitions.get(HISTORICAL_PARTITION_VALUE);
                    if (reg != null) {
                        partitionId[0] = reg.getPartitionId();
                        return true;
                    }
                    return false;
                },
                Duration.ofMinutes(1),
                "Fail to wait for __historical__ partition to be registered.");
        return partitionId[0];
    }

    /**
     * Generates a KvRecordBatch for the historical partition table schema. Each record has
     * (a=startId+i, b="val_"+startId+i, c=partitionValue).
     */
    private KvRecordBatch genHistoricalKvRecordBatch(int startId, int count, String partitionValue)
            throws Exception {
        // Use KvRecordFactory with manually-encoded PaimonKeyEncoder keys to match the
        // server's key encoding during both putAsLeader and recovery (KvRecoverHelper).
        KvRecordTestUtils.KvRecordFactory kvRecordFactory =
                KvRecordTestUtils.KvRecordFactory.of(HISTORICAL_ROW_TYPE);
        KvRecordTestUtils.KvRecordBatchFactory kvBatchFactory =
                KvRecordTestUtils.KvRecordBatchFactory.of(DEFAULT_SCHEMA_ID);
        List<KvRecord> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            int id = startId + i;
            Object[] values = new Object[] {id, "val_" + id, partitionValue};
            byte[] keyBytes = HISTORICAL_KV_KEY_ENCODER.encodeKey(row(values));
            records.add(kvRecordFactory.ofRecord(keyBytes, values));
        }
        return kvBatchFactory.ofRecords(records);
    }

    /**
     * Puts a KvRecordBatch to a partitioned bucket with partition_id and partition_name set in the
     * request. This is needed for historical partition writes where the server uses the
     * partition_name for composite key encoding.
     */
    private CompletableFuture<?> putKvToPartition(
            TableBucket tableBucket,
            int leaderServer,
            KvRecordBatch kvRecordBatch,
            String partitionName) {
        PutKvRequest putKvRequest = new PutKvRequest();
        putKvRequest.setTableId(tableBucket.getTableId()).setAcks(-1).setTimeoutMs(30000);
        PbPutKvReqForBucket reqForBucket = new PbPutKvReqForBucket();
        reqForBucket.setBucketId(tableBucket.getBucket());
        reqForBucket.setPartitionId(tableBucket.getPartitionId());
        reqForBucket.setPartitionName(partitionName);
        if (kvRecordBatch instanceof DefaultKvRecordBatch) {
            DefaultKvRecordBatch batch = (DefaultKvRecordBatch) kvRecordBatch;
            reqForBucket.setRecords(
                    batch.getMemorySegment(), batch.getPosition(), batch.sizeInBytes());
        } else {
            throw new IllegalArgumentException(
                    "Unsupported KvRecordBatch type: " + kvRecordBatch.getClass().getName());
        }
        putKvRequest.addAllBucketsReqs(Collections.singletonList(reqForBucket));
        TabletServerGateway gateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leaderServer);
        return gateway.putKv(putKvRequest);
    }

    /**
     * Creates a lookup request for a partitioned bucket with partition_id and partition_name set.
     * The partition_name is needed for historical partitions so the server can construct the
     * composite key for lookup.
     */
    private LookupRequest newPartitionedLookupRequest(
            long tableId, long partitionId, int bucketId, byte[] key, String partitionName) {
        LookupRequest lookupRequest = new LookupRequest().setTableId(tableId);
        PbLookupReqForBucket reqForBucket = lookupRequest.addBucketsReq();
        reqForBucket.setBucketId(bucketId);
        reqForBucket.setPartitionId(partitionId);
        reqForBucket.setPartitionName(partitionName);
        reqForBucket.addKey(key);
        return lookupRequest;
    }

    /** Encodes expected row values into the KV store value format (schema_id + compacted row). */
    private static byte[] expectedValue(Object... values) {
        return ValueEncoder.encodeValue(
                DEFAULT_SCHEMA_ID, compactedRow(HISTORICAL_ROW_TYPE, values));
    }

    /**
     * Asserts a historical partition lookup result. If {@code expectedValue} is non-null, asserts
     * the lookup returns a matching value; if null, asserts the key is not found.
     */
    private void assertHistoricalLookup(
            TabletServerGateway gateway,
            long tableId,
            long partitionId,
            int bucketId,
            byte[] key,
            String partitionName,
            byte[] expectedValue)
            throws Exception {
        LookupRequest request =
                newPartitionedLookupRequest(tableId, partitionId, bucketId, key, partitionName);
        PbLookupRespForBucket resp = gateway.lookup(request).get().getBucketsRespAt(0);
        assertThat(resp.getValuesCount()).isEqualTo(1);
        PbValue pbValue = resp.getValueAt(0);
        if (expectedValue != null) {
            assertThat(pbValue.hasValues())
                    .as("Expected record to be found in historical partition lookup")
                    .isTrue();
            assertThat(pbValue.getValues()).isEqualTo(expectedValue);
        } else {
            assertThat(pbValue.hasValues())
                    .as("Expected record NOT to be found in historical partition lookup")
                    .isFalse();
        }
    }
}
