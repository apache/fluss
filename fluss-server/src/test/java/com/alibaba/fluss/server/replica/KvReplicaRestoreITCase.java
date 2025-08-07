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

package com.alibaba.fluss.server.replica;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.config.MemorySize;
import com.alibaba.fluss.exception.NotLeaderOrFollowerException;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.KvRecord;
import com.alibaba.fluss.record.KvRecordBatch;
import com.alibaba.fluss.rpc.gateway.TabletServerGateway;
import com.alibaba.fluss.rpc.messages.PbLookupRespForBucket;
import com.alibaba.fluss.rpc.messages.PutKvRequest;
import com.alibaba.fluss.server.kv.snapshot.ZooKeeperCompletedSnapshotHandleStore;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import com.alibaba.fluss.utils.ExceptionUtils;
import com.alibaba.fluss.utils.FileUtils;
import com.alibaba.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static com.alibaba.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR_PK;
import static com.alibaba.fluss.server.testutils.KvTestUtils.assertLookupResponse;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newLookupRequest;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
import static com.alibaba.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static com.alibaba.fluss.testutils.DataTestUtils.genKvRecords;
import static com.alibaba.fluss.testutils.DataTestUtils.getKeyValuePairs;
import static com.alibaba.fluss.testutils.DataTestUtils.toKvRecordBatch;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.waitUntil;

/** The IT case for the restoring of kv replica. */
class KvReplicaRestoreITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(initConfig())
                    .build();

    private ZooKeeperCompletedSnapshotHandleStore completedSnapshotHandleStore;

    @BeforeEach
    void beforeEach() {
        completedSnapshotHandleStore =
                new ZooKeeperCompletedSnapshotHandleStore(
                        FLUSS_CLUSTER_EXTENSION.getZooKeeperClient());
    }

    @Test
    void testRestore() throws Exception {
        // create multiple tables for testing
        int tableNum = 2;
        int bucketNum = 3;
        List<TableBucket> tableBuckets = new ArrayList<>();
        for (int i = 0; i < tableNum; i++) {
            TablePath tablePath = TablePath.of("test_db", "test_table_" + i);
            long tableId =
                    createTable(FLUSS_CLUSTER_EXTENSION, tablePath, DATA1_TABLE_DESCRIPTOR_PK);
            for (int bucket = 0; bucket < bucketNum; bucket++) {
                tableBuckets.add(new TableBucket(tableId, bucket));
            }
        }

        for (TableBucket tableBucket : tableBuckets) {
            // wait replica become leader
            FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(tableBucket);
            int leaderServer = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tableBucket);

            KvRecordBatch kvRecordBatch =
                    genKvRecordBatch(new Object[] {1, "k1"}, new Object[] {2, "k2"});

            putRecordBatch(tableBucket, leaderServer, kvRecordBatch);
        }

        // wait for snapshot finish so that we can restore from snapshot
        for (TableBucket tableBucket : tableBuckets) {
            final long snapshot1Id = 0;
            waitUntil(
                    () -> completedSnapshotHandleStore.get(tableBucket, snapshot1Id).isPresent(),
                    Duration.ofMinutes(2),
                    "Fail to wait for the snapshot 0 for bucket " + tableBucket);
        }

        // pick one bucket put kv to make it fail
        TableBucket tableBucket = tableBuckets.get(new Random().nextInt(tableBuckets.size()));
        final int leaderServer = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tableBucket);
        Replica replica = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(tableBucket);

        int recordsNum = 10000;
        // create pretty many records to make can flush to file
        List<KvRecord> records = new ArrayList<>(recordsNum);
        for (int i = 0; i < recordsNum; i++) {
            records.addAll(genKvRecords(new Object[] {i, "k" + i}));
        }
        // we delete the kv dir to make flush fail
        FileUtils.deleteDirectory(replica.getKvTablet().getKvTabletDir());

        // should fail
        putRecordBatch(tableBucket, leaderServer, toKvRecordBatch(records));

        // wait for the replica to restore in another server
        AtomicInteger newLeaderServer = new AtomicInteger(-1);
        waitUntil(
                () -> {
                    int restoreServer = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tableBucket);
                    if (restoreServer != leaderServer) {
                        newLeaderServer.set(restoreServer);
                        return true;
                    }
                    return false;
                },
                Duration.ofMinutes(2),
                "Fail to wait for the replica to restore in another server. "
                        + "Previous leader is "
                        + leaderServer
                        + ", restored leader is "
                        + newLeaderServer.get());
        // wait the new replica become leader
        TabletServerGateway leaderGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(newLeaderServer.get());

        // although the records have been put into kv, but has been written to log,
        // once restore in another server, it should also restore the records to kv
        List<Tuple2<byte[], byte[]>> expectedKeyValues = getKeyValuePairs(records);

        // wait until we can lookup the last record from the kv
        waitUntil(
                () -> {
                    try {
                        PbLookupRespForBucket pbLookupRespForBucket =
                                leaderGateway
                                        .lookup(
                                                newLookupRequest(
                                                        tableBucket.getTableId(),
                                                        tableBucket.getBucket(),
                                                        expectedKeyValues.get(recordsNum - 1).f0))
                                        .get()
                                        .getBucketsRespAt(0);

                        return pbLookupRespForBucket.getValuesCount() > 0
                                && pbLookupRespForBucket.getValueAt(0).hasValues();
                    } catch (Exception e) {
                        if (ExceptionUtils.stripExecutionException(e)
                                instanceof NotLeaderOrFollowerException) {
                            // the new leader is not ready yet.
                            return false;
                        }
                        throw e;
                    }
                },
                Duration.ofMinutes(2),
                "Fail to wait for the last record to be flushed to kv.");

        // then, we can lookup the all records put, check all
        for (Tuple2<byte[], byte[]> keyValue : expectedKeyValues) {
            assertLookupResponse(
                    leaderGateway
                            .lookup(
                                    newLookupRequest(
                                            tableBucket.getTableId(),
                                            tableBucket.getBucket(),
                                            keyValue.f0))
                            .get(),
                    keyValue.f1);
        }
    }

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        // set a shorter interval for test
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofSeconds(1));
        // set a shorter write buffer size to flush can write ssts
        conf.set(ConfigOptions.KV_WRITE_BUFFER_SIZE, MemorySize.parse("1b"));
        // set a shorter max lag time
        conf.set(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME, Duration.ofSeconds(5));

        return conf;
    }

    private void putRecordBatch(
            TableBucket tableBucket, int leaderServer, KvRecordBatch kvRecordBatch) {
        PutKvRequest putKvRequest =
                newPutKvRequest(
                        tableBucket.getTableId(), tableBucket.getBucket(), -1, kvRecordBatch);
        TabletServerGateway leaderGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leaderServer);
        leaderGateway.putKv(putKvRequest);
    }
}
