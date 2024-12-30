/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.replica;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.metadata.LogFormat;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.KvRecordBatch;
import com.alibaba.fluss.record.KvRecordTestUtils;
import com.alibaba.fluss.record.LogRecords;
import com.alibaba.fluss.record.MemoryLogRecords;
import com.alibaba.fluss.record.RowKind;
import com.alibaba.fluss.server.entity.NotifyLeaderAndIsrData;
import com.alibaba.fluss.server.kv.KvTablet;
import com.alibaba.fluss.server.kv.snapshot.CompletedSnapshot;
import com.alibaba.fluss.server.kv.snapshot.TestingCompletedKvSnapshotCommitter;
import com.alibaba.fluss.server.log.FetchParams;
import com.alibaba.fluss.server.log.LogAppendInfo;
import com.alibaba.fluss.server.log.LogReadInfo;
import com.alibaba.fluss.server.testutils.KvTestUtils;
import com.alibaba.fluss.server.zk.NOPErrorHandler;
import com.alibaba.fluss.server.zk.data.LeaderAndIsr;
import com.alibaba.fluss.testutils.DataTestUtils;
import com.alibaba.fluss.testutils.common.ManuallyTriggeredScheduledExecutorService;
import com.alibaba.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.alibaba.fluss.record.LogRecordBatch.NO_BATCH_SEQUENCE;
import static com.alibaba.fluss.record.LogRecordBatch.NO_WRITER_ID;
import static com.alibaba.fluss.record.TestData.DATA1;
import static com.alibaba.fluss.record.TestData.DATA1_PHYSICAL_TABLE_PATH;
import static com.alibaba.fluss.record.TestData.DATA1_PHYSICAL_TABLE_PATH_PK;
import static com.alibaba.fluss.record.TestData.DATA1_ROW_TYPE;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_ID;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_ID_PK;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static com.alibaba.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static com.alibaba.fluss.server.coordinator.CoordinatorContext.INITIAL_COORDINATOR_EPOCH;
import static com.alibaba.fluss.server.zk.data.LeaderAndIsr.INITIAL_LEADER_EPOCH;
import static com.alibaba.fluss.testutils.DataTestUtils.assertLogRecordsEquals;
import static com.alibaba.fluss.testutils.DataTestUtils.createBasicMemoryLogRecords;
import static com.alibaba.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static com.alibaba.fluss.testutils.DataTestUtils.genKvRecords;
import static com.alibaba.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static com.alibaba.fluss.testutils.DataTestUtils.getKeyValuePairs;
import static com.alibaba.fluss.testutils.LogRecordsAssert.assertThatLogRecords;
import static com.alibaba.fluss.utils.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link Replica}. */
final class ReplicaTest extends ReplicaTestBase {
    // TODO add more tests refer to kafka's PartitionTest.
    // TODO add more tests to cover partition table

    @Test
    void testMakeLeader() throws Exception {
        Replica logReplica =
                makeLogReplica(DATA1_PHYSICAL_TABLE_PATH, new TableBucket(DATA1_TABLE_ID, 1));
        // log table.
        assertThat(logReplica.isKvTable()).isFalse();
        assertThat(logReplica.getLogTablet()).isNotNull();
        assertThat(logReplica.getKvTablet()).isNull();
        makeLogReplicaAsLeader(logReplica);
        assertThat(logReplica.getLogTablet()).isNotNull();
        assertThat(logReplica.getKvTablet()).isNull();

        Replica kvReplica =
                makeKvReplica(DATA1_PHYSICAL_TABLE_PATH_PK, new TableBucket(DATA1_TABLE_ID_PK, 1));
        // Kv table.
        assertThat(kvReplica.isKvTable()).isTrue();
        assertThat(kvReplica.getLogTablet()).isNotNull();
        makeKvReplicaAsLeader(kvReplica);
        assertThat(kvReplica.getLogTablet()).isNotNull();
        // TODO get kv is true.
        assertThat(kvReplica.getKvTablet()).isNotNull();
    }

    @Test
    void testAppendRecordsToLeader() throws Exception {
        Replica logReplica =
                makeLogReplica(DATA1_PHYSICAL_TABLE_PATH, new TableBucket(DATA1_TABLE_ID, 1));
        makeLogReplicaAsLeader(logReplica);

        MemoryLogRecords mr = genMemoryLogRecordsByObject(DATA1);
        LogAppendInfo appendInfo = logReplica.appendRecordsToLeader(mr, 0);
        assertThat(appendInfo.shallowCount()).isEqualTo(1);

        FetchParams fetchParams =
                new FetchParams(
                        -1,
                        (int)
                                conf.get(ConfigOptions.CLIENT_SCANNER_LOG_FETCH_MAX_BYTES)
                                        .getBytes());
        fetchParams.setCurrentFetch(DATA1_TABLE_ID, 0, Integer.MAX_VALUE, DATA1_ROW_TYPE, null);
        LogReadInfo logReadInfo = logReplica.fetchRecords(fetchParams);
        assertLogRecordsEquals(DATA1_ROW_TYPE, logReadInfo.getFetchedData().getRecords(), DATA1);
    }

    @Test
    void testPartialPutRecordsToLeader() throws Exception {
        Replica kvReplica =
                makeKvReplica(DATA1_PHYSICAL_TABLE_PATH_PK, new TableBucket(DATA1_TABLE_ID_PK, 1));
        makeKvReplicaAsLeader(kvReplica);

        // two records in a batch with same key, should also generate +I/-U/+U
        KvRecordTestUtils.KvRecordFactory kvRecordFactory =
                KvRecordTestUtils.KvRecordFactory.of(DATA1_ROW_TYPE);
        KvRecordTestUtils.KvRecordBatchFactory kvRecordBatchFactory =
                KvRecordTestUtils.KvRecordBatchFactory.of(DEFAULT_SCHEMA_ID);
        KvRecordBatch kvRecords =
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord("k1", new Object[] {1, null}),
                        kvRecordFactory.ofRecord("k1", new Object[] {2, null}),
                        kvRecordFactory.ofRecord("k2", new Object[] {3, null}));

        int[] targetColumns = new int[] {0};
        // put records
        putRecordsToLeader(kvReplica, kvRecords, targetColumns);

        targetColumns = new int[] {0, 1};
        kvRecords =
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord("k1", new Object[] {2, "aa"}),
                        kvRecordFactory.ofRecord("k2", new Object[] {3, "bb2"}),
                        kvRecordFactory.ofRecord("k2", new Object[] {3, "bb4"}));
        LogAppendInfo logAppendInfo = putRecordsToLeader(kvReplica, kvRecords, targetColumns);

        assertThat(logAppendInfo.lastOffset()).isEqualTo(9);

        MemoryLogRecords expected =
                logRecords(
                        4,
                        Arrays.asList(
                                RowKind.UPDATE_BEFORE,
                                RowKind.UPDATE_AFTER,
                                RowKind.UPDATE_BEFORE,
                                RowKind.UPDATE_AFTER,
                                RowKind.UPDATE_BEFORE,
                                RowKind.UPDATE_AFTER),
                        Arrays.asList(
                                // for k1
                                new Object[] {2, null},
                                new Object[] {2, "aa"},
                                // for k2
                                new Object[] {3, null},
                                new Object[] {3, "bb2"},
                                // for k2
                                new Object[] {3, "bb2"},
                                new Object[] {3, "bb4"}));

        assertThatLogRecords(fetchRecords(kvReplica, 4))
                .withSchema(DATA1_ROW_TYPE)
                .isEqualTo(expected);
    }

    @Test
    void testPutRecordsToLeader() throws Exception {
        Replica kvReplica =
                makeKvReplica(DATA1_PHYSICAL_TABLE_PATH_PK, new TableBucket(DATA1_TABLE_ID_PK, 1));
        makeKvReplicaAsLeader(kvReplica);

        // two records in a batch with same key, should also generate +I/-U/+U
        KvRecordTestUtils.KvRecordFactory kvRecordFactory =
                KvRecordTestUtils.KvRecordFactory.of(DATA1_ROW_TYPE);
        KvRecordTestUtils.KvRecordBatchFactory kvRecordBatchFactory =
                KvRecordTestUtils.KvRecordBatchFactory.of(DEFAULT_SCHEMA_ID);
        KvRecordBatch kvRecords =
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord("k1", new Object[] {1, "a"}),
                        kvRecordFactory.ofRecord("k1", new Object[] {2, "b"}),
                        kvRecordFactory.ofRecord("k2", new Object[] {3, "b1"}));
        LogAppendInfo logAppendInfo = putRecordsToLeader(kvReplica, kvRecords);
        assertThat(logAppendInfo.lastOffset()).isEqualTo(3);
        MemoryLogRecords expected =
                logRecords(
                        0L,
                        Arrays.asList(
                                RowKind.INSERT,
                                RowKind.UPDATE_BEFORE,
                                RowKind.UPDATE_AFTER,
                                RowKind.INSERT),
                        Arrays.asList(
                                new Object[] {1, "a"},
                                new Object[] {1, "a"},
                                new Object[] {2, "b"},
                                new Object[] {3, "b1"}));
        assertThatLogRecords(fetchRecords(kvReplica))
                .withSchema(DATA1_ROW_TYPE)
                .isEqualTo(expected);
        int currentOffset = 4;

        // now, append another batch, it should also produce
        // delete & update_before & update_after message
        kvRecords =
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord("k1", null),
                        kvRecordFactory.ofRecord("k2", new Object[] {4, "b2"}),
                        kvRecordFactory.ofRecord("k2", new Object[] {5, "b4"}));
        logAppendInfo = putRecordsToLeader(kvReplica, kvRecords);
        assertThat(logAppendInfo.lastOffset()).isEqualTo(8);
        expected =
                logRecords(
                        currentOffset,
                        Arrays.asList(
                                RowKind.DELETE,
                                RowKind.UPDATE_BEFORE,
                                RowKind.UPDATE_AFTER,
                                RowKind.UPDATE_BEFORE,
                                RowKind.UPDATE_AFTER),
                        Arrays.asList(
                                // for k1
                                new Object[] {2, "b"},
                                // for k2
                                new Object[] {3, "b1"},
                                new Object[] {4, "b2"},
                                // for k2
                                new Object[] {4, "b2"},
                                new Object[] {5, "b4"}));
        assertThatLogRecords(fetchRecords(kvReplica, currentOffset))
                .withSchema(DATA1_ROW_TYPE)
                .isEqualTo(expected);
        currentOffset += 5;

        // put for k1, delete for k2, put for k3; it should produce
        // +I for k1 since k1 has been deleted, -D for k2; +I for k3
        kvRecords =
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord("k1", new Object[] {1, "a1"}),
                        kvRecordFactory.ofRecord("k2", null),
                        kvRecordFactory.ofRecord("k3", new Object[] {6, "b4"}));
        logAppendInfo = putRecordsToLeader(kvReplica, kvRecords);
        assertThat(logAppendInfo.lastOffset()).isEqualTo(11);
        expected =
                logRecords(
                        currentOffset,
                        Arrays.asList(RowKind.INSERT, RowKind.DELETE, RowKind.INSERT),
                        Arrays.asList(
                                // for k1
                                new Object[] {1, "a1"},
                                // for k2
                                new Object[] {5, "b4"},
                                // for k3
                                new Object[] {6, "b4"}));
        assertThatLogRecords(fetchRecords(kvReplica, currentOffset))
                .withSchema(DATA1_ROW_TYPE)
                .isEqualTo(expected);
        currentOffset += 3;

        // delete k2 again, shouldn't produce any log records
        kvRecords = kvRecordBatchFactory.ofRecords(kvRecordFactory.ofRecord("k2", null));
        logAppendInfo = putRecordsToLeader(kvReplica, kvRecords);
        assertThat(logAppendInfo.lastOffset()).isEqualTo(11);
        assertThatLogRecords(fetchRecords(kvReplica, currentOffset))
                .withSchema(DATA1_ROW_TYPE)
                .isEqualTo(MemoryLogRecords.EMPTY);

        // delete k1 and put k1 again, should produce -D, +I
        kvRecords =
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord("k1", null),
                        kvRecordFactory.ofRecord("k1", new Object[] {1, "aaa"}));
        logAppendInfo = putRecordsToLeader(kvReplica, kvRecords);
        assertThat(logAppendInfo.lastOffset()).isEqualTo(13);
        expected =
                logRecords(
                        currentOffset,
                        Arrays.asList(RowKind.DELETE, RowKind.INSERT),
                        Arrays.asList(new Object[] {1, "a1"}, new Object[] {1, "aaa"}));
        assertThatLogRecords(fetchRecords(kvReplica, currentOffset))
                .withSchema(DATA1_ROW_TYPE)
                .isEqualTo(expected);
    }

    @Test
    void testKvReplicaSnapshot(@TempDir File snapshotKvTabletDir) throws Exception {
        TableBucket tableBucket = new TableBucket(DATA1_TABLE_ID_PK, 1);

        // create test context
        TestSnapshotContext testKvSnapshotContext =
                new TestSnapshotContext(tableBucket, snapshotKvTabletDir.getPath());
        ManuallyTriggeredScheduledExecutorService scheduledExecutorService =
                testKvSnapshotContext.scheduledExecutorService;
        TestingCompletedKvSnapshotCommitter kvSnapshotStore =
                testKvSnapshotContext.testKvSnapshotStore;

        // make a kv replica
        Replica kvReplica =
                makeKvReplica(DATA1_PHYSICAL_TABLE_PATH_PK, tableBucket, testKvSnapshotContext);
        makeKvReplicaAsLeader(kvReplica);
        KvRecordBatch kvRecords =
                genKvRecordBatch(
                        Tuple2.of("k1", new Object[] {1, "a"}),
                        Tuple2.of("k1", new Object[] {2, "b"}),
                        Tuple2.of("k2", new Object[] {3, "b1"}));
        putRecordsToLeader(kvReplica, kvRecords);

        // trigger one snapshot,
        scheduledExecutorService.triggerNonPeriodicScheduledTask();

        // wait util the snapshot 0 success
        CompletedSnapshot completedSnapshot0 =
                kvSnapshotStore.waitUtilSnapshotComplete(tableBucket, 0);

        // check snapshot
        long expectedLogOffset = 4;
        List<Tuple2<byte[], byte[]>> expectedKeyValues =
                getKeyValuePairs(
                        genKvRecords(
                                Tuple2.of("k1", new Object[] {2, "b"}),
                                Tuple2.of("k2", new Object[] {3, "b1"})));
        KvTestUtils.checkSnapshot(completedSnapshot0, expectedKeyValues, expectedLogOffset);

        // put some data again
        kvRecords =
                genKvRecordBatch(Tuple2.of("k2", new Object[] {4, "bk2"}), Tuple2.of("k1", null));
        putRecordsToLeader(kvReplica, kvRecords);

        // trigger next checkpoint
        scheduledExecutorService.triggerNonPeriodicScheduledTask();
        // wait util the snapshot 1 success
        CompletedSnapshot completedSnapshot1 =
                kvSnapshotStore.waitUtilSnapshotComplete(tableBucket, 1);

        // check snapshot
        expectedLogOffset = 7;
        expectedKeyValues =
                getKeyValuePairs(genKvRecords(Tuple2.of("k2", new Object[] {4, "bk2"})));
        KvTestUtils.checkSnapshot(completedSnapshot1, expectedKeyValues, expectedLogOffset);

        // check the snapshot should be incremental, with only one newly file
        KvTestUtils.checkSnapshotIncrementWithNewlyFiles(
                completedSnapshot1.getKvSnapshotHandle(),
                completedSnapshot0.getKvSnapshotHandle(),
                1);
        // now, make the replica as follower to make kv can be destroyed
        makeKvReplicaAsFollower(kvReplica, 1);

        // make a new kv replica
        testKvSnapshotContext =
                new TestSnapshotContext(
                        tableBucket, snapshotKvTabletDir.getPath(), kvSnapshotStore);
        kvReplica = makeKvReplica(DATA1_PHYSICAL_TABLE_PATH_PK, tableBucket, testKvSnapshotContext);
        scheduledExecutorService = testKvSnapshotContext.scheduledExecutorService;
        kvSnapshotStore = testKvSnapshotContext.testKvSnapshotStore;
        makeKvReplicaAsFollower(kvReplica, 1);

        // check the kv tablet should be null since it has become follower
        assertThat(kvReplica.getKvTablet()).isNull();

        // make as leader again, should restore from snapshot
        makeKvReplicaAsLeader(kvReplica, 2);

        // put some data
        kvRecords =
                genKvRecordBatch(
                        Tuple2.of("k2", new Object[] {4, "bk21"}),
                        Tuple2.of("k3", new Object[] {5, "k3"}));
        putRecordsToLeader(kvReplica, kvRecords);

        // trigger another one snapshot,
        scheduledExecutorService.triggerNonPeriodicScheduledTask();
        //  wait util the snapshot 2 success
        CompletedSnapshot completedSnapshot2 =
                kvSnapshotStore.waitUtilSnapshotComplete(tableBucket, 2);
        expectedLogOffset = 10;
        expectedKeyValues =
                getKeyValuePairs(
                        genKvRecords(
                                Tuple2.of("k2", new Object[] {4, "bk21"}),
                                Tuple2.of("k3", new Object[] {5, "k3"})));
        KvTestUtils.checkSnapshot(completedSnapshot2, expectedKeyValues, expectedLogOffset);
    }

    @Test
    void testRestore(@TempDir Path snapshotKvTabletDirPath) throws Exception {
        TableBucket tableBucket = new TableBucket(DATA1_TABLE_ID_PK, 1);
        TestSnapshotContext testKvSnapshotContext =
                new TestSnapshotContext(tableBucket, snapshotKvTabletDirPath.toString());
        ManuallyTriggeredScheduledExecutorService scheduledExecutorService =
                testKvSnapshotContext.scheduledExecutorService;
        TestingCompletedKvSnapshotCommitter kvSnapshotStore =
                testKvSnapshotContext.testKvSnapshotStore;

        // make a kv replica
        Replica kvReplica =
                makeKvReplica(DATA1_PHYSICAL_TABLE_PATH_PK, tableBucket, testKvSnapshotContext);
        makeKvReplicaAsLeader(kvReplica);
        putRecordsToLeader(
                kvReplica,
                DataTestUtils.genKvRecordBatch(new Object[] {1, "a"}, new Object[] {2, "b"}));
        makeKvReplicaAsFollower(kvReplica, 1);

        // make a kv replica again, should restore from log
        makeKvReplicaAsLeader(kvReplica, 2);
        assertThat(kvReplica.getKvTablet()).isNotNull();
        KvTablet kvTablet = kvReplica.getKvTablet();

        // check result
        List<Tuple2<byte[], byte[]>> expectedKeyValues =
                getKeyValuePairs(genKvRecords(new Object[] {1, "a"}, new Object[] {2, "b"}));
        verifyGetKeyValues(kvTablet, expectedKeyValues);

        // We have to remove the first scheduled snapshot task since it's for the previous kv tablet
        // whose rocksdb has been dropped.
        scheduledExecutorService.removeNonPeriodicScheduledTask();

        // trigger one snapshot,
        scheduledExecutorService.triggerNonPeriodicScheduledTask();
        // wait util the snapshot success
        kvSnapshotStore.waitUtilSnapshotComplete(tableBucket, 0);

        // write data again
        putRecordsToLeader(
                kvReplica,
                DataTestUtils.genKvRecordBatch(new Object[] {2, "bbb"}, new Object[] {3, "c"}));

        // restore again
        makeKvReplicaAsLeader(kvReplica, 3);
        expectedKeyValues =
                getKeyValuePairs(
                        genKvRecords(
                                new Object[] {1, "a"},
                                new Object[] {2, "bbb"},
                                new Object[] {3, "c"}));
        kvTablet = kvReplica.getKvTablet();
        verifyGetKeyValues(kvTablet, expectedKeyValues);
    }

    private void makeLogReplicaAsLeader(Replica replica) throws Exception {
        makeLeaderReplica(
                replica,
                DATA1_TABLE_PATH,
                new TableBucket(DATA1_TABLE_ID, 1),
                INITIAL_LEADER_EPOCH);
    }

    private void makeKvReplicaAsLeader(Replica replica) throws Exception {
        makeLeaderReplica(
                replica,
                DATA1_TABLE_PATH_PK,
                new TableBucket(DATA1_TABLE_ID_PK, 1),
                INITIAL_LEADER_EPOCH);
    }

    private void makeKvReplicaAsLeader(Replica replica, int leaderEpoch) throws Exception {
        makeLeaderReplica(
                replica, DATA1_TABLE_PATH_PK, new TableBucket(DATA1_TABLE_ID_PK, 1), leaderEpoch);
    }

    private void makeKvReplicaAsFollower(Replica replica, int leaderEpoch) {
        replica.makeFollower(
                new NotifyLeaderAndIsrData(
                        PhysicalTablePath.of(DATA1_TABLE_PATH_PK),
                        new TableBucket(DATA1_TABLE_ID_PK, 1),
                        Collections.singletonList(TABLET_SERVER_ID),
                        new LeaderAndIsr(
                                TABLET_SERVER_ID,
                                leaderEpoch,
                                Collections.singletonList(TABLET_SERVER_ID),
                                INITIAL_COORDINATOR_EPOCH,
                                // we also use the leader epoch as bucket epoch
                                leaderEpoch)));
    }

    private void makeLeaderReplica(
            Replica replica, TablePath tablePath, TableBucket tableBucket, int leaderEpoch)
            throws Exception {
        replica.makeLeader(
                new NotifyLeaderAndIsrData(
                        PhysicalTablePath.of(tablePath),
                        tableBucket,
                        Collections.singletonList(TABLET_SERVER_ID),
                        new LeaderAndIsr(
                                TABLET_SERVER_ID,
                                leaderEpoch,
                                Collections.singletonList(TABLET_SERVER_ID),
                                INITIAL_COORDINATOR_EPOCH,
                                // we also use the leader epoch as bucket epoch
                                leaderEpoch)));
    }

    private static LogRecords fetchRecords(Replica replica) throws IOException {
        return fetchRecords(replica, 0);
    }

    private static LogRecords fetchRecords(Replica replica, long offset) throws IOException {
        FetchParams fetchParams = new FetchParams(-1, Integer.MAX_VALUE);
        fetchParams.setCurrentFetch(
                replica.getTableBucket().getTableId(),
                offset,
                Integer.MAX_VALUE,
                replica.getRowType(),
                null);
        LogReadInfo logReadInfo = replica.fetchRecords(fetchParams);
        return logReadInfo.getFetchedData().getRecords();
    }

    private static MemoryLogRecords logRecords(
            long baseOffset, List<RowKind> rowKinds, List<Object[]> values) throws Exception {
        return createBasicMemoryLogRecords(
                DATA1_ROW_TYPE,
                DEFAULT_SCHEMA_ID,
                baseOffset,
                -1L,
                NO_WRITER_ID,
                NO_BATCH_SEQUENCE,
                rowKinds,
                values,
                LogFormat.ARROW);
    }

    private LogAppendInfo putRecordsToLeader(
            Replica replica, KvRecordBatch kvRecords, int[] targetColumns) throws Exception {
        LogAppendInfo logAppendInfo = replica.putRecordsToLeader(kvRecords, targetColumns, 0);
        KvTablet kvTablet = checkNotNull(replica.getKvTablet());
        // flush to make data visible
        kvTablet.flush(replica.getLocalLogEndOffset(), NOPErrorHandler.INSTANCE);
        return logAppendInfo;
    }

    private LogAppendInfo putRecordsToLeader(Replica replica, KvRecordBatch kvRecords)
            throws Exception {
        return putRecordsToLeader(replica, kvRecords, null);
    }

    private void verifyGetKeyValues(
            KvTablet kvTablet, List<Tuple2<byte[], byte[]>> expectedKeyValues) throws IOException {
        List<byte[]> keys = new ArrayList<>();
        List<byte[]> expectValues = new ArrayList<>();
        for (Tuple2<byte[], byte[]> expectedKeyValue : expectedKeyValues) {
            keys.add(expectedKeyValue.f0);
            expectValues.add(expectedKeyValue.f1);
        }
        assertThat(kvTablet.multiGet(keys)).containsExactlyElementsOf(expectValues);
    }
}
