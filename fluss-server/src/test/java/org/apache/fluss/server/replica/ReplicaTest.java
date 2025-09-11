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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.OutOfOrderSequenceException;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.KvRecordTestUtils;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.kv.KvTablet;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.kv.snapshot.TestingCompletedKvSnapshotCommitter;
import org.apache.fluss.server.log.FetchParams;
import org.apache.fluss.server.log.LogAppendInfo;
import org.apache.fluss.server.log.LogReadInfo;
import org.apache.fluss.server.testutils.KvTestUtils;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.testutils.DataTestUtils;
import org.apache.fluss.testutils.common.ManuallyTriggeredScheduledExecutorService;
import org.apache.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.LogRecordBatch.CURRENT_LOG_MAGIC_VALUE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_BATCH_SEQUENCE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_WRITER_ID;
import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_PHYSICAL_TABLE_PATH;
import static org.apache.fluss.record.TestData.DATA1_PHYSICAL_TABLE_PATH_PK;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.server.coordinator.CoordinatorContext.INITIAL_COORDINATOR_EPOCH;
import static org.apache.fluss.server.zk.data.LeaderAndIsr.INITIAL_LEADER_EPOCH;
import static org.apache.fluss.testutils.DataTestUtils.assertLogRecordsEquals;
import static org.apache.fluss.testutils.DataTestUtils.createBasicMemoryLogRecords;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecords;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsWithWriterId;
import static org.apache.fluss.testutils.DataTestUtils.getKeyValuePairs;
import static org.apache.fluss.testutils.LogRecordsAssert.assertThatLogRecords;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        fetchParams.setCurrentFetch(
                DATA1_TABLE_ID, 0, Integer.MAX_VALUE, DATA1_ROW_TYPE, DEFAULT_COMPRESSION, null);
        LogReadInfo logReadInfo = logReplica.fetchRecords(fetchParams);
        assertLogRecordsEquals(DATA1_ROW_TYPE, logReadInfo.getFetchedData().getRecords(), DATA1);
    }

    @Test
    void testAppendRecordsWithOutOfOrderBatchSequence() throws Exception {
        Replica logReplica =
                makeLogReplica(DATA1_PHYSICAL_TABLE_PATH, new TableBucket(DATA1_TABLE_ID, 1));
        makeLogReplicaAsLeader(logReplica);

        long writerId = 101L;

        // 1. append a batch with batchSequence = 0
        logReplica.appendRecordsToLeader(genMemoryLogRecordsWithWriterId(DATA1, writerId, 0, 0), 0);

        // manual advance time and remove expired writer, the state of writer 101 will be removed
        manualClock.advanceTime(Duration.ofHours(12));
        manualClock.advanceTime(Duration.ofSeconds(1));
        assertThat(logReplica.getLogTablet().writerStateManager().activeWriters().size())
                .isEqualTo(1);
        logReplica.getLogTablet().removeExpiredWriter(manualClock.milliseconds());
        assertThat(logReplica.getLogTablet().writerStateManager().activeWriters().size())
                .isEqualTo(0);

        // 2. try to append an out of ordered batch as leader, will throw
        // OutOfOrderSequenceException
        assertThatThrownBy(
                        () ->
                                logReplica.appendRecordsToLeader(
                                        genMemoryLogRecordsWithWriterId(DATA1, writerId, 2, 10), 0))
                .isInstanceOf(OutOfOrderSequenceException.class);
        assertThat(logReplica.getLocalLogEndOffset()).isEqualTo(10);

        // 3. try to append an out of ordered batch as follower
        logReplica.appendRecordsToFollower(genMemoryLogRecordsWithWriterId(DATA1, writerId, 2, 10));
        assertThat(logReplica.getLocalLogEndOffset()).isEqualTo(20);
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
                                ChangeType.UPDATE_BEFORE,
                                ChangeType.UPDATE_AFTER,
                                ChangeType.UPDATE_BEFORE,
                                ChangeType.UPDATE_AFTER,
                                ChangeType.UPDATE_BEFORE,
                                ChangeType.UPDATE_AFTER),
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
                                ChangeType.INSERT,
                                ChangeType.UPDATE_BEFORE,
                                ChangeType.UPDATE_AFTER,
                                ChangeType.INSERT),
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
                                ChangeType.DELETE,
                                ChangeType.UPDATE_BEFORE,
                                ChangeType.UPDATE_AFTER,
                                ChangeType.UPDATE_BEFORE,
                                ChangeType.UPDATE_AFTER),
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
                        Arrays.asList(ChangeType.INSERT, ChangeType.DELETE, ChangeType.INSERT),
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

        // delete k2 again, will produce a batch with empty record.
        kvRecords = kvRecordBatchFactory.ofRecords(kvRecordFactory.ofRecord("k2", null));
        logAppendInfo = putRecordsToLeader(kvReplica, kvRecords);
        assertThat(logAppendInfo.lastOffset()).isEqualTo(12);
        LogRecords logRecords = fetchRecords(kvReplica, currentOffset);
        Iterator<LogRecordBatch> iterator = logRecords.batches().iterator();
        assertThat(iterator.hasNext()).isTrue();
        LogRecordBatch batch = iterator.next();
        assertThat(batch.getRecordCount()).isEqualTo(0);
        currentOffset += 1;

        // delete k1 and put k1 again, should produce -D, +I
        kvRecords =
                kvRecordBatchFactory.ofRecords(
                        kvRecordFactory.ofRecord("k1", null),
                        kvRecordFactory.ofRecord("k1", new Object[] {1, "aaa"}));
        logAppendInfo = putRecordsToLeader(kvReplica, kvRecords);
        assertThat(logAppendInfo.lastOffset()).isEqualTo(14);
        expected =
                logRecords(
                        currentOffset,
                        Arrays.asList(ChangeType.DELETE, ChangeType.INSERT),
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
                new TestSnapshotContext(snapshotKvTabletDir.getPath());
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

        // wait until the snapshot 0 success
        CompletedSnapshot completedSnapshot0 =
                kvSnapshotStore.waitUntilSnapshotComplete(tableBucket, 0);

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
        // wait until the snapshot 1 success
        CompletedSnapshot completedSnapshot1 =
                kvSnapshotStore.waitUntilSnapshotComplete(tableBucket, 1);

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
                new TestSnapshotContext(snapshotKvTabletDir.getPath(), kvSnapshotStore);
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
        //  wait until the snapshot 2 success
        CompletedSnapshot completedSnapshot2 =
                kvSnapshotStore.waitUntilSnapshotComplete(tableBucket, 2);
        expectedLogOffset = 10;
        expectedKeyValues =
                getKeyValuePairs(
                        genKvRecords(
                                Tuple2.of("k2", new Object[] {4, "bk21"}),
                                Tuple2.of("k3", new Object[] {5, "k3"})));
        KvTestUtils.checkSnapshot(completedSnapshot2, expectedKeyValues, expectedLogOffset);
    }

    @Test
    void testSnapshotUseLatestLeaderEpoch(@TempDir File snapshotKvTabletDir) throws Exception {
        TableBucket tableBucket = new TableBucket(DATA1_TABLE_ID_PK, 1);
        // create test context
        ImmediateTriggeredScheduledExecutorService immediateTriggeredScheduledExecutorService =
                new ImmediateTriggeredScheduledExecutorService();
        TestSnapshotContext testKvSnapshotContext =
                new TestSnapshotContext(
                        snapshotKvTabletDir.getPath(), immediateTriggeredScheduledExecutorService);
        TestingCompletedKvSnapshotCommitter kvSnapshotStore =
                testKvSnapshotContext.testKvSnapshotStore;

        // make a kv replica
        Replica kvReplica =
                makeKvReplica(DATA1_PHYSICAL_TABLE_PATH_PK, tableBucket, testKvSnapshotContext);
        // now, make the replica as leader
        makeKvReplicaAsLeader(kvReplica, 0);
        KvRecordBatch kvRecords =
                genKvRecordBatch(
                        Tuple2.of("k1", new Object[] {1, "a"}),
                        Tuple2.of("k2", new Object[] {2, "b"}));
        putRecordsToLeader(kvReplica, kvRecords);

        // make leader again with a new epoch, check the snapshot should use the new epoch
        immediateTriggeredScheduledExecutorService.reset();
        int latestLeaderEpoch = 1;
        int snapshot = 0;
        makeKvReplicaAsLeader(kvReplica, latestLeaderEpoch);
        kvSnapshotStore.waitUntilSnapshotComplete(tableBucket, snapshot);
        assertThat(kvSnapshotStore.getSnapshotLeaderEpoch(tableBucket, snapshot))
                .isEqualTo(latestLeaderEpoch);
    }

    @Test
    void testBrokenSnapshotRecovery(@TempDir File snapshotKvTabletDir) throws Exception {
        TableBucket tableBucket = new TableBucket(DATA1_TABLE_ID_PK, 1);

        // create test context with custom snapshot store
        TestSnapshotContext testKvSnapshotContext =
                new TestSnapshotContext(snapshotKvTabletDir.getPath());
        ManuallyTriggeredScheduledExecutorService scheduledExecutorService =
                testKvSnapshotContext.scheduledExecutorService;
        TestingCompletedKvSnapshotCommitter kvSnapshotStore =
                testKvSnapshotContext.testKvSnapshotStore;

        // create a replica and make it leader
        Replica kvReplica =
                makeKvReplica(DATA1_PHYSICAL_TABLE_PATH_PK, tableBucket, testKvSnapshotContext);
        makeKvReplicaAsLeader(kvReplica);

        // put initial data and create first snapshot
        KvRecordBatch kvRecords =
                genKvRecordBatch(
                        Tuple2.of("k1", new Object[] {1, "a"}),
                        Tuple2.of("k2", new Object[] {2, "b"}));
        putRecordsToLeader(kvReplica, kvRecords);

        // trigger first snapshot
        scheduledExecutorService.triggerNonPeriodicScheduledTask();
        kvSnapshotStore.waitUntilSnapshotComplete(tableBucket, 0);

        // put more data and create second snapshot
        kvRecords =
                genKvRecordBatch(
                        Tuple2.of("k1", new Object[] {3, "c"}),
                        Tuple2.of("k3", new Object[] {4, "d"}));
        putRecordsToLeader(kvReplica, kvRecords);

        // trigger second snapshot
        scheduledExecutorService.triggerNonPeriodicScheduledTask();
        kvSnapshotStore.waitUntilSnapshotComplete(tableBucket, 1);

        // put more data and create third snapshot (this will be the broken one)
        kvRecords =
                genKvRecordBatch(
                        Tuple2.of("k4", new Object[] {5, "e"}),
                        Tuple2.of("k5", new Object[] {6, "f"}));
        putRecordsToLeader(kvReplica, kvRecords);

        // trigger third snapshot
        scheduledExecutorService.triggerNonPeriodicScheduledTask();
        CompletedSnapshot snapshot2 = kvSnapshotStore.waitUntilSnapshotComplete(tableBucket, 2);

        // verify that snapshot2 is the latest one before we break it
        assertThat(kvSnapshotStore.getLatestCompletedSnapshot(tableBucket).getSnapshotID())
                .isEqualTo(2);

        // now simulate the latest snapshot (snapshot2) being broken by
        // deleting its metadata files and unshared SST files
        // This simulates file corruption while ZK metadata remains intact
        snapshot2.getKvSnapshotHandle().discard();

        // ZK metadata should still show snapshot2 as latest (file corruption hasn't been detected
        // yet)
        assertThat(kvSnapshotStore.getLatestCompletedSnapshot(tableBucket).getSnapshotID())
                .isEqualTo(2);

        // make the replica follower to destroy the current kv tablet
        makeKvReplicaAsFollower(kvReplica, 1);

        // create a new replica with the same snapshot context
        // During initialization, it will try to use snapshot2 but find it broken,
        // then handle the broken snapshot and fall back to snapshot1
        testKvSnapshotContext =
                new TestSnapshotContext(snapshotKvTabletDir.getPath(), kvSnapshotStore);
        kvReplica = makeKvReplica(DATA1_PHYSICAL_TABLE_PATH_PK, tableBucket, testKvSnapshotContext);

        // make it leader again - this should trigger the broken snapshot recovery logic
        // The system should detect that snapshot2 files are missing, clean up its metadata,
        // and successfully recover using snapshot1
        makeKvReplicaAsLeader(kvReplica, 2);

        // verify that KvTablet is successfully initialized despite the broken snapshot
        assertThat(kvReplica.getKvTablet()).isNotNull();
        KvTablet kvTablet = kvReplica.getKvTablet();

        // verify that the data from snapshot1 is restored (snapshot2 was broken and cleaned up)
        // snapshot1 should contain: k1->3,c and k3->4,d
        List<Tuple2<byte[], byte[]>> expectedKeyValues =
                getKeyValuePairs(
                        genKvRecords(
                                Tuple2.of("k1", new Object[] {3, "c"}),
                                Tuple2.of("k3", new Object[] {4, "d"})));
        verifyGetKeyValues(kvTablet, expectedKeyValues);

        // Verify the core functionality: KvTablet successfully initialized despite broken snapshot
        // The key test is that the system can handle broken snapshots and recover correctly

        // Verify that we successfully simulated the broken snapshot condition
        File metadataFile = new File(snapshot2.getMetadataFilePath().getPath());
        assertThat(metadataFile.exists()).isFalse();
    }

    @Test
    void testRestore(@TempDir Path snapshotKvTabletDirPath) throws Exception {
        TableBucket tableBucket = new TableBucket(DATA1_TABLE_ID_PK, 1);
        TestSnapshotContext testKvSnapshotContext =
                new TestSnapshotContext(snapshotKvTabletDirPath.toString());
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
        // wait until the snapshot success
        kvSnapshotStore.waitUntilSnapshotComplete(tableBucket, 0);

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
                DEFAULT_COMPRESSION,
                null);
        LogReadInfo logReadInfo = replica.fetchRecords(fetchParams);
        return logReadInfo.getFetchedData().getRecords();
    }

    private static MemoryLogRecords logRecords(
            long baseOffset, List<ChangeType> changeTypes, List<Object[]> values) throws Exception {
        return createBasicMemoryLogRecords(
                DATA1_ROW_TYPE,
                DEFAULT_SCHEMA_ID,
                baseOffset,
                -1L,
                CURRENT_LOG_MAGIC_VALUE,
                NO_WRITER_ID,
                NO_BATCH_SEQUENCE,
                changeTypes,
                values,
                LogFormat.ARROW,
                DEFAULT_COMPRESSION);
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

    /** A scheduledExecutorService that will execute the scheduled task immediately. */
    private static class ImmediateTriggeredScheduledExecutorService
            extends ManuallyTriggeredScheduledExecutorService {
        private boolean isScheduled = false;

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            // we only schedule task for once, if has scheduled, return null to skip schedule
            // the task
            if (isScheduled) {
                return null;
            }
            isScheduled = true;
            ScheduledFuture<?> scheduledFuture = super.schedule(command, delay, unit);
            triggerNonPeriodicScheduledTask();
            return scheduledFuture;
        }

        public void reset() {
            isScheduled = false;
        }
    }
}
