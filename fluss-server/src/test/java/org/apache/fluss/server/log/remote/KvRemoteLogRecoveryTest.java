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

package org.apache.fluss.server.log.remote;

import org.apache.fluss.exception.KvStorageException;
import org.apache.fluss.exception.OutOfOrderSequenceException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.remote.RemoteLogManifest;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.types.Tuple2;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_KEY_TYPE;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static org.apache.fluss.server.coordinator.CoordinatorContext.INITIAL_COORDINATOR_EPOCH;
import static org.apache.fluss.server.kv.KvTabletTestUtils.flushAndWait;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatchWithWriterId;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecords;
import static org.apache.fluss.testutils.DataTestUtils.getKeyValuePairs;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests KV replay and writer state recovery after losing the local WAL. */
class KvRemoteLogRecoveryTest extends RemoteLogTestBase {

    private static final long WRITER_ID = 123;

    @Test
    void testRecoverKvWithWriterSnapshot() throws Exception {
        Replica replica = prepareEmptyReplica();
        LogTablet log = replica.getLogTablet();
        int recoveredEnd = DATA1.size();

        try (TestingRecoveryAppender appender = new TestingRecoveryAppender()) {
            recoverReplica(replica, 1);
            assertThat(appender.messages).isEmpty();
        }
        assertThat(log.localLogStartOffset()).isEqualTo(recoveredEnd);
        assertThat(log.localLogEndOffset()).isEqualTo(recoveredEnd);
        assertThat(log.getHighWatermark()).isEqualTo(recoveredEnd);
        assertKvValues(replica, recoveredEnd);

        // A retry at the already recovered boundary must still replay KV correctly.
        recoverReplica(replica, 2);
        assertKvValues(replica, recoveredEnd);
        KvRecordBatch lastBatch =
                batch(
                        Collections.singletonList(DATA1.get(recoveredEnd - 1)),
                        WRITER_ID,
                        recoveredEnd - 1);
        assertThat(log.writerStateManager().lastEntry(WRITER_ID)).isPresent();
        replica.putRecordsToLeader(lastBatch, null, MergeMode.DEFAULT, 0);
        assertThat(log.localLogEndOffset()).isEqualTo(recoveredEnd);

        replica.putRecordsToLeader(
                batch(Collections.singletonList(new Object[] {11, "after-recovery"}), 456, 0),
                null,
                MergeMode.DEFAULT,
                0);
        flushAndWait(replica.getKvTablet(), recoveredEnd + 1);
        assertThat(log.localLogEndOffset()).isEqualTo(recoveredEnd + 1);
        assertThat(replica.getRowCount()).isEqualTo(recoveredEnd + 1);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testCorruptWriterSnapshotReportsDegradationButValidEmptySnapshotDoesNot(boolean corrupt)
            throws Exception {
        Replica replica = prepareEmptyReplica();
        LogTablet log = replica.getLogTablet();
        File writerSnapshot = new File(remoteWriterSnapshot(remoteSegment(replica)).getPath());
        if (corrupt) {
            // An empty file is corrupt; a successfully loaded snapshot with zero writers is not.
            try (RandomAccessFile file = new RandomAccessFile(writerSnapshot, "rw")) {
                file.setLength(0);
            }
        } else {
            log.writerStateManager().updateMapEndOffset(10);
            log.writerStateManager().takeSnapshot();
            Files.copy(
                    FlussPaths.writerSnapshotFile(log.getLogDir(), 10).toPath(),
                    writerSnapshot.toPath(),
                    StandardCopyOption.REPLACE_EXISTING);
            logManager.truncateFullyAndStartAt(replica.getTableBucket(), 0);
        }

        try (TestingRecoveryAppender appender = new TestingRecoveryAppender()) {
            // Retry at the recovered boundary too: the replacement empty local snapshot must
            // not disguise a corrupt downloaded snapshot as successfully restored writer state.
            for (int epoch = 1; epoch <= 2; epoch++) {
                appender.messages.clear();
                recoverReplica(replica, epoch);
                assertKvValues(replica, 10);
                assertThat(log.localLogStartOffset()).isEqualTo(10);
                assertThat(log.localLogEndOffset()).isEqualTo(10);
                assertThat(log.getHighWatermark()).isEqualTo(10);
                assertThat(log.writerStateManager().activeWriters()).isEmpty();
                assertThat(log.writerStateManager().latestSnapshotOffset()).contains(10L);
                if (corrupt) {
                    assertDegradedRecovery(appender, replica, 0, 10, 10, "corrupt_writer_snapshot");
                } else {
                    assertThat(appender.messages).isEmpty();
                }
            }
        }
        assertThatThrownBy(
                        () ->
                                replica.putRecordsToLeader(
                                        batch(
                                                Collections.singletonList(DATA1.get(9)),
                                                WRITER_ID,
                                                9),
                                        null,
                                        MergeMode.DEFAULT,
                                        0))
                .isInstanceOf(OutOfOrderSequenceException.class);
    }

    @ParameterizedTest
    @CsvSource({"20,10", "20,-1", "0,-1"})
    void testSnapshotAheadOfRemoteWalReportsWriterStateDegradation(
            long snapshotOffset, long remoteEndOffset) throws Exception {
        Replica replica;
        if (remoteEndOffset > 0) {
            replica = prepareEmptyReplica();
            FsPath writerSnapshot = remoteWriterSnapshot(remoteSegment(replica));
            // The old remote writer snapshot must not be used at the newer recovery boundary.
            assertThat(writerSnapshot.getFileSystem().delete(writerSnapshot, false)).isTrue();
        } else {
            makeKvTableAsLeader(DATA1_TABLE_ID_PK, DATA1_TABLE_PATH_PK, 0);
            replica = replicaManager.getReplicaOrException(new TableBucket(DATA1_TABLE_ID_PK, 0));
        }

        try (TestingRecoveryAppender appender = new TestingRecoveryAppender()) {
            // snapshotOffset models KV state already restored by the caller. Exercise the log
            // boundary and writer state initialization, including interrupted recovery.
            for (int attempt = 0; attempt < 2; attempt++) {
                appender.messages.clear();
                remoteLogManager.recoverEmptyKvLog(replica, snapshotOffset);
                assertThat(replica.getLocalLogStartOffset()).isEqualTo(snapshotOffset);
                assertThat(replica.getLocalLogEndOffset()).isEqualTo(snapshotOffset);
                assertThat(replica.getLogHighWatermark()).isEqualTo(snapshotOffset);
                assertThat(replica.getLogTablet().writerStateManager().activeWriters()).isEmpty();
                if (snapshotOffset > 0) {
                    assertDegradedRecovery(
                            appender,
                            replica,
                            snapshotOffset,
                            remoteEndOffset,
                            snapshotOffset,
                            "kv_snapshot_ahead_of_remote_wal");
                } else {
                    assertThat(appender.messages).isEmpty();
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(longs = {10, 20})
    void testEndClippedRemoteTailCannotRecover(long copiedEndOffset) throws Exception {
        Replica replica = prepareEmptyReplica();
        // Even with a complete physical file, recovery to logical end 5 is unsafe: a follower
        // still jumps to physical end 10 and could skip new leader writes in [5, 10). Reject
        // before advancing the local boundary, whether the copied watermark is 10 or beyond it.
        loadManifest(replica, remoteSegment(replica).withLogicalRange(0, 5), copiedEndOffset);

        try (TestingRecoveryAppender appender = new TestingRecoveryAppender()) {
            assertThatThrownBy(() -> recoverReplica(replica, 1))
                    .isInstanceOf(KvStorageException.class)
                    .hasStackTraceContaining(
                            "copied offset " + copiedEndOffset + " is beyond snapshot offset 0");
            assertThat(appender.messages).isEmpty();
        }
        assertThat(replica.getLocalLogStartOffset()).isZero();
        assertThat(replica.getLocalLogEndOffset()).isZero();
        assertThat(replica.getLogHighWatermark()).isZero();
        assertThat(replica.getLogTablet().writerStateManager().activeWriters()).isEmpty();
        assertThat(replica.getKvTablet()).isNull();
    }

    @Test
    void testRemoteLogWithMissingRecoveryRangeFails() throws Exception {
        Replica replica = prepareEmptyReplica();
        loadManifest(replica, remoteSegment(replica).withLogicalRange(2, 10), 10);

        assertThatThrownBy(() -> recoverReplica(replica, 1))
                .isInstanceOf(KvStorageException.class)
                .hasStackTraceContaining("remote log is missing offsets [0, 10)");
        assertThat(replica.getKvTablet()).isNull();
    }

    @Test
    void testTruncatedRequiredWalFails() throws Exception {
        Replica replica = prepareEmptyReplica();
        RemoteLogSegment segment = remoteSegment(replica);
        FsPath logFile =
                FlussPaths.remoteLogSegmentFile(
                        FlussPaths.remoteLogSegmentDir(
                                remoteLogManager
                                        .remoteLogTablet(replica.getTableBucket())
                                        .getRemoteLogDir(),
                                segment),
                        segment.remoteLogStartOffset());
        int position = remoteLogManager.lookupPositionForOffset(segment, 9);
        try (RandomAccessFile file = new RandomAccessFile(new File(logFile.getPath()), "rw")) {
            file.setLength(position);
        }

        assertThatThrownBy(() -> recoverReplica(replica, 1))
                .isInstanceOf(KvStorageException.class)
                .hasStackTraceContaining("ended at offset 9 before required offset 10");
        assertThat(replica.getKvTablet()).isNull();
    }

    private Replica prepareEmptyReplica() throws Exception {
        TableBucket bucket = new TableBucket(DATA1_TABLE_ID_PK, 0);
        makeKvTableAsLeader(DATA1_TABLE_ID_PK, DATA1_TABLE_PATH_PK, 0);
        Replica replica = replicaManager.getReplicaOrException(bucket);
        for (int i = 0; i < DATA1.size(); i++) {
            replica.putRecordsToLeader(
                    batch(Collections.singletonList(DATA1.get(i)), WRITER_ID, i),
                    null,
                    MergeMode.DEFAULT,
                    0);
        }
        flushAndWait(replica.getKvTablet(), DATA1.size());
        LogTablet log = replica.getLogTablet();
        // Flush progress is visible before its callback advances the high watermark. Tiering
        // only uploads committed segments, so wait for that callback before triggering it.
        waitUntil(
                () -> log.getHighWatermark() == DATA1.size(),
                Duration.ofSeconds(30),
                "High watermark did not reach the flushed KV offset.");
        log.roll(Optional.empty());
        remoteLogTaskScheduler.triggerPeriodicScheduledTasks();
        assertThat(remoteSegment(replica).remoteLogEndOffset()).isEqualTo(DATA1.size());
        logManager.truncateFullyAndStartAt(bucket, 0);
        return replica;
    }

    private RemoteLogSegment remoteSegment(Replica replica) {
        List<RemoteLogSegment> segments =
                remoteLogManager.relevantRemoteLogSegments(replica.getTableBucket(), 0);
        assertThat(segments).hasSize(1);
        return segments.get(0);
    }

    private FsPath remoteWriterSnapshot(RemoteLogSegment segment) {
        return FlussPaths.remoteWriterSnapshotFile(
                remoteLogManager.remoteLogTablet(segment.tableBucket()).getRemoteLogDir(),
                segment,
                RemoteLogStorage.IndexType.getFileSuffix(
                        RemoteLogStorage.IndexType.WRITER_ID_SNAPSHOT));
    }

    private void loadManifest(Replica replica, RemoteLogSegment segment, long copiedEnd) {
        remoteLogManager
                .remoteLogTablet(replica.getTableBucket())
                .loadRemoteLogManifest(
                        new RemoteLogManifest(
                                replica.getLogTablet().getPhysicalTablePath(),
                                replica.getTableBucket(),
                                Collections.singletonList(segment),
                                copiedEnd));
    }

    private void recoverReplica(Replica replica, int epoch) throws Exception {
        replica.makeLeader(
                new NotifyLeaderAndIsrData(
                        replica.getLogTablet().getPhysicalTablePath(),
                        replica.getTableBucket(),
                        Collections.singletonList(TABLET_SERVER_ID),
                        new LeaderAndIsr(
                                TABLET_SERVER_ID,
                                epoch,
                                Collections.singletonList(TABLET_SERVER_ID),
                                Collections.emptyList(),
                                INITIAL_COORDINATOR_EPOCH,
                                epoch),
                        TEST_ROUTING_BUCKET_COUNT,
                        0L));
    }

    private static KvRecordBatch batch(List<Object[]> rows, long writerId, int sequence)
            throws Exception {
        return genKvRecordBatchWithWriterId(
                rows.stream()
                        .map(row -> Tuple2.of(new Object[] {row[0]}, row))
                        .collect(Collectors.toList()),
                DATA1_KEY_TYPE,
                DATA1_ROW_TYPE,
                writerId,
                sequence);
    }

    private static void assertKvValues(Replica replica, int recoveredEnd) throws Exception {
        List<Tuple2<byte[], byte[]>> expected =
                getKeyValuePairs(genKvRecords(DATA1.toArray(new Object[0][])));
        for (int i = 0; i < expected.size(); i++) {
            if (i < recoveredEnd) {
                assertThat(
                                replica.getKvTablet()
                                        .multiGet(Collections.singletonList(expected.get(i).f0))
                                        .get(0)
                                        .toByteArray())
                        .isEqualTo(expected.get(i).f1);
            } else {
                assertThat(
                                replica.getKvTablet()
                                        .multiGet(Collections.singletonList(expected.get(i).f0))
                                        .get(0))
                        .isNull();
            }
        }
        assertThat(replica.getRowCount()).isEqualTo(recoveredEnd);
    }

    private static void assertDegradedRecovery(
            TestingRecoveryAppender appender,
            Replica replica,
            long snapshotOffset,
            long remoteEndOffset,
            long recoveredEndOffset,
            String reason) {
        assertThat(appender.messages)
                .singleElement()
                .asString()
                .contains(
                        "bucket=" + replica.getTableBucket(),
                        "snapshotOffset=" + snapshotOffset,
                        "remoteEndOffset=" + remoteEndOffset,
                        "recoveredEndOffset=" + recoveredEndOffset,
                        "reason=" + reason,
                        "Previous writer sequence and deduplication state was not restored");
    }

    private static class TestingRecoveryAppender extends AbstractAppender implements AutoCloseable {
        private final Logger logger = (Logger) LogManager.getLogger(RemoteLogManager.class);
        private final Level previousLevel = logger.getLevel();
        private final boolean previousAdditive = logger.isAdditive();
        private final List<String> messages = new ArrayList<>();

        private TestingRecoveryAppender() {
            super("recovery", null, null, true, Property.EMPTY_ARRAY);
            start();
            logger.addAppender(this);
            logger.setLevel(Level.WARN);
            logger.setAdditive(false);
        }

        @Override
        public void append(LogEvent event) {
            String message = event.getMessage().getFormattedMessage();
            if (event.getLevel() == Level.WARN && message.contains("degraded writer state")) {
                messages.add(message);
            }
        }

        @Override
        public void close() {
            logger.removeAppender(this);
            logger.setLevel(previousLevel);
            logger.setAdditive(previousAdditive);
            stop();
        }
    }
}
