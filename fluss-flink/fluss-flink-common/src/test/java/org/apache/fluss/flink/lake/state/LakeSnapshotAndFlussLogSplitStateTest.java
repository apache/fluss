/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.flink.lake.state;

import org.apache.fluss.flink.lake.LakeRecordRecordEmitter;
import org.apache.fluss.flink.lake.LakeSplitReaderGenerator;
import org.apache.fluss.flink.lake.split.LakeSnapshotAndFlussLogSplit;
import org.apache.fluss.flink.source.reader.BoundedSplitReader;
import org.apache.fluss.flink.source.reader.RecordAndPos;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.flink.source.split.SourceSplitSerializer;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.lake.source.TestingLakeSource;
import org.apache.fluss.lake.source.TestingLakeSplit;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.GenericRecord;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link LakeSnapshotAndFlussLogSplitState}, covering which lake splits are checkpointed
 * and that a split restored from any checkpointed read position reads every lake record once.
 */
class LakeSnapshotAndFlussLogSplitStateTest {

    private static final TableBucket TABLE_BUCKET = new TableBucket(1L, 0);
    private static final long SNAPSHOT_LOG_OFFSET = 100L;

    /** Records per lake split; split 2 is empty, as lake splits can be after deletes. */
    private static final int[] LAKE_SPLIT_SIZES = {3, 1, 0, 4, 2};

    private final TestingLakeSource lakeSource = new RowsPerSplitLakeSource(LAKE_SPLIT_SIZES);
    private final SourceSplitSerializer splitSerializer = new SourceSplitSerializer(lakeSource);

    @Test
    void testFinishedStreamingSplitDropsLakeSplits() throws Exception {
        LakeSnapshotAndFlussLogSplit split = streamingSplit();
        LakeSnapshotAndFlussLogSplitState state = new LakeSnapshotAndFlussLogSplitState(split);
        state.setCurrentLakeSplitIndex(4);
        state.setRecordsToSkip(2);
        state.markLakeSplitFinished();
        state.setNextLogOffset(SNAPSHOT_LOG_OFFSET + 7);

        LakeSnapshotAndFlussLogSplit checkpointed = checkpoint(state);

        assertThat(checkpointed.getLakeSplits()).isNull();
        assertThat(checkpointed.isLakeSplitFinished()).isTrue();
        assertThat(checkpointed.getCurrentLakeSplitIndex()).isZero();
        assertThat(checkpointed.getStartingOffset()).isEqualTo(SNAPSHOT_LOG_OFFSET + 7);
        assertThat(checkpointed.isStreaming()).isTrue();
        assertThat(splitSerializer.serialize(checkpointed).length)
                .isLessThan(splitSerializer.serialize(split).length);
        assertSplitUnchanged(split);
    }

    @Test
    void testStreamingSplitKeepsOnlyRemainingLakeSplits() throws Exception {
        LakeSnapshotAndFlussLogSplit split = streamingSplit();
        LakeSnapshotAndFlussLogSplitState state = new LakeSnapshotAndFlussLogSplitState(split);
        state.setCurrentLakeSplitIndex(3);
        state.setRecordsToSkip(2);

        LakeSnapshotAndFlussLogSplit checkpointed = checkpoint(state);

        assertThat(checkpointed.getLakeSplits())
                .extracting(LakeSplit::bucket)
                .containsExactly(3, 4);
        assertThat(checkpointed.getCurrentLakeSplitIndex()).isZero();
        assertThat(checkpointed.getRecordsToSkip()).isEqualTo(2);
        assertThat(checkpointed.isLakeSplitFinished()).isFalse();
        assertThat(checkpointed.getStartingOffset()).isEqualTo(SNAPSHOT_LOG_OFFSET);
        assertSplitUnchanged(split);
    }

    @Test
    void testBatchSplitKeepsAllLakeSplits() throws Exception {
        long stoppingOffset = SNAPSHOT_LOG_OFFSET + 10;
        LakeSnapshotAndFlussLogSplit split =
                new LakeSnapshotAndFlussLogSplit(
                        TABLE_BUCKET, null, lakeSplits(), SNAPSHOT_LOG_OFFSET, stoppingOffset);
        LakeSnapshotAndFlussLogSplitState state = new LakeSnapshotAndFlussLogSplitState(split);
        state.setRecordsToSkip(5);
        state.markLakeSplitFinished();

        LakeSnapshotAndFlussLogSplit checkpointed = checkpoint(state);

        assertThat(checkpointed.getLakeSplits())
                .extracting(LakeSplit::bucket)
                .containsExactly(0, 1, 2, 3, 4);
        assertThat(checkpointed.getRecordsToSkip()).isEqualTo(5);
        assertThat(checkpointed.isLakeSplitFinished()).isTrue();
        assertThat(checkpointed.getStoppingOffset()).hasValue(stoppingOffset);
    }

    @Test
    void testStreamingSplitWithoutLakeSnapshot() throws Exception {
        LakeSnapshotAndFlussLogSplit split =
                new LakeSnapshotAndFlussLogSplit(
                        TABLE_BUCKET, null, null, 0L, LogSplit.NO_STOPPING_OFFSET);
        LakeSnapshotAndFlussLogSplitState state = new LakeSnapshotAndFlussLogSplitState(split);
        state.setNextLogOffset(5L);

        LakeSnapshotAndFlussLogSplit checkpointed = checkpoint(state);

        assertThat(checkpointed.getLakeSplits()).isNull();
        assertThat(checkpointed.isLakeSplitFinished()).isTrue();
        assertThat(checkpointed.getStartingOffset()).isEqualTo(5L);
    }

    @Test
    void testRestoreFromEveryCheckpointedPosition() throws Exception {
        List<String> allRecords = readAll(streamingSplit());
        assertThat(allRecords).hasSize(sum(LAKE_SPLIT_SIZES));

        for (int recordsBeforeCheckpoint = 0;
                recordsBeforeCheckpoint <= allRecords.size();
                recordsBeforeCheckpoint++) {
            LakeSnapshotAndFlussLogSplitState state =
                    new LakeSnapshotAndFlussLogSplitState(streamingSplit());
            List<String> records = new ArrayList<>();
            read(state, recordsBeforeCheckpoint, records);

            LakeSnapshotAndFlussLogSplit checkpointed = checkpoint(state);
            // the lake splits before the one of the last read record are not checkpointed
            int currentLakeSplit =
                    recordsBeforeCheckpoint == 0 ? 0 : lakeSplitOf(records.get(records.size() - 1));
            int remainingLakeSplits = LAKE_SPLIT_SIZES.length - currentLakeSplit;
            assertThat(checkpointed.getLakeSplits())
                    .as("records before checkpoint: %s", recordsBeforeCheckpoint)
                    .hasSize(remainingLakeSplits);

            LakeSnapshotAndFlussLogSplitState restored =
                    new LakeSnapshotAndFlussLogSplitState(checkpointed);
            read(restored, Integer.MAX_VALUE, records);

            assertThat(records)
                    .as("records before checkpoint: %s", recordsBeforeCheckpoint)
                    .containsExactlyElementsOf(allRecords);
            assertThat(checkpoint(restored).isLakeSplitFinished()).isTrue();
            assertThat(checkpoint(restored).getLakeSplits()).isNull();
        }
    }

    @Test
    void testRestoreFromGlobalRecordsToSkip() throws Exception {
        // checkpoints taken before the read position was split-local hold all lake splits, index
        // 0 and the count of records read across all lake splits
        List<String> allRecords = readAll(streamingSplit());
        for (int recordsToSkip = 0; recordsToSkip <= allRecords.size(); recordsToSkip++) {
            LakeSnapshotAndFlussLogSplit legacySplit =
                    new LakeSnapshotAndFlussLogSplit(
                            TABLE_BUCKET,
                            null,
                            lakeSplits(),
                            SNAPSHOT_LOG_OFFSET,
                            LogSplit.NO_STOPPING_OFFSET,
                            recordsToSkip,
                            0,
                            false);
            List<String> records = new ArrayList<>();
            LakeSnapshotAndFlussLogSplitState state =
                    new LakeSnapshotAndFlussLogSplitState(roundTrip(legacySplit));
            // read one record, checkpoint with a split-local position, and read the rest
            read(state, 1, records);
            read(
                    new LakeSnapshotAndFlussLogSplitState(checkpoint(state)),
                    Integer.MAX_VALUE,
                    records);

            assertThat(records)
                    .as("records to skip: %s", recordsToSkip)
                    .containsExactlyElementsOf(
                            allRecords.subList(recordsToSkip, allRecords.size()));
        }
    }

    // ------------------------------------------------------------------------------------------

    private static LakeSnapshotAndFlussLogSplit streamingSplit() {
        return new LakeSnapshotAndFlussLogSplit(
                TABLE_BUCKET, null, lakeSplits(), SNAPSHOT_LOG_OFFSET, LogSplit.NO_STOPPING_OFFSET);
    }

    private static List<LakeSplit> lakeSplits() {
        List<LakeSplit> lakeSplits = new ArrayList<>();
        for (int i = 0; i < LAKE_SPLIT_SIZES.length; i++) {
            // the bucket identifies the lake split in RowsPerSplitLakeSource
            lakeSplits.add(new TestingLakeSplit(i, Collections.emptyList()));
        }
        return lakeSplits;
    }

    private static void assertSplitUnchanged(LakeSnapshotAndFlussLogSplit split) {
        assertThat(split.getLakeSplits()).hasSize(LAKE_SPLIT_SIZES.length);
        assertThat(split.getCurrentLakeSplitIndex()).isZero();
        assertThat(split.getRecordsToSkip()).isZero();
        assertThat(split.isLakeSplitFinished()).isFalse();
        assertThat(split.getStartingOffset()).isEqualTo(SNAPSHOT_LOG_OFFSET);
    }

    /** The lake split a record read by {@link #read} comes from. */
    private static int lakeSplitOf(String record) {
        return Integer.parseInt(record.split("-")[0]);
    }

    private static int sum(int[] values) {
        int sum = 0;
        for (int value : values) {
            sum += value;
        }
        return sum;
    }

    /** Takes the split of the state as a checkpoint does, including the serialization. */
    private LakeSnapshotAndFlussLogSplit checkpoint(LakeSnapshotAndFlussLogSplitState state)
            throws Exception {
        return roundTrip((LakeSnapshotAndFlussLogSplit) state.toSourceSplit());
    }

    private LakeSnapshotAndFlussLogSplit roundTrip(LakeSnapshotAndFlussLogSplit split)
            throws Exception {
        return (LakeSnapshotAndFlussLogSplit)
                splitSerializer.deserialize(
                        splitSerializer.getVersion(), splitSerializer.serialize(split));
    }

    private List<String> readAll(LakeSnapshotAndFlussLogSplit split) throws Exception {
        List<String> records = new ArrayList<>();
        read(new LakeSnapshotAndFlussLogSplitState(split), Integer.MAX_VALUE, records);
        return records;
    }

    /**
     * Reads up to {@code maxRecords} lake records of the split of the state like the source reader
     * does, emitting them to update the state.
     */
    private void read(LakeSnapshotAndFlussLogSplitState state, int maxRecords, List<String> output)
            throws Exception {
        LakeSnapshotAndFlussLogSplit split = (LakeSnapshotAndFlussLogSplit) state.toSourceSplit();
        if (split.isLakeSplitFinished()) {
            return;
        }
        LakeRecordRecordEmitter<String> emitter =
                new LakeRecordRecordEmitter<>(
                        (record, out) ->
                                output.add(
                                        record.getRow().getInt(0)
                                                + "-"
                                                + record.getRow().getInt(1)));
        LakeSplitReaderGenerator readerGenerator =
                new LakeSplitReaderGenerator(null, null, lakeSource);
        int emitted = 0;
        try (BoundedSplitReader reader = readerGenerator.getBoundedSplitScanner(split)) {
            CloseableIterator<RecordAndPos> batch;
            while (emitted < maxRecords && (batch = reader.readBatch()) != null) {
                try {
                    while (emitted < maxRecords && batch.hasNext()) {
                        emitter.emitRecord(state, null, batch.next());
                        emitted++;
                    }
                } finally {
                    batch.close();
                }
            }
            if (emitted < maxRecords) {
                // the split reader signals the end of the lake phase of a streaming split
                emitter.emitRecord(state, null, RecordAndPos.snapshotPhaseFinished());
            }
        }
    }

    /** A lake source whose lake split {@code i} holds {@code sizes[i]} records. */
    private static class RowsPerSplitLakeSource extends TestingLakeSource {

        private static final long serialVersionUID = 1L;

        private final int[] sizes;

        private RowsPerSplitLakeSource(int[] sizes) {
            this.sizes = sizes;
        }

        @Override
        public RecordReader createRecordReader(ReaderContext<LakeSplit> context) {
            int splitId = context.lakeSplit().bucket();
            List<LogRecord> records = new ArrayList<>();
            for (int i = 0; i < sizes[splitId]; i++) {
                records.add(
                        new GenericRecord(-1, -1, ChangeType.INSERT, GenericRow.of(splitId, i)));
            }
            return () -> CloseableIterator.wrap(records.iterator());
        }
    }
}
