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

package org.apache.fluss.flink.sink.testutils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

/**
 * A counting source that can inject failures at specified points for testing checkpoint-based
 * failover recovery.
 *
 * <p>This source emits records like CountingSource but throws a RuntimeException after emitting a
 * configured number of records. The failure is "one-shot" - after recovery from checkpoint, the
 * source continues without failing again.
 *
 * <p>The source operates in three phases:
 *
 * <ol>
 *   <li>Phase 1: Emit {@code recordsBeforeFailure} records
 *   <li>Phase 2: Throw RuntimeException to trigger job failover
 *   <li>Phase 3: After recovery, emit {@code recordsAfterFailure} records
 * </ol>
 *
 * <p>The {@code hasFailed} flag is tracked using a file-based marker. This allows failure state to
 * persist across Enumerator and Reader restarts, even when different ClassLoaders are used.
 */
public class FailingCountingSource
        implements Source<
                        RowData,
                        FailingCountingSource.FailingCountingSplit,
                        FailingCountingSource.FailingEnumeratorState>,
                ResultTypeQueryable<RowData> {

    private static final long serialVersionUID = 1L;

    // Unique identifier for this source instance
    private final String sourceId;
    // Directory for failure marker files (passed from test, survives ClassLoader changes)
    private final String markerDirPath;
    private final long key;
    private final long value;
    private final int recordsBeforeFailure;
    private final int recordsAfterFailure;
    private final boolean multiKey;

    private FailingCountingSource(
            String sourceId,
            String markerDirPath,
            long key,
            long value,
            int recordsBeforeFailure,
            int recordsAfterFailure,
            boolean multiKey) {
        this.sourceId = sourceId;
        this.markerDirPath = markerDirPath;
        this.key = key;
        this.value = value;
        this.recordsBeforeFailure = recordsBeforeFailure;
        this.recordsAfterFailure = recordsAfterFailure;
        this.multiKey = multiKey;
    }

    /**
     * Creates a single-key source that emits records for the specified key with failure injection.
     *
     * @param markerDir directory for failure marker files (use @TempDir for test isolation)
     * @param key the key to emit records for
     * @param value the value for each record
     * @param recordsBeforeFailure number of records to emit before triggering failure
     * @param recordsAfterFailure number of records to emit after recovery
     * @return a new FailingCountingSource instance
     */
    public static FailingCountingSource singleKey(
            java.io.File markerDir,
            long key,
            long value,
            int recordsBeforeFailure,
            int recordsAfterFailure) {
        String sourceId = "single-" + key + "-" + System.nanoTime();
        return new FailingCountingSource(
                sourceId,
                markerDir.getAbsolutePath(),
                key,
                value,
                recordsBeforeFailure,
                recordsAfterFailure,
                false);
    }

    /**
     * Creates a multi-key source that emits records for keys 1, 2, 3 in round-robin with failure
     * injection.
     *
     * @param markerDir directory for failure marker files (use @TempDir for test isolation)
     * @param value the value for each record
     * @param recordsBeforeFailurePerKey number of records per key to emit before triggering failure
     * @param recordsAfterFailurePerKey number of records per key to emit after recovery
     * @return a new FailingCountingSource instance
     */
    public static FailingCountingSource multiKey(
            java.io.File markerDir,
            long value,
            int recordsBeforeFailurePerKey,
            int recordsAfterFailurePerKey) {
        String sourceId = "multi-" + System.nanoTime();
        return new FailingCountingSource(
                sourceId,
                markerDir.getAbsolutePath(),
                0,
                value,
                recordsBeforeFailurePerKey,
                recordsAfterFailurePerKey,
                true);
    }

    @Override
    public Boundedness getBoundedness() {
        // CONTINUOUS_UNBOUNDED allows the source to idle after emitting records
        // This is needed for checkpoint tests where we want checkpoints to complete
        // after all records are emitted but before the job terminates
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<FailingCountingSplit, FailingEnumeratorState> createEnumerator(
            SplitEnumeratorContext<FailingCountingSplit> context) {
        return new FailingCountingSplitEnumerator(
                context, recordsBeforeFailure, recordsAfterFailure, multiKey, false);
    }

    @Override
    public SplitEnumerator<FailingCountingSplit, FailingEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<FailingCountingSplit> context,
            FailingEnumeratorState checkpoint) {
        return new FailingCountingSplitEnumerator(
                context,
                checkpoint.getRecordsBeforeFailure(),
                checkpoint.getRecordsAfterFailure(),
                checkpoint.isMultiKey(),
                checkpoint.isSplitAssigned());
    }

    @Override
    public SimpleVersionedSerializer<FailingCountingSplit> getSplitSerializer() {
        return new FailingCountingSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<FailingEnumeratorState> getEnumeratorCheckpointSerializer() {
        return new FailingEnumeratorStateSerializer();
    }

    @Override
    public SourceReader<RowData, FailingCountingSplit> createReader(
            SourceReaderContext readerContext) {
        return new FailingCountingReader(
                sourceId,
                markerDirPath,
                key,
                value,
                multiKey,
                recordsBeforeFailure,
                recordsAfterFailure);
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new RowType.RowField("key", DataTypes.BIGINT().getLogicalType()),
                                new RowType.RowField(
                                        "value", DataTypes.BIGINT().getLogicalType())));
        return InternalTypeInfo.of(rowType);
    }

    // ==================== FailingCountingSplit ====================

    /**
     * Split state for FailingCountingSource that tracks failure status.
     *
     * <p>This split contains:
     *
     * <ul>
     *   <li>{@code splitId} - unique identifier for the split
     *   <li>{@code emittedRecords} - number of records emitted so far
     *   <li>{@code hasFailed} - whether the failure has already been triggered (one-shot flag)
     * </ul>
     */
    public static class FailingCountingSplit implements SourceSplit, Serializable {
        private static final long serialVersionUID = 1L;

        private final int splitId;
        private final int emittedRecords;
        private final boolean hasFailed;

        public FailingCountingSplit(int splitId, int emittedRecords, boolean hasFailed) {
            this.splitId = splitId;
            this.emittedRecords = emittedRecords;
            this.hasFailed = hasFailed;
        }

        @Override
        public String splitId() {
            return String.valueOf(splitId);
        }

        public int getSplitIdInt() {
            return splitId;
        }

        public int getEmittedRecords() {
            return emittedRecords;
        }

        public boolean hasFailed() {
            return hasFailed;
        }

        @Override
        public String toString() {
            return "FailingCountingSplit{"
                    + "splitId="
                    + splitId
                    + ", emittedRecords="
                    + emittedRecords
                    + ", hasFailed="
                    + hasFailed
                    + '}';
        }
    }

    // ==================== FailingEnumeratorState ====================

    /**
     * Enumerator state for FailingCountingSource.
     *
     * <p>This state contains configuration needed to restore the enumerator:
     *
     * <ul>
     *   <li>{@code recordsBeforeFailure} - records to emit before failure
     *   <li>{@code recordsAfterFailure} - records to emit after recovery
     *   <li>{@code multiKey} - whether multi-key mode is enabled
     *   <li>{@code splitAssigned} - whether a split has been assigned
     * </ul>
     */
    public static class FailingEnumeratorState implements Serializable {
        private static final long serialVersionUID = 1L;

        private final int recordsBeforeFailure;
        private final int recordsAfterFailure;
        private final boolean multiKey;
        private final boolean splitAssigned;

        public FailingEnumeratorState(
                int recordsBeforeFailure,
                int recordsAfterFailure,
                boolean multiKey,
                boolean splitAssigned) {
            this.recordsBeforeFailure = recordsBeforeFailure;
            this.recordsAfterFailure = recordsAfterFailure;
            this.multiKey = multiKey;
            this.splitAssigned = splitAssigned;
        }

        public int getRecordsBeforeFailure() {
            return recordsBeforeFailure;
        }

        public int getRecordsAfterFailure() {
            return recordsAfterFailure;
        }

        public boolean isMultiKey() {
            return multiKey;
        }

        public boolean isSplitAssigned() {
            return splitAssigned;
        }
    }

    // ==================== FailingCountingSplitEnumerator ====================

    /**
     * Split enumerator for FailingCountingSource.
     *
     * <p>This enumerator assigns a single split to the first reader (subtask 0).
     */
    private static class FailingCountingSplitEnumerator
            implements SplitEnumerator<FailingCountingSplit, FailingEnumeratorState> {

        private final SplitEnumeratorContext<FailingCountingSplit> context;
        private final int recordsBeforeFailure;
        private final int recordsAfterFailure;
        private final boolean multiKey;
        private boolean splitAssigned;

        FailingCountingSplitEnumerator(
                SplitEnumeratorContext<FailingCountingSplit> context,
                int recordsBeforeFailure,
                int recordsAfterFailure,
                boolean multiKey,
                boolean splitAssigned) {
            this.context = context;
            this.recordsBeforeFailure = recordsBeforeFailure;
            this.recordsAfterFailure = recordsAfterFailure;
            this.multiKey = multiKey;
            this.splitAssigned = splitAssigned;
        }

        @Override
        public void start() {}

        @Override
        public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
            // Not used - we assign splits proactively
        }

        @Override
        public void addSplitsBack(List<FailingCountingSplit> splits, int subtaskId) {
            // Re-assign splits if a reader fails
            if (!splits.isEmpty()) {
                splitAssigned = false;
            }
        }

        @Override
        public void addReader(int subtaskId) {
            if (!splitAssigned && subtaskId == 0) {
                // Assign a fresh split - failure state is tracked in static map
                context.assignSplit(new FailingCountingSplit(0, 0, false), subtaskId);
                splitAssigned = true;
            }
        }

        @Override
        public FailingEnumeratorState snapshotState(long checkpointId) {
            return new FailingEnumeratorState(
                    recordsBeforeFailure, recordsAfterFailure, multiKey, splitAssigned);
        }

        @Override
        public void close() {}
    }

    // ==================== FailingCountingReader ====================

    /**
     * Reader that emits records and triggers failure at the configured point.
     *
     * <p>The reader tracks:
     *
     * <ul>
     *   <li>{@code totalEmitted} - total records emitted across all phases
     *   <li>{@code hasFailed} - whether the failure has been triggered (one-shot)
     * </ul>
     *
     * <p>Uses file-based markers to track failure across ClassLoader restarts.
     */
    private static class FailingCountingReader
            implements SourceReader<RowData, FailingCountingSplit> {

        private final String sourceId;
        private final String markerDirPath;
        private final long singleKey;
        private final long value;
        private final boolean multiKey;
        private final int recordsBeforeFailure;
        private final int recordsAfterFailure;
        private final Queue<FailingCountingSplit> splits = new ArrayDeque<>();
        private CompletableFuture<Void> availableFuture = new CompletableFuture<>();

        // State tracked across checkpoints
        private int totalEmitted = 0;
        private boolean hasFailed = false;
        private int keyIndex = 0;

        FailingCountingReader(
                String sourceId,
                String markerDirPath,
                long singleKey,
                long value,
                boolean multiKey,
                int recordsBeforeFailure,
                int recordsAfterFailure) {
            this.sourceId = sourceId;
            this.markerDirPath = markerDirPath;
            this.singleKey = singleKey;
            this.value = value;
            this.multiKey = multiKey;
            this.recordsBeforeFailure = recordsBeforeFailure;
            this.recordsAfterFailure = recordsAfterFailure;
        }

        private boolean hasFailedGlobally() {
            return new java.io.File(markerDirPath, sourceId).exists();
        }

        private void markFailedGlobally() {
            java.io.File markerDir = new java.io.File(markerDirPath);
            markerDir.mkdirs();
            try {
                new java.io.File(markerDir, sourceId).createNewFile();
            } catch (Exception e) {
                // Ignore
            }
        }

        @Override
        public void start() {}

        @Override
        public InputStatus pollNext(ReaderOutput<RowData> output) throws Exception {
            if (splits.isEmpty()) {
                return InputStatus.NOTHING_AVAILABLE;
            }

            // Calculate total records based on mode
            int totalRecordsBeforeFailure =
                    multiKey ? recordsBeforeFailure * 3 : recordsBeforeFailure;
            int totalRecordsAfterFailure = multiKey ? recordsAfterFailure * 3 : recordsAfterFailure;
            int totalRecords = totalRecordsBeforeFailure + totalRecordsAfterFailure;

            // Check if we should fail (one-shot)
            // Use file-based marker to track failure across restarts
            if (!hasFailed && !hasFailedGlobally() && totalEmitted >= totalRecordsBeforeFailure) {
                hasFailed = true;
                markFailedGlobally();
                throw new RuntimeException("Injected failure for testing checkpoint recovery");
            }

            // Check if we've emitted all records
            if (totalEmitted >= totalRecords) {
                // All records emitted, idle (don't return END_OF_INPUT)
                // This allows checkpoints/savepoints to be taken
                return InputStatus.NOTHING_AVAILABLE;
            }

            // Emit a record
            GenericRowData row = new GenericRowData(2);
            if (multiKey) {
                row.setField(0, (long) (keyIndex + 1));
                keyIndex = (keyIndex + 1) % 3;
            } else {
                row.setField(0, singleKey);
            }
            row.setField(1, value);
            output.collect(row);
            totalEmitted++;

            // Small delay to avoid overwhelming the system
            Thread.sleep(10);

            return InputStatus.MORE_AVAILABLE;
        }

        @Override
        public List<FailingCountingSplit> snapshotState(long checkpointId) {
            // Checkpoint the current state including hasFailed flag
            return Collections.singletonList(new FailingCountingSplit(0, totalEmitted, hasFailed));
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            return availableFuture;
        }

        @Override
        public void addSplits(List<FailingCountingSplit> newSplits) {
            // Restore state from split (assigned by Enumerator or restored from checkpoint)
            for (FailingCountingSplit split : newSplits) {
                this.totalEmitted = split.getEmittedRecords();
                this.hasFailed = split.hasFailed();
                splits.add(split);
            }
            availableFuture.complete(null);
            availableFuture = new CompletableFuture<>();
        }

        @Override
        public void notifyNoMoreSplits() {
            // No action needed
        }

        @Override
        public void close() {}
    }

    // ==================== Serializers ====================

    /**
     * Serializer for FailingCountingSplit.
     *
     * <p>Serialization format: splitId (4 bytes) + emittedRecords (4 bytes) + hasFailed (1 byte)
     */
    private static class FailingCountingSplitSerializer
            implements SimpleVersionedSerializer<FailingCountingSplit> {
        private static final int VERSION = 1;

        @Override
        public int getVersion() {
            return VERSION;
        }

        @Override
        public byte[] serialize(FailingCountingSplit split) {
            ByteBuffer buffer = ByteBuffer.allocate(9);
            buffer.putInt(split.getSplitIdInt());
            buffer.putInt(split.getEmittedRecords());
            buffer.put((byte) (split.hasFailed() ? 1 : 0));
            return buffer.array();
        }

        @Override
        public FailingCountingSplit deserialize(int version, byte[] serialized) {
            ByteBuffer buffer = ByteBuffer.wrap(serialized);
            int splitId = buffer.getInt();
            int emittedRecords = buffer.getInt();
            boolean hasFailed = buffer.get() == 1;
            return new FailingCountingSplit(splitId, emittedRecords, hasFailed);
        }
    }

    /**
     * Serializer for FailingEnumeratorState.
     *
     * <p>Serialization format: recordsBeforeFailure (4 bytes) + recordsAfterFailure (4 bytes) +
     * multiKey (1 byte) + splitAssigned (1 byte)
     */
    private static class FailingEnumeratorStateSerializer
            implements SimpleVersionedSerializer<FailingEnumeratorState> {
        private static final int VERSION = 1;

        @Override
        public int getVersion() {
            return VERSION;
        }

        @Override
        public byte[] serialize(FailingEnumeratorState state) {
            ByteBuffer buffer = ByteBuffer.allocate(10);
            buffer.putInt(state.getRecordsBeforeFailure());
            buffer.putInt(state.getRecordsAfterFailure());
            buffer.put((byte) (state.isMultiKey() ? 1 : 0));
            buffer.put((byte) (state.isSplitAssigned() ? 1 : 0));
            return buffer.array();
        }

        @Override
        public FailingEnumeratorState deserialize(int version, byte[] serialized) {
            ByteBuffer buffer = ByteBuffer.wrap(serialized);
            int recordsBeforeFailure = buffer.getInt();
            int recordsAfterFailure = buffer.getInt();
            boolean multiKey = buffer.get() == 1;
            boolean splitAssigned = buffer.get() == 1;
            return new FailingEnumeratorState(
                    recordsBeforeFailure, recordsAfterFailure, multiKey, splitAssigned);
        }
    }
}
