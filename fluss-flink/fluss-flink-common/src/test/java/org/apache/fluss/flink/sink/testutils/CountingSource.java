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
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

/**
 * A FLIP-27 based counting source for testing undo recovery.
 *
 * <p>This source emits a specified number of records then idles (doesn't terminate), allowing
 * savepoints to be taken. The job must be explicitly cancelled or stopped with savepoint.
 *
 * <p>This implementation is compatible with Flink 1.18+ and Flink 2.x, replacing the deprecated
 * SourceFunction-based implementation.
 *
 * <p><b>Important:</b> When restored from a savepoint, this source will emit a fresh set of records
 * (ignoring the checkpoint state). This is intentional for undo recovery testing, where each phase
 * should emit its own records regardless of previous phases.
 */
public class CountingSource
        implements Source<RowData, CountingSource.CountingSplit, CountingSource.EnumeratorState>,
                ResultTypeQueryable<RowData> {

    private static final long serialVersionUID = 1L;

    private final long key;
    private final long value;
    private final int maxRecords;
    private final boolean multiKey;

    private CountingSource(long key, long value, int maxRecords, boolean multiKey) {
        this.key = key;
        this.value = value;
        this.maxRecords = maxRecords;
        this.multiKey = multiKey;
    }

    /** Creates a single-key source that emits records for the specified key. */
    public static CountingSource singleKey(long key, long value, int maxRecords) {
        return new CountingSource(key, value, maxRecords, false);
    }

    /** Creates a multi-key source that emits records for keys 1, 2, 3 in round-robin. */
    public static CountingSource multiKey(long value, int maxRecordsPerKey) {
        return new CountingSource(0, value, maxRecordsPerKey, true);
    }

    @Override
    public Boundedness getBoundedness() {
        // CONTINUOUS_UNBOUNDED allows the source to idle after emitting records
        // This is needed for savepoint tests where we want to take savepoints
        // after all records are emitted but before the job terminates
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<CountingSplit, EnumeratorState> createEnumerator(
            SplitEnumeratorContext<CountingSplit> context) {
        return new CountingEnumerator(context, maxRecords, multiKey);
    }

    @Override
    public SplitEnumerator<CountingSplit, EnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<CountingSplit> context, EnumeratorState checkpoint) {
        // When restoring from savepoint, we want to emit a fresh set of records
        // using the current source's maxRecords (not the checkpoint's maxRecords)
        // So we reset splitAssigned to false to allow a new split to be assigned
        return new CountingEnumerator(context, maxRecords, multiKey);
    }

    @Override
    public SimpleVersionedSerializer<CountingSplit> getSplitSerializer() {
        return new CountingSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<EnumeratorState> getEnumeratorCheckpointSerializer() {
        return new EnumeratorStateSerializer();
    }

    @Override
    public SourceReader<RowData, CountingSplit> createReader(SourceReaderContext readerContext) {
        return new CountingReader(key, value, multiKey, maxRecords);
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return TypeInformation.of(RowData.class);
    }

    // ==================== Split ====================

    /** A split representing a range of records to emit. */
    public static class CountingSplit implements SourceSplit, Serializable {
        private static final long serialVersionUID = 1L;

        private final int splitId;
        private final int maxRecords;
        private int emittedRecords;

        public CountingSplit(int splitId, int maxRecords) {
            this(splitId, maxRecords, 0);
        }

        public CountingSplit(int splitId, int maxRecords, int emittedRecords) {
            this.splitId = splitId;
            this.maxRecords = maxRecords;
            this.emittedRecords = emittedRecords;
        }

        @Override
        public String splitId() {
            return String.valueOf(splitId);
        }

        public int getMaxRecords() {
            return maxRecords;
        }

        public int getEmittedRecords() {
            return emittedRecords;
        }

        public void incrementEmitted() {
            emittedRecords++;
        }

        public boolean isFinished() {
            return emittedRecords >= maxRecords;
        }
    }

    // ==================== Enumerator ====================

    /** Enumerator state for checkpointing. */
    public static class EnumeratorState implements Serializable {
        private static final long serialVersionUID = 1L;

        private final int maxRecords;
        private final boolean multiKey;
        private final boolean splitAssigned;

        public EnumeratorState(int maxRecords, boolean multiKey, boolean splitAssigned) {
            this.maxRecords = maxRecords;
            this.multiKey = multiKey;
            this.splitAssigned = splitAssigned;
        }
    }

    /** Simple enumerator that assigns a single split to the first reader. */
    private static class CountingEnumerator
            implements SplitEnumerator<CountingSplit, EnumeratorState> {

        private final SplitEnumeratorContext<CountingSplit> context;
        private final int maxRecords;
        private final boolean multiKey;
        private boolean splitAssigned;

        CountingEnumerator(
                SplitEnumeratorContext<CountingSplit> context, int maxRecords, boolean multiKey) {
            this.context = context;
            this.maxRecords = maxRecords;
            this.multiKey = multiKey;
            this.splitAssigned = false;
        }

        CountingEnumerator(
                SplitEnumeratorContext<CountingSplit> context, EnumeratorState checkpoint) {
            this.context = context;
            this.maxRecords = checkpoint.maxRecords;
            this.multiKey = checkpoint.multiKey;
            this.splitAssigned = checkpoint.splitAssigned;
        }

        @Override
        public void start() {}

        @Override
        public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
            // Not used - we assign splits proactively
        }

        @Override
        public void addSplitsBack(List<CountingSplit> splits, int subtaskId) {
            // Re-assign splits if a reader fails
            if (!splits.isEmpty()) {
                splitAssigned = false;
            }
        }

        @Override
        public void addReader(int subtaskId) {
            if (!splitAssigned && subtaskId == 0) {
                // For multi-key, total records = maxRecords * 3 (keys 1, 2, 3)
                int totalRecords = multiKey ? maxRecords * 3 : maxRecords;
                context.assignSplit(new CountingSplit(0, totalRecords), subtaskId);
                splitAssigned = true;
            }
        }

        @Override
        public EnumeratorState snapshotState(long checkpointId) {
            return new EnumeratorState(maxRecords, multiKey, splitAssigned);
        }

        @Override
        public void close() {}
    }

    // ==================== Reader ====================

    /** Reader that emits records then idles. */
    private static class CountingReader implements SourceReader<RowData, CountingSplit> {

        private final long singleKey;
        private final long value;
        private final boolean multiKey;
        private final int maxRecords;
        private final Queue<CountingSplit> splits = new ArrayDeque<>();
        private CompletableFuture<Void> availableFuture = new CompletableFuture<>();
        private int keyIndex = 0;

        CountingReader(long singleKey, long value, boolean multiKey, int maxRecords) {
            this.singleKey = singleKey;
            this.value = value;
            this.multiKey = multiKey;
            this.maxRecords = maxRecords;
        }

        @Override
        public void start() {}

        @Override
        public InputStatus pollNext(ReaderOutput<RowData> output) throws Exception {
            CountingSplit split = splits.peek();
            if (split == null) {
                return InputStatus.NOTHING_AVAILABLE;
            }

            if (split.isFinished()) {
                // All records emitted, idle (don't return END_OF_INPUT)
                // This allows savepoints to be taken
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
            split.incrementEmitted();

            // Small delay to avoid overwhelming the system
            Thread.sleep(10);

            return InputStatus.MORE_AVAILABLE;
        }

        @Override
        public List<CountingSplit> snapshotState(long checkpointId) {
            return new ArrayList<>(splits);
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            return availableFuture;
        }

        @Override
        public void addSplits(List<CountingSplit> newSplits) {
            // When restoring from savepoint, use the reader's maxRecords (from the new source)
            // instead of the checkpoint's maxRecords, and reset emittedRecords
            int totalRecords = multiKey ? maxRecords * 3 : maxRecords;
            for (CountingSplit split : newSplits) {
                // Create a fresh split with the reader's maxRecords and reset emittedRecords
                splits.add(new CountingSplit(Integer.parseInt(split.splitId()), totalRecords, 0));
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

    /** Serializer for CountingSplit. */
    private static class CountingSplitSerializer
            implements SimpleVersionedSerializer<CountingSplit> {
        private static final int VERSION = 1;

        @Override
        public int getVersion() {
            return VERSION;
        }

        @Override
        public byte[] serialize(CountingSplit split) {
            ByteBuffer buffer = ByteBuffer.allocate(12);
            buffer.putInt(split.splitId);
            buffer.putInt(split.maxRecords);
            buffer.putInt(split.emittedRecords);
            return buffer.array();
        }

        @Override
        public CountingSplit deserialize(int version, byte[] serialized) {
            ByteBuffer buffer = ByteBuffer.wrap(serialized);
            int splitId = buffer.getInt();
            int maxRecords = buffer.getInt();
            int emittedRecords = buffer.getInt();
            return new CountingSplit(splitId, maxRecords, emittedRecords);
        }
    }

    /** Serializer for EnumeratorState. */
    private static class EnumeratorStateSerializer
            implements SimpleVersionedSerializer<EnumeratorState> {
        private static final int VERSION = 1;

        @Override
        public int getVersion() {
            return VERSION;
        }

        @Override
        public byte[] serialize(EnumeratorState state) {
            ByteBuffer buffer = ByteBuffer.allocate(6);
            buffer.putInt(state.maxRecords);
            buffer.put((byte) (state.multiKey ? 1 : 0));
            buffer.put((byte) (state.splitAssigned ? 1 : 0));
            return buffer.array();
        }

        @Override
        public EnumeratorState deserialize(int version, byte[] serialized) {
            ByteBuffer buffer = ByteBuffer.wrap(serialized);
            int maxRecords = buffer.getInt();
            boolean multiKey = buffer.get() == 1;
            boolean splitAssigned = buffer.get() == 1;
            return new EnumeratorState(maxRecords, multiKey, splitAssigned);
        }
    }
}
