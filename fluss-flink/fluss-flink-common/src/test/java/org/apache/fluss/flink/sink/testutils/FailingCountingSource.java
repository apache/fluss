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

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** A deterministic single-key source for checkpoint failover tests. */
public class FailingCountingSource
        implements Source<
                        RowData,
                        FailingCountingSource.FailingCountingSplit,
                        FailingCountingSource.FailingEnumeratorState>,
                ResultTypeQueryable<RowData> {

    private static final long serialVersionUID = 2L;
    private static final long MARKER_POLL_INTERVAL_MILLIS = 10L;
    private static final String DIRTY_RELEASE_MARKER_SUFFIX = ".release-dirty";
    private static final String FAILURE_TRIGGER_MARKER_SUFFIX = ".trigger-failure";
    private static final String FAILURE_MARKER_SUFFIX = ".failed";
    private static final String RESTART_MARKER_SUFFIX = ".restarted";
    private static final String RECOVERY_COMPLETE_MARKER_SUFFIX = ".recovery-complete";
    private static final CompletableFuture<Void> AVAILABLE =
            CompletableFuture.completedFuture(null);

    private final String sourceId;
    private final String markerDirPath;
    private final long key;
    private final long committedValue;
    private final int committedRecords;
    private final long dirtyValue;
    private final int dirtyRecords;
    private final long recoveryValue;
    private final int recoveryRecords;

    private FailingCountingSource(
            String sourceId,
            String markerDirPath,
            long key,
            long committedValue,
            int committedRecords,
            long dirtyValue,
            int dirtyRecords,
            long recoveryValue,
            int recoveryRecords) {
        if (committedRecords < 0 || dirtyRecords < 0 || recoveryRecords <= 0) {
            throw new IllegalArgumentException(
                    "Committed and dirty record counts must not be negative, and recovery records must be positive");
        }
        this.sourceId = sourceId;
        this.markerDirPath = markerDirPath;
        this.key = key;
        this.committedValue = committedValue;
        this.committedRecords = committedRecords;
        this.dirtyValue = dirtyValue;
        this.dirtyRecords = dirtyRecords;
        this.recoveryValue = recoveryValue;
        this.recoveryRecords = recoveryRecords;
    }

    /** Creates a source with independently controlled committed, dirty, and recovery phases. */
    public static FailingCountingSource coordinatedSingleKey(
            File markerDir,
            long key,
            long committedValue,
            int committedRecords,
            long dirtyValue,
            int dirtyRecords,
            long recoveryValue,
            int recoveryRecords) {
        return new FailingCountingSource(
                "coordinated-single-" + key + "-" + System.nanoTime(),
                markerDir.getAbsolutePath(),
                key,
                committedValue,
                committedRecords,
                dirtyValue,
                dirtyRecords,
                recoveryValue,
                recoveryRecords);
    }

    /** Releases the source's dirty suffix. */
    public void releaseDirtyEmission() throws IOException {
        createMarker(DIRTY_RELEASE_MARKER_SUFFIX);
    }

    /** Triggers the source's one-shot failure. */
    public void triggerFailure() throws IOException {
        createMarker(FAILURE_TRIGGER_MARKER_SUFFIX);
    }

    /** Returns whether the source has injected its failure. */
    public boolean hasFailed() {
        return markerExists(FAILURE_MARKER_SUFFIX);
    }

    /** Returns whether the failed reader has restarted. */
    public boolean hasRestarted() {
        return markerExists(RESTART_MARKER_SUFFIX);
    }

    /** Returns whether all post-restart records have been emitted. */
    public boolean hasRecoveryEmissionCompleted() {
        return markerExists(RECOVERY_COMPLETE_MARKER_SUFFIX);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<FailingCountingSplit, FailingEnumeratorState> createEnumerator(
            SplitEnumeratorContext<FailingCountingSplit> context) {
        return new FailingCountingSplitEnumerator(
                context, new FailingEnumeratorState(new FailingCountingSplit(0, 0)));
    }

    @Override
    public SplitEnumerator<FailingCountingSplit, FailingEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<FailingCountingSplit> context,
            FailingEnumeratorState checkpoint) {
        return new FailingCountingSplitEnumerator(context, checkpoint);
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
                committedValue,
                committedRecords,
                dirtyValue,
                dirtyRecords,
                recoveryValue,
                recoveryRecords);
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

    private boolean markerExists(String suffix) {
        return new File(markerDirPath, sourceId + suffix).isFile();
    }

    private void createMarker(String suffix) throws IOException {
        createMarker(markerDirPath, sourceId + suffix);
    }

    private static void createMarker(String markerDirPath, String markerName) throws IOException {
        File markerDir = new File(markerDirPath);
        if (!markerDir.isDirectory() && !markerDir.mkdirs() && !markerDir.isDirectory()) {
            throw new IOException("Failed to create marker directory " + markerDir);
        }
        File marker = new File(markerDir, markerName);
        if (!marker.createNewFile() && !marker.isFile()) {
            throw new IOException("Failed to create marker " + marker);
        }
    }

    /** Checkpointed progress for the source's single split. */
    public static class FailingCountingSplit implements SourceSplit, Serializable {
        private static final long serialVersionUID = 2L;

        private final int splitId;
        private final int emittedRecords;

        private FailingCountingSplit(int splitId, int emittedRecords) {
            this.splitId = splitId;
            this.emittedRecords = emittedRecords;
        }

        @Override
        public String splitId() {
            return String.valueOf(splitId);
        }
    }

    /** Checkpointed assignment for the source's single split. */
    public static class FailingEnumeratorState implements Serializable {
        private static final long serialVersionUID = 2L;

        @Nullable private final FailingCountingSplit pendingSplit;

        private FailingEnumeratorState(@Nullable FailingCountingSplit pendingSplit) {
            this.pendingSplit = pendingSplit;
        }
    }

    private static class FailingCountingSplitEnumerator
            implements SplitEnumerator<FailingCountingSplit, FailingEnumeratorState> {

        private final SplitEnumeratorContext<FailingCountingSplit> context;
        @Nullable private FailingCountingSplit pendingSplit;

        private FailingCountingSplitEnumerator(
                SplitEnumeratorContext<FailingCountingSplit> context,
                FailingEnumeratorState checkpoint) {
            this.context = context;
            this.pendingSplit = checkpoint.pendingSplit;
        }

        @Override
        public void start() {}

        @Override
        public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {}

        @Override
        public void addSplitsBack(List<FailingCountingSplit> splits, int subtaskId) {
            if (splits.size() != 1) {
                throw new IllegalArgumentException(
                        "Expected one returned split, but got " + splits.size());
            }
            pendingSplit = splits.get(0);
            assignPendingSplit(subtaskId);
        }

        @Override
        public void addReader(int subtaskId) {
            assignPendingSplit(subtaskId);
        }

        private void assignPendingSplit(int subtaskId) {
            if (subtaskId == 0
                    && pendingSplit != null
                    && context.registeredReaders().containsKey(subtaskId)) {
                context.assignSplit(pendingSplit, subtaskId);
                pendingSplit = null;
            }
        }

        @Override
        public FailingEnumeratorState snapshotState(long checkpointId) {
            return new FailingEnumeratorState(pendingSplit);
        }

        @Override
        public void close() {}
    }

    private static class FailingCountingReader
            implements SourceReader<RowData, FailingCountingSplit> {

        private final String sourceId;
        private final String markerDirPath;
        private final long key;
        private final long committedValue;
        private final int committedRecords;
        private final long dirtyValue;
        private final int dirtyRecords;
        private final long recoveryValue;
        private final int recoveryRecords;
        private final CompletableFuture<Void> splitAvailability = new CompletableFuture<>();

        @Nullable private FailingCountingSplit activeSplit;
        private int emittedRecords;

        private FailingCountingReader(
                String sourceId,
                String markerDirPath,
                long key,
                long committedValue,
                int committedRecords,
                long dirtyValue,
                int dirtyRecords,
                long recoveryValue,
                int recoveryRecords) {
            this.sourceId = sourceId;
            this.markerDirPath = markerDirPath;
            this.key = key;
            this.committedValue = committedValue;
            this.committedRecords = committedRecords;
            this.dirtyValue = dirtyValue;
            this.dirtyRecords = dirtyRecords;
            this.recoveryValue = recoveryValue;
            this.recoveryRecords = recoveryRecords;
        }

        @Override
        public void start() {
            if (markerExists(FAILURE_MARKER_SUFFIX)) {
                createMarker(RESTART_MARKER_SUFFIX);
            }
        }

        @Override
        public InputStatus pollNext(ReaderOutput<RowData> output) throws Exception {
            if (activeSplit == null) {
                return InputStatus.NOTHING_AVAILABLE;
            }

            int dirtyEnd = committedRecords + dirtyRecords;
            int totalRecords = dirtyEnd + recoveryRecords;
            long value;
            if (emittedRecords < committedRecords) {
                value = committedValue;
            } else if (emittedRecords < dirtyEnd) {
                if (!markerExists(DIRTY_RELEASE_MARKER_SUFFIX)) {
                    return waitForMarker();
                }
                value = dirtyValue;
            } else if (!markerExists(FAILURE_MARKER_SUFFIX)) {
                if (!markerExists(FAILURE_TRIGGER_MARKER_SUFFIX)) {
                    return waitForMarker();
                }
                createMarker(FAILURE_MARKER_SUFFIX);
                throw new RuntimeException("Injected failure for testing checkpoint recovery");
            } else if (emittedRecords < totalRecords) {
                value = recoveryValue;
            } else {
                return waitForMarker();
            }

            GenericRowData row = new GenericRowData(2);
            row.setField(0, key);
            row.setField(1, value);
            output.collect(row);
            emittedRecords++;
            if (markerExists(FAILURE_MARKER_SUFFIX) && emittedRecords == totalRecords) {
                createMarker(RECOVERY_COMPLETE_MARKER_SUFFIX);
            }
            return InputStatus.MORE_AVAILABLE;
        }

        private InputStatus waitForMarker() throws InterruptedException {
            Thread.sleep(MARKER_POLL_INTERVAL_MILLIS);
            return InputStatus.NOTHING_AVAILABLE;
        }

        @Override
        public List<FailingCountingSplit> snapshotState(long checkpointId) {
            if (activeSplit == null) {
                return Collections.emptyList();
            }
            return Collections.singletonList(
                    new FailingCountingSplit(activeSplit.splitId, emittedRecords));
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            return activeSplit == null ? splitAvailability : AVAILABLE;
        }

        @Override
        public void addSplits(List<FailingCountingSplit> splits) {
            if (activeSplit != null || splits.size() != 1) {
                throw new IllegalArgumentException("Expected exactly one assigned split");
            }
            activeSplit = splits.get(0);
            emittedRecords = activeSplit.emittedRecords;
            splitAvailability.complete(null);
        }

        @Override
        public void notifyNoMoreSplits() {}

        @Override
        public void close() {}

        private boolean markerExists(String suffix) {
            return new File(markerDirPath, sourceId + suffix).isFile();
        }

        private void createMarker(String suffix) {
            try {
                FailingCountingSource.createMarker(markerDirPath, sourceId + suffix);
            } catch (IOException e) {
                throw new RuntimeException("Failed to create source coordination marker", e);
            }
        }
    }

    private static class FailingCountingSplitSerializer
            implements SimpleVersionedSerializer<FailingCountingSplit> {
        private static final int VERSION = 3;
        private static final int SERIALIZED_LENGTH = 8;

        @Override
        public int getVersion() {
            return VERSION;
        }

        @Override
        public byte[] serialize(FailingCountingSplit split) {
            ByteBuffer buffer = ByteBuffer.allocate(SERIALIZED_LENGTH);
            buffer.putInt(split.splitId);
            buffer.putInt(split.emittedRecords);
            return buffer.array();
        }

        @Override
        public FailingCountingSplit deserialize(int version, byte[] serialized) throws IOException {
            if (version != VERSION || serialized.length != SERIALIZED_LENGTH) {
                throw new IOException("Unsupported split state");
            }
            ByteBuffer buffer = ByteBuffer.wrap(serialized);
            return new FailingCountingSplit(buffer.getInt(), buffer.getInt());
        }
    }

    private static class FailingEnumeratorStateSerializer
            implements SimpleVersionedSerializer<FailingEnumeratorState> {
        private static final int VERSION = 2;

        @Override
        public int getVersion() {
            return VERSION;
        }

        @Override
        public byte[] serialize(FailingEnumeratorState state) {
            if (state.pendingSplit == null) {
                return new byte[] {1};
            }
            ByteBuffer buffer = ByteBuffer.allocate(9);
            buffer.put((byte) 0);
            buffer.putInt(state.pendingSplit.splitId);
            buffer.putInt(state.pendingSplit.emittedRecords);
            return buffer.array();
        }

        @Override
        public FailingEnumeratorState deserialize(int version, byte[] serialized)
                throws IOException {
            if (version != VERSION || (serialized.length != 1 && serialized.length != 9)) {
                throw new IOException("Unsupported enumerator state");
            }
            ByteBuffer buffer = ByteBuffer.wrap(serialized);
            byte assigned = buffer.get();
            if (assigned == 1 && serialized.length == 1) {
                return new FailingEnumeratorState(null);
            }
            if (assigned == 0 && serialized.length == 9) {
                return new FailingEnumeratorState(
                        new FailingCountingSplit(buffer.getInt(), buffer.getInt()));
            }
            throw new IOException("Invalid enumerator state");
        }
    }
}
