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

package org.apache.fluss.flink.source;

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
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.utils.Preconditions.checkState;

/** A bounded singleton-split source that loads metadata once for each execution. */
class SingleSplitMetadataSource
        implements Source<
                        RowData,
                        SingleSplitMetadataSource.MetadataSplit,
                        SingleSplitMetadataSource.MetadataEnumeratorState>,
                ResultTypeQueryable<RowData> {

    private static final long serialVersionUID = 1L;

    private final MetadataLoader metadataLoader;
    private final TypeInformation<RowData> producedType;

    SingleSplitMetadataSource(
            MetadataLoader metadataLoader, TypeInformation<RowData> producedType) {
        this.metadataLoader = metadataLoader;
        this.producedType = producedType;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SplitEnumerator<MetadataSplit, MetadataEnumeratorState> createEnumerator(
            SplitEnumeratorContext<MetadataSplit> enumContext) {
        return new MetadataSplitEnumerator(enumContext, new MetadataSplit(0));
    }

    @Override
    public SplitEnumerator<MetadataSplit, MetadataEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<MetadataSplit> enumContext, MetadataEnumeratorState checkpoint) {
        return new MetadataSplitEnumerator(enumContext, checkpoint.pendingSplit);
    }

    @Override
    public SimpleVersionedSerializer<MetadataSplit> getSplitSerializer() {
        return MetadataSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<MetadataEnumeratorState> getEnumeratorCheckpointSerializer() {
        return MetadataEnumeratorStateSerializer.INSTANCE;
    }

    @Override
    public SourceReader<RowData, MetadataSplit> createReader(SourceReaderContext readerContext) {
        return new MetadataSourceReader(metadataLoader);
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedType;
    }

    interface MetadataLoader extends Serializable {
        List<RowData> load();
    }

    static final class MetadataSplit implements SourceSplit, Serializable {

        private static final long serialVersionUID = 1L;
        private static final String SPLIT_ID = "metadata";

        private int nextRow;

        MetadataSplit(int nextRow) {
            this.nextRow = nextRow;
        }

        @Override
        public String splitId() {
            return SPLIT_ID;
        }

        private void advance() {
            nextRow++;
        }
    }

    static final class MetadataEnumeratorState implements Serializable {

        private static final long serialVersionUID = 1L;

        @Nullable private final MetadataSplit pendingSplit;

        private MetadataEnumeratorState(@Nullable MetadataSplit pendingSplit) {
            this.pendingSplit = pendingSplit;
        }
    }

    private static final class MetadataSplitEnumerator
            implements SplitEnumerator<MetadataSplit, MetadataEnumeratorState> {

        private final SplitEnumeratorContext<MetadataSplit> enumContext;
        @Nullable private MetadataSplit pendingSplit;

        private MetadataSplitEnumerator(
                SplitEnumeratorContext<MetadataSplit> enumContext,
                @Nullable MetadataSplit pendingSplit) {
            this.enumContext = enumContext;
            this.pendingSplit = pendingSplit;
        }

        @Override
        public void start() {}

        @Override
        public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
            assignSplitOrSignalFinished(subtaskId);
        }

        @Override
        public void addSplitsBack(List<MetadataSplit> splits, int subtaskId) {
            checkState(pendingSplit == null, "A metadata split is already pending.");
            checkState(splits.size() == 1, "Expected one metadata split to be returned.");
            pendingSplit = splits.get(0);
        }

        @Override
        public void addReader(int subtaskId) {
            assignSplitOrSignalFinished(subtaskId);
        }

        @Override
        public MetadataEnumeratorState snapshotState(long checkpointId) {
            return new MetadataEnumeratorState(pendingSplit);
        }

        @Override
        public void close() {}

        private void assignSplitOrSignalFinished(int subtaskId) {
            if (pendingSplit != null) {
                enumContext.assignSplit(pendingSplit, subtaskId);
                pendingSplit = null;
            }
            enumContext.signalNoMoreSplits(subtaskId);
        }
    }

    private static final class MetadataSourceReader
            implements SourceReader<RowData, MetadataSplit> {

        private static final CompletableFuture<Void> AVAILABLE =
                CompletableFuture.completedFuture(null);

        private final MetadataLoader metadataLoader;

        @Nullable private MetadataSplit currentSplit;
        @Nullable private List<RowData> metadataRows;
        private CompletableFuture<Void> availability = new CompletableFuture<>();
        private boolean noMoreSplits;

        private MetadataSourceReader(MetadataLoader metadataLoader) {
            this.metadataLoader = metadataLoader;
        }

        @Override
        public void start() {}

        @Override
        public InputStatus pollNext(ReaderOutput<RowData> output) {
            if (currentSplit == null) {
                return noMoreSplits ? InputStatus.END_OF_INPUT : InputStatus.NOTHING_AVAILABLE;
            }

            if (metadataRows == null) {
                metadataRows = metadataLoader.load();
            }

            if (currentSplit.nextRow >= metadataRows.size()) {
                currentSplit = null;
                metadataRows = null;
                if (!noMoreSplits) {
                    availability = new CompletableFuture<>();
                }
                return noMoreSplits ? InputStatus.END_OF_INPUT : InputStatus.NOTHING_AVAILABLE;
            }

            output.collect(metadataRows.get(currentSplit.nextRow));
            currentSplit.advance();
            return InputStatus.MORE_AVAILABLE;
        }

        @Override
        public List<MetadataSplit> snapshotState(long checkpointId) {
            return currentSplit == null
                    ? Collections.emptyList()
                    : Collections.singletonList(new MetadataSplit(currentSplit.nextRow));
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            return currentSplit != null || noMoreSplits ? AVAILABLE : availability;
        }

        @Override
        public void addSplits(List<MetadataSplit> splits) {
            checkState(currentSplit == null, "A metadata split is already being read.");
            checkState(splits.size() == 1, "Expected exactly one metadata split.");
            currentSplit = splits.get(0);
            availability.complete(null);
        }

        @Override
        public void notifyNoMoreSplits() {
            noMoreSplits = true;
            availability.complete(null);
        }

        @Override
        public void close() {}
    }

    private static final class MetadataSplitSerializer
            implements SimpleVersionedSerializer<MetadataSplit> {

        private static final MetadataSplitSerializer INSTANCE = new MetadataSplitSerializer();
        private static final int VERSION = 1;

        @Override
        public int getVersion() {
            return VERSION;
        }

        @Override
        public byte[] serialize(MetadataSplit split) {
            return ByteBuffer.allocate(Integer.BYTES).putInt(split.nextRow).array();
        }

        @Override
        public MetadataSplit deserialize(int version, byte[] serialized) throws IOException {
            checkVersion(version);
            return new MetadataSplit(ByteBuffer.wrap(serialized).getInt());
        }
    }

    private static final class MetadataEnumeratorStateSerializer
            implements SimpleVersionedSerializer<MetadataEnumeratorState> {

        private static final MetadataEnumeratorStateSerializer INSTANCE =
                new MetadataEnumeratorStateSerializer();
        private static final int VERSION = 1;

        @Override
        public int getVersion() {
            return VERSION;
        }

        @Override
        public byte[] serialize(MetadataEnumeratorState state) {
            if (state.pendingSplit == null) {
                return new byte[] {0};
            }
            return ByteBuffer.allocate(1 + Integer.BYTES)
                    .put((byte) 1)
                    .putInt(state.pendingSplit.nextRow)
                    .array();
        }

        @Override
        public MetadataEnumeratorState deserialize(int version, byte[] serialized)
                throws IOException {
            checkVersion(version);
            ByteBuffer buffer = ByteBuffer.wrap(serialized);
            return buffer.get() == 0
                    ? new MetadataEnumeratorState(null)
                    : new MetadataEnumeratorState(new MetadataSplit(buffer.getInt()));
        }
    }

    private static void checkVersion(int version) throws IOException {
        if (version != 1) {
            throw new IOException("Unsupported metadata source serializer version: " + version);
        }
    }
}
