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

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.metadata.BucketInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.ExceptionUtils;

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
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.fluss.utils.Preconditions.checkState;

/** A bounded source that loads bucket metadata once for each execution. */
final class BucketsSource
        implements Source<
                        RowData, BucketsSource.BucketsSplit, BucketsSource.BucketsEnumeratorState>,
                ResultTypeQueryable<RowData> {

    private static final long serialVersionUID = 1L;

    private final BucketMetadataLoader metadataLoader;
    private final TypeInformation<RowData> producedType;

    BucketsSource(
            TablePath tablePath, Configuration flussConfig, TypeInformation<RowData> producedType) {
        this(new AdminBucketMetadataLoader(tablePath, flussConfig), producedType);
    }

    BucketsSource(BucketMetadataLoader metadataLoader, TypeInformation<RowData> producedType) {
        this.metadataLoader = metadataLoader;
        this.producedType = producedType;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SplitEnumerator<BucketsSplit, BucketsEnumeratorState> createEnumerator(
            SplitEnumeratorContext<BucketsSplit> enumContext) {
        return new BucketsSplitEnumerator(enumContext, new BucketsSplit(0));
    }

    @Override
    public SplitEnumerator<BucketsSplit, BucketsEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<BucketsSplit> enumContext, BucketsEnumeratorState checkpoint) {
        return new BucketsSplitEnumerator(enumContext, checkpoint.pendingSplit);
    }

    @Override
    public SimpleVersionedSerializer<BucketsSplit> getSplitSerializer() {
        return BucketsSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<BucketsEnumeratorState> getEnumeratorCheckpointSerializer() {
        return BucketsEnumeratorStateSerializer.INSTANCE;
    }

    @Override
    public SourceReader<RowData, BucketsSplit> createReader(SourceReaderContext readerContext) {
        return new BucketsSourceReader(metadataLoader);
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedType;
    }

    static RowData toRowData(BucketInfo bucketInfo) {
        return GenericRowData.of(
                StringData.fromString(bucketInfo.getTablePath().toString()),
                bucketInfo.getTableId(),
                optionalLong(bucketInfo.getPartitionId()),
                bucketInfo.getPartitionName() == null
                        ? null
                        : StringData.fromString(bucketInfo.getPartitionName()),
                bucketInfo.getBucketId(),
                optionalInt(bucketInfo.getLeaderId()),
                optionalInt(bucketInfo.getLeaderEpoch()),
                optionalInt(bucketInfo.getBucketEpoch()),
                new GenericArrayData(bucketInfo.getReplicas().toArray(new Integer[0])),
                new GenericArrayData(bucketInfo.getIsr().toArray(new Integer[0])));
    }

    private static Long optionalLong(OptionalLong value) {
        return value.isPresent() ? value.getAsLong() : null;
    }

    private static Integer optionalInt(OptionalInt value) {
        return value.isPresent() ? value.getAsInt() : null;
    }

    interface BucketMetadataLoader extends Serializable {
        List<RowData> load();
    }

    private static final class AdminBucketMetadataLoader implements BucketMetadataLoader {

        private static final long serialVersionUID = 1L;

        private final TablePath tablePath;
        private final Configuration flussConfig;

        private AdminBucketMetadataLoader(TablePath tablePath, Configuration flussConfig) {
            this.tablePath = tablePath;
            this.flussConfig = flussConfig;
        }

        @Override
        public List<RowData> load() {
            try (Connection connection = ConnectionFactory.createConnection(flussConfig);
                    Admin admin = connection.getAdmin()) {
                return admin.describeBuckets(tablePath).get().stream()
                        .map(BucketsSource::toRowData)
                        .collect(Collectors.toList());
            } catch (Exception e) {
                throw new FlussRuntimeException(
                        String.format("Failed to describe buckets for table %s.", tablePath),
                        ExceptionUtils.stripExecutionException(e));
            }
        }
    }

    static final class BucketsSplit implements SourceSplit, Serializable {

        private static final long serialVersionUID = 1L;
        private static final String SPLIT_ID = "buckets";

        private int nextRow;

        BucketsSplit(int nextRow) {
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

    static final class BucketsEnumeratorState implements Serializable {

        private static final long serialVersionUID = 1L;

        @Nullable private final BucketsSplit pendingSplit;

        private BucketsEnumeratorState(@Nullable BucketsSplit pendingSplit) {
            this.pendingSplit = pendingSplit;
        }
    }

    private static final class BucketsSplitEnumerator
            implements SplitEnumerator<BucketsSplit, BucketsEnumeratorState> {

        private final SplitEnumeratorContext<BucketsSplit> enumContext;
        @Nullable private BucketsSplit pendingSplit;

        private BucketsSplitEnumerator(
                SplitEnumeratorContext<BucketsSplit> enumContext,
                @Nullable BucketsSplit pendingSplit) {
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
        public void addSplitsBack(List<BucketsSplit> splits, int subtaskId) {
            checkState(pendingSplit == null, "A bucket metadata split is already pending.");
            checkState(splits.size() == 1, "Expected one bucket metadata split to be returned.");
            pendingSplit = splits.get(0);
        }

        @Override
        public void addReader(int subtaskId) {
            assignSplitOrSignalFinished(subtaskId);
        }

        @Override
        public BucketsEnumeratorState snapshotState(long checkpointId) {
            return new BucketsEnumeratorState(pendingSplit);
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

    private static final class BucketsSourceReader implements SourceReader<RowData, BucketsSplit> {

        private static final CompletableFuture<Void> AVAILABLE =
                CompletableFuture.completedFuture(null);

        private final BucketMetadataLoader metadataLoader;

        @Nullable private BucketsSplit currentSplit;
        @Nullable private List<RowData> bucketRows;
        private CompletableFuture<Void> availability = new CompletableFuture<>();
        private boolean noMoreSplits;

        private BucketsSourceReader(BucketMetadataLoader metadataLoader) {
            this.metadataLoader = metadataLoader;
        }

        @Override
        public void start() {}

        @Override
        public InputStatus pollNext(ReaderOutput<RowData> output) {
            if (currentSplit == null) {
                return noMoreSplits ? InputStatus.END_OF_INPUT : InputStatus.NOTHING_AVAILABLE;
            }

            if (bucketRows == null) {
                bucketRows = metadataLoader.load();
            }

            if (currentSplit.nextRow >= bucketRows.size()) {
                currentSplit = null;
                bucketRows = null;
                if (!noMoreSplits) {
                    availability = new CompletableFuture<>();
                }
                return noMoreSplits ? InputStatus.END_OF_INPUT : InputStatus.NOTHING_AVAILABLE;
            }

            output.collect(bucketRows.get(currentSplit.nextRow));
            currentSplit.advance();
            return InputStatus.MORE_AVAILABLE;
        }

        @Override
        public List<BucketsSplit> snapshotState(long checkpointId) {
            return currentSplit == null
                    ? Collections.emptyList()
                    : Collections.singletonList(new BucketsSplit(currentSplit.nextRow));
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            return currentSplit != null || noMoreSplits ? AVAILABLE : availability;
        }

        @Override
        public void addSplits(List<BucketsSplit> splits) {
            checkState(currentSplit == null, "A bucket metadata split is already being read.");
            checkState(splits.size() == 1, "Expected exactly one bucket metadata split.");
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

    private static final class BucketsSplitSerializer
            implements SimpleVersionedSerializer<BucketsSplit> {

        private static final BucketsSplitSerializer INSTANCE = new BucketsSplitSerializer();
        private static final int VERSION = 1;

        @Override
        public int getVersion() {
            return VERSION;
        }

        @Override
        public byte[] serialize(BucketsSplit split) {
            return ByteBuffer.allocate(Integer.BYTES).putInt(split.nextRow).array();
        }

        @Override
        public BucketsSplit deserialize(int version, byte[] serialized) throws IOException {
            checkVersion(version);
            return new BucketsSplit(ByteBuffer.wrap(serialized).getInt());
        }
    }

    private static final class BucketsEnumeratorStateSerializer
            implements SimpleVersionedSerializer<BucketsEnumeratorState> {

        private static final BucketsEnumeratorStateSerializer INSTANCE =
                new BucketsEnumeratorStateSerializer();
        private static final int VERSION = 1;

        @Override
        public int getVersion() {
            return VERSION;
        }

        @Override
        public byte[] serialize(BucketsEnumeratorState state) {
            if (state.pendingSplit == null) {
                return new byte[] {0};
            }
            return ByteBuffer.allocate(1 + Integer.BYTES)
                    .put((byte) 1)
                    .putInt(state.pendingSplit.nextRow)
                    .array();
        }

        @Override
        public BucketsEnumeratorState deserialize(int version, byte[] serialized)
                throws IOException {
            checkVersion(version);
            ByteBuffer buffer = ByteBuffer.wrap(serialized);
            return buffer.get() == 0
                    ? new BucketsEnumeratorState(null)
                    : new BucketsEnumeratorState(new BucketsSplit(buffer.getInt()));
        }
    }

    private static void checkVersion(int version) throws IOException {
        if (version != 1) {
            throw new IOException("Unsupported bucket source serializer version: " + version);
        }
    }
}
