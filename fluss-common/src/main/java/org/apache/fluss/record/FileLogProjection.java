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

package org.apache.fluss.record;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.compression.ArrowCompressionInfo;
import org.apache.fluss.exception.InvalidColumnProjectionException;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.record.bytesview.MultiBytesView;
import org.apache.fluss.shaded.arrow.com.google.flatbuffers.FlatBufferBuilder;
import org.apache.fluss.shaded.arrow.org.apache.arrow.flatbuf.Buffer;
import org.apache.fluss.shaded.arrow.org.apache.arrow.flatbuf.FieldNode;
import org.apache.fluss.shaded.arrow.org.apache.arrow.flatbuf.Message;
import org.apache.fluss.shaded.arrow.org.apache.arrow.flatbuf.RecordBatch;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.TypeLayout;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.ArrowBodyCompression;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.ArrowBuffer;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.Field;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.Schema;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.ArrowUtils;
import org.apache.fluss.utils.types.Tuple2;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import static org.apache.fluss.record.DefaultLogRecordBatch.APPEND_ONLY_FLAG_MASK;
import static org.apache.fluss.record.LogRecordBatchFormat.LENGTH_OFFSET;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V0;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V1;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_OVERHEAD;
import static org.apache.fluss.record.LogRecordBatchFormat.MAGIC_OFFSET;
import static org.apache.fluss.record.LogRecordBatchFormat.V0_RECORD_BATCH_HEADER_SIZE;
import static org.apache.fluss.record.LogRecordBatchFormat.V1_RECORD_BATCH_HEADER_SIZE;
import static org.apache.fluss.record.LogRecordBatchFormat.arrowChangeTypeOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.attributeOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.recordBatchHeaderSize;
import static org.apache.fluss.record.LogRecordBatchFormat.recordsCountOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.schemaIdOffset;
import static org.apache.fluss.utils.FileUtils.readFully;
import static org.apache.fluss.utils.FileUtils.readFullyOrFail;
import static org.apache.fluss.utils.Preconditions.checkState;

/** Column projection util on Arrow format {@link FileLogRecords}. */
public class FileLogProjection {

    // see the arrow binary message format in the page:
    // https://arrow.apache.org/docs/format/Columnar.html#encapsulated-message-format
    private static final int ARROW_IPC_CONTINUATION_LENGTH = 4;
    private static final int ARROW_IPC_METADATA_SIZE_OFFSET = ARROW_IPC_CONTINUATION_LENGTH;
    private static final int ARROW_IPC_METADATA_SIZE_LENGTH = 4;
    private static final int ARROW_HEADER_SIZE =
            ARROW_IPC_CONTINUATION_LENGTH + ARROW_IPC_METADATA_SIZE_LENGTH;

    // the projection cache shared in the TabletServer
    private final ProjectionPushdownCache projectionsCache;

    // shared resources for multiple projections
    private final ByteArrayOutputStream outputStream;
    private final WriteChannel writeChannel;

    /**
     * Buffer to read log records batch header. V1 is larger than V0, so use V1 head buffer can read
     * V0 header even if there is no enough bytes in log file.
     */
    private final ByteBuffer logHeaderBuffer = ByteBuffer.allocate(V1_RECORD_BATCH_HEADER_SIZE);

    private final ByteBuffer arrowHeaderBuffer = ByteBuffer.allocate(ARROW_HEADER_SIZE);
    private ByteBuffer arrowMetadataBuffer;
    private SchemaGetter schemaGetter;
    private long tableId;
    private ArrowCompressionInfo compressionInfo;
    private int[] selectedFieldPositions;

    public FileLogProjection(ProjectionPushdownCache projectionsCache) {
        this.projectionsCache = projectionsCache;
        this.outputStream = new ByteArrayOutputStream();
        this.writeChannel = new WriteChannel(Channels.newChannel(outputStream));
        // fluss use little endian for encoding log records batch
        this.logHeaderBuffer.order(ByteOrder.LITTLE_ENDIAN);
        // arrow force use little endian to encode int32 values
        this.arrowHeaderBuffer.order(ByteOrder.LITTLE_ENDIAN);
    }

    public void setCurrentProjection(
            long tableId,
            SchemaGetter schemaGetter,
            ArrowCompressionInfo compressionInfo,
            int[] selectedFieldPositions) {
        this.tableId = tableId;
        this.schemaGetter = schemaGetter;
        this.compressionInfo = compressionInfo;
        this.selectedFieldPositions = selectedFieldPositions;
    }

    /**
     * Project the log records to a subset of fields and the size of returned log records shouldn't
     * exceed maxBytes.
     *
     * @return the projected records.
     */
    public BytesViewLogRecords project(FileChannel channel, int start, int end, int maxBytes)
            throws IOException {

        MultiBytesView.Builder builder = MultiBytesView.builder();
        int position = start;

        ProjectionInfo currentProjection = null;
        short prevSchemaId = -1;
        // The condition is an optimization to avoid read log header when there is no enough bytes,
        // So we use V0 header size here for a conservative judgment. In the end, the condition
        // of (position >= end - recordBatchHeaderSize) will ensure the final correctness.
        while (maxBytes > V0_RECORD_BATCH_HEADER_SIZE) {
            if (position >= end - V0_RECORD_BATCH_HEADER_SIZE) {
                // the remaining bytes in the file are not enough to read a batch header up to
                // magic.
                return new BytesViewLogRecords(builder.build());
            }
            // read log header
            logHeaderBuffer.rewind();
            readLogHeaderFullyOrFail(channel, logHeaderBuffer, position);

            logHeaderBuffer.rewind();
            byte magic = logHeaderBuffer.get(MAGIC_OFFSET);
            int recordBatchHeaderSize = recordBatchHeaderSize(magic);
            int batchSizeInBytes = LOG_OVERHEAD + logHeaderBuffer.getInt(LENGTH_OFFSET);
            short schemaId = logHeaderBuffer.getShort(schemaIdOffset(magic));

            // reuse projection in the current log file
            if (currentProjection == null || prevSchemaId != schemaId) {
                prevSchemaId = schemaId;
                currentProjection = getOrCreateProjectionInfo(schemaId);
            }

            if (position > end - batchSizeInBytes) {
                // the remaining bytes in the file are not enough to read a full batch
                return new BytesViewLogRecords(builder.build());
            }

            // Skip empty batch. The empty batch was generated when build cdc log batch when there
            // is no cdc log generated for this kv batch. See the comments about the field
            // 'lastOffsetDelta' in DefaultLogRecordBatch.
            if (batchSizeInBytes == recordBatchHeaderSize) {
                position += batchSizeInBytes;
                continue;
            }

            boolean isAppendOnly =
                    (logHeaderBuffer.get(attributeOffset(magic)) & APPEND_ONLY_FLAG_MASK) > 0;

            final int changeTypeBytes;
            final long arrowHeaderOffset;
            if (isAppendOnly) {
                changeTypeBytes = 0;
                arrowHeaderOffset = position + recordBatchHeaderSize;
            } else {
                changeTypeBytes = logHeaderBuffer.getInt(recordsCountOffset(magic));
                arrowHeaderOffset = position + recordBatchHeaderSize + changeTypeBytes;
            }

            // read arrow header
            arrowHeaderBuffer.rewind();
            readFullyOrFail(channel, arrowHeaderBuffer, arrowHeaderOffset, "arrow header");
            arrowHeaderBuffer.position(ARROW_IPC_METADATA_SIZE_OFFSET);
            int arrowMetadataSize = arrowHeaderBuffer.getInt();

            resizeArrowMetadataBuffer(arrowMetadataSize);
            arrowMetadataBuffer.rewind();
            readFullyOrFail(
                    channel,
                    arrowMetadataBuffer,
                    arrowHeaderOffset + ARROW_HEADER_SIZE,
                    "arrow metadata");

            arrowMetadataBuffer.rewind();
            Message metadata = Message.getRootAsMessage(arrowMetadataBuffer);
            ProjectedArrowBatch projectedArrowBatch =
                    projectArrowBatch(
                            metadata,
                            currentProjection.nodesProjection,
                            currentProjection.buffersProjection,
                            currentProjection.bufferCount);
            long arrowBodyLength = projectedArrowBatch.bodyLength();

            int newBatchSizeInBytes =
                    recordBatchHeaderSize
                            + changeTypeBytes
                            + currentProjection.arrowMetadataLength
                            + (int) arrowBodyLength; // safe to cast to int
            if (newBatchSizeInBytes > maxBytes) {
                // the remaining bytes in the file are not enough to read a full batch
                return new BytesViewLogRecords(builder.build());
            }

            // 3. create new arrow batch metadata which already projected.
            byte[] headerMetadata =
                    serializeArrowRecordBatchMetadata(
                            projectedArrowBatch,
                            arrowBodyLength,
                            currentProjection.bodyCompression);
            checkState(
                    headerMetadata.length == currentProjection.arrowMetadataLength,
                    "Invalid metadata length");

            // 4. update and copy log batch header
            logHeaderBuffer.position(LENGTH_OFFSET);
            logHeaderBuffer.putInt(newBatchSizeInBytes - LOG_OVERHEAD);
            logHeaderBuffer.rewind();
            // the logHeader can't be reused, as it will be sent to network
            byte[] logHeader = new byte[recordBatchHeaderSize];
            logHeaderBuffer.get(logHeader);

            // 5. build log records
            builder.addBytes(logHeader);
            if (!isAppendOnly) {
                builder.addBytes(channel, position + arrowChangeTypeOffset(magic), changeTypeBytes);
            }
            builder.addBytes(headerMetadata);
            final long bufferOffset = arrowHeaderOffset + ARROW_HEADER_SIZE + arrowMetadataSize;
            projectedArrowBatch.buffers.forEach(
                    b ->
                            builder.addBytes(
                                    channel, bufferOffset + b.getOffset(), (int) b.getSize()));

            maxBytes -= newBatchSizeInBytes;
            position += batchSizeInBytes;
        }

        return new BytesViewLogRecords(builder.build());
    }

    private ProjectedArrowBatch projectArrowBatch(
            Message metadata, BitSet nodesProjection, BitSet buffersProjection, int bufferCount) {
        List<ArrowFieldNode> newNodes = new ArrayList<>();
        List<ArrowBuffer> newBufferLayouts = new ArrayList<>();
        List<ArrowBuffer> selectedBuffers = new ArrayList<>();
        RecordBatch recordBatch = (RecordBatch) metadata.header(new RecordBatch());
        long numRecords = recordBatch.length();
        for (int i = nodesProjection.nextSetBit(0); i >= 0; i = nodesProjection.nextSetBit(i + 1)) {
            FieldNode node = recordBatch.nodes(i);
            newNodes.add(new ArrowFieldNode(node.length(), node.nullCount()));
        }
        long bodyLength = metadata.bodyLength();
        long newOffset = 0L;
        for (int i = buffersProjection.nextSetBit(0);
                i >= 0;
                i = buffersProjection.nextSetBit(i + 1)) {
            Buffer buf = recordBatch.buffers(i);
            long nextOffset =
                    i < bufferCount - 1 ? recordBatch.buffers(i + 1).offset() : bodyLength;
            long paddedLength = nextOffset - buf.offset();
            selectedBuffers.add(new ArrowBuffer(buf.offset(), paddedLength));
            newBufferLayouts.add(new ArrowBuffer(newOffset, buf.length()));
            newOffset += paddedLength;
        }

        return new ProjectedArrowBatch(numRecords, newNodes, newBufferLayouts, selectedBuffers);
    }

    /**
     * Serialize metadata of a {@link ArrowRecordBatch}. This avoids to create an instance of {@link
     * ArrowRecordBatch}.
     *
     * @see MessageSerializer#serialize(WriteChannel, ArrowRecordBatch)
     * @see ArrowRecordBatch#writeTo(FlatBufferBuilder)
     */
    private byte[] serializeArrowRecordBatchMetadata(
            ProjectedArrowBatch batch, long arrowBodyLength, ArrowBodyCompression bodyCompression)
            throws IOException {
        outputStream.reset();
        ArrowUtils.serializeArrowRecordBatchMetadata(
                writeChannel,
                batch.numRecords,
                batch.nodes,
                batch.buffersLayout,
                bodyCompression,
                arrowBodyLength);
        return outputStream.toByteArray();
    }

    private void resizeArrowMetadataBuffer(int metadataSize) {
        if (arrowMetadataBuffer == null || arrowMetadataBuffer.capacity() < metadataSize) {
            arrowMetadataBuffer = ByteBuffer.allocate(metadataSize);
            arrowMetadataBuffer.order(ByteOrder.LITTLE_ENDIAN);
        } else {
            arrowMetadataBuffer.limit(metadataSize);
        }
    }

    /** Flatten fields by a pre-order depth-first traversal of the fields in the schema. */
    private void flattenFields(
            List<Field> arrowFields,
            BitSet selectedFields,
            List<Tuple2<Field, Boolean>> flattenedFields) {
        for (int i = 0; i < arrowFields.size(); i++) {
            Field field = arrowFields.get(i);
            boolean selected = selectedFields.get(i);
            flattenedFields.add(Tuple2.of(field, selected));
            List<Field> children = field.getChildren();
            flattenFields(children, fillBitSet(children.size(), selected), flattenedFields);
        }
    }

    private static BitSet toBitSet(int length, int[] selectedIndexes) {
        BitSet bitset = new BitSet(length);
        int prev = -1;
        for (int i : selectedIndexes) {
            if (i < prev) {
                throw new InvalidColumnProjectionException(
                        "The projection indexes should be in field order, but is "
                                + Arrays.toString(selectedIndexes));
            } else if (i == prev) {
                throw new InvalidColumnProjectionException(
                        "The projection indexes should not contain duplicated fields, but is "
                                + Arrays.toString(selectedIndexes));
            } else if (i >= length) {
                throw new InvalidColumnProjectionException(
                        "Projected fields "
                                + Arrays.toString(selectedIndexes)
                                + " is out of bound for schema with "
                                + length
                                + " fields.");
            }
            bitset.set(i);
            prev = i;
        }
        return bitset;
    }

    private static BitSet fillBitSet(int length, boolean value) {
        BitSet bitset = new BitSet(length);
        if (value) {
            bitset.set(0, length);
        } else {
            bitset.clear();
        }
        return bitset;
    }

    /**
     * Read log header fully or fail with EOFException if there is no enough bytes to read a full
     * log header. This handles different log header size for magic v0 and v1.
     */
    static void readLogHeaderFullyOrFail(FileChannel channel, ByteBuffer buffer, int position)
            throws IOException {
        if (position < 0) {
            throw new IllegalArgumentException(
                    "The file channel position cannot be negative, but it is " + position);
        }
        readFully(channel, buffer, position);
        if (buffer.hasRemaining()) {
            int size = buffer.position();
            byte magic = buffer.get(MAGIC_OFFSET);
            if (magic == LOG_MAGIC_VALUE_V0 && size < V0_RECORD_BATCH_HEADER_SIZE) {
                throw new EOFException(
                        String.format(
                                "Failed to read v0 log header from file channel `%s`. Expected to read %d bytes, "
                                        + "but reached end of file after reading %d bytes. Started read from position %d.",
                                channel, V0_RECORD_BATCH_HEADER_SIZE, size, position));
            } else if (magic == LOG_MAGIC_VALUE_V1 && size < V1_RECORD_BATCH_HEADER_SIZE) {
                throw new EOFException(
                        String.format(
                                "Failed to read v1 log header from file channel `%s`. Expected to read %d bytes, "
                                        + "but reached end of file after reading %d bytes. Started read from position %d.",
                                channel, V1_RECORD_BATCH_HEADER_SIZE, size, position));
            }
        }
    }

    @VisibleForTesting
    ByteBuffer getLogHeaderBuffer() {
        return logHeaderBuffer;
    }

    private ProjectionInfo getOrCreateProjectionInfo(short schemaId) {
        ProjectionInfo cachedProjection =
                projectionsCache.getProjectionInfo(tableId, schemaId, selectedFieldPositions);
        if (cachedProjection == null) {
            cachedProjection = createProjectionInfo(schemaId, selectedFieldPositions);
            projectionsCache.setProjectionInfo(
                    tableId, schemaId, selectedFieldPositions, cachedProjection);
        }
        return cachedProjection;
    }

    private ProjectionInfo createProjectionInfo(short schemaId, int[] selectedFieldPositions) {
        org.apache.fluss.metadata.Schema schema = schemaGetter.getSchema(schemaId);
        RowType rowType = schema.getRowType();

        // initialize the projection util information
        Schema arrowSchema = ArrowUtils.toArrowSchema(rowType);
        BitSet selection = toBitSet(arrowSchema.getFields().size(), selectedFieldPositions);
        List<Tuple2<Field, Boolean>> flattenedFields = new ArrayList<>();
        flattenFields(arrowSchema.getFields(), selection, flattenedFields);
        int totalFieldNodes = flattenedFields.size();
        int[] bufferLayoutCount = new int[totalFieldNodes];
        BitSet nodesProjection = new BitSet(totalFieldNodes);
        int totalBuffers = 0;
        for (int i = 0; i < totalFieldNodes; i++) {
            Field fieldNode = flattenedFields.get(i).f0;
            boolean selected = flattenedFields.get(i).f1;
            nodesProjection.set(i, selected);
            bufferLayoutCount[i] = TypeLayout.getTypeBufferCount(fieldNode.getType());
            totalBuffers += bufferLayoutCount[i];
        }
        BitSet buffersProjection = new BitSet(totalBuffers);
        int bufferIndex = 0;
        for (int i = 0; i < totalFieldNodes; i++) {
            if (nodesProjection.get(i)) {
                buffersProjection.set(bufferIndex, bufferIndex + bufferLayoutCount[i]);
            }
            bufferIndex += bufferLayoutCount[i];
        }

        Schema projectedArrowSchema =
                ArrowUtils.toArrowSchema(rowType.project(selectedFieldPositions));
        ArrowBodyCompression bodyCompression =
                CompressionUtil.createBodyCompression(compressionInfo.createCompressionCodec());
        int metadataLength =
                ArrowUtils.estimateArrowMetadataLength(projectedArrowSchema, bodyCompression);
        return new ProjectionInfo(
                nodesProjection,
                buffersProjection,
                bufferIndex,
                metadataLength,
                bodyCompression,
                selectedFieldPositions);
    }

    /** Projection pushdown information for a specific schema and selected fields. */
    public static final class ProjectionInfo {
        final BitSet nodesProjection;
        final BitSet buffersProjection;
        final int bufferCount;
        final int arrowMetadataLength;
        final ArrowBodyCompression bodyCompression;
        final int[] selectedFieldPositions;

        private ProjectionInfo(
                BitSet nodesProjection,
                BitSet buffersProjection,
                int bufferCount,
                int arrowMetadataLength,
                ArrowBodyCompression bodyCompression,
                int[] selectedFieldPositions) {
            this.nodesProjection = nodesProjection;
            this.buffersProjection = buffersProjection;
            this.bufferCount = bufferCount;
            this.arrowMetadataLength = arrowMetadataLength;
            this.bodyCompression = bodyCompression;
            this.selectedFieldPositions = selectedFieldPositions;
        }
    }

    /** Metadata of a projected arrow record batch. */
    static final class ProjectedArrowBatch {
        /** Number of records. */
        final long numRecords;

        /** The projected nodes of {@link ArrowRecordBatch#getNodes()}. */
        final List<ArrowFieldNode> nodes;

        /** The new buffer layouts of the {@link #buffers}. */
        final List<ArrowBuffer> buffersLayout;

        /** The projected buffer positions of {@link ArrowRecordBatch#getBuffers()}. */
        final List<ArrowBuffer> buffers;

        public ProjectedArrowBatch(
                long numRecords,
                List<ArrowFieldNode> nodes,
                List<ArrowBuffer> buffersLayout,
                List<ArrowBuffer> buffers) {
            this.numRecords = numRecords;
            this.nodes = nodes;
            this.buffersLayout = buffersLayout;
            this.buffers = buffers;
        }

        public long bodyLength() {
            long bodyLength = 0;
            for (ArrowBuffer buffer : buffers) {
                bodyLength += buffer.getSize();
            }
            return bodyLength;
        }
    }
}
