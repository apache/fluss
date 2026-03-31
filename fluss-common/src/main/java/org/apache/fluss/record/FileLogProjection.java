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
import org.apache.fluss.record.FileLogInputStream.FileChannelLogRecordBatch;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.record.bytesview.MultiBytesView;
import org.apache.fluss.shaded.arrow.com.google.flatbuffers.FlatBufferBuilder;
import org.apache.fluss.shaded.arrow.org.apache.arrow.flatbuf.Buffer;
import org.apache.fluss.shaded.arrow.org.apache.arrow.flatbuf.FieldNode;
import org.apache.fluss.shaded.arrow.org.apache.arrow.flatbuf.Message;
import org.apache.fluss.shaded.arrow.org.apache.arrow.flatbuf.MessageHeader;
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

import javax.annotation.Nullable;

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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.fluss.record.DefaultLogRecordBatch.APPEND_ONLY_FLAG_MASK;
import static org.apache.fluss.record.LogRecordBatchFormat.LENGTH_OFFSET;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V0;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V1;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V2;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_OVERHEAD;
import static org.apache.fluss.record.LogRecordBatchFormat.MAGIC_OFFSET;
import static org.apache.fluss.record.LogRecordBatchFormat.V0_RECORD_BATCH_HEADER_SIZE;
import static org.apache.fluss.record.LogRecordBatchFormat.V1_RECORD_BATCH_HEADER_SIZE;
import static org.apache.fluss.record.LogRecordBatchFormat.V2_RECORD_BATCH_HEADER_SIZE;
import static org.apache.fluss.record.LogRecordBatchFormat.attributeOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.recordBatchHeaderSize;
import static org.apache.fluss.record.LogRecordBatchFormat.recordsCountOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.schemaIdOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.statisticsLengthOffset;
import static org.apache.fluss.utils.FileUtils.readFully;
import static org.apache.fluss.utils.FileUtils.readFullyOrFail;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
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
    private final ByteBuffer logHeaderBuffer = ByteBuffer.allocate(V2_RECORD_BATCH_HEADER_SIZE);

    private final ByteBuffer arrowHeaderBuffer = ByteBuffer.allocate(ARROW_HEADER_SIZE);
    private ByteBuffer arrowMetadataBuffer;
    private SchemaGetter schemaGetter;
    private long tableId;
    private ArrowCompressionInfo compressionInfo;
    private int[] selectedFieldPositions;
    /**
     * Variant sub-field projection hints. Maps top-level table column index to the list of
     * top-level Variant field names to project within that Variant's typed_value StructVector. Null
     * means project all sub-fields (no filtering).
     */
    @Nullable private Map<Integer, List<String>> variantFieldProjection;

    // Pre-computed HashSets to avoid per-batch allocation in Variant sub-field pruning.
    @Nullable private Map<Integer, Set<String>> variantFieldProjectionSets;

    // Cache the most recently seen shredded Schema and its derived ProjectionInfo plus serialized
    // Schema prefix to avoid recomputation when consecutive batches share the same actual Arrow
    // Schema.
    @Nullable private Schema cachedActualSchema;
    @Nullable private ProjectionInfo cachedShreddedProjection;
    @Nullable private byte[] cachedProjectedSchemaPrefix;
    private boolean cachedHasShredding;

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
        setCurrentProjection(tableId, schemaGetter, compressionInfo, selectedFieldPositions, null);
    }

    public void setCurrentProjection(
            long tableId,
            SchemaGetter schemaGetter,
            ArrowCompressionInfo compressionInfo,
            int[] selectedFieldPositions,
            @Nullable Map<Integer, List<String>> variantFieldProjection) {
        // Validate projection against the latest schema to catch genuinely invalid
        // projections (out-of-bound, non-ascending, duplicated) early.
        // Per-batch schema evolution filtering is handled in createProjectionInfo.
        int latestFieldCount =
                schemaGetter.getLatestSchemaInfo().getSchema().getRowType().getFieldCount();
        toBitSet(latestFieldCount, selectedFieldPositions);

        this.tableId = tableId;
        this.schemaGetter = schemaGetter;
        this.compressionInfo = compressionInfo;
        this.selectedFieldPositions = selectedFieldPositions;
        this.variantFieldProjection = variantFieldProjection;

        // Pre-compute HashSets for variant sub-field projection.
        if (variantFieldProjection != null) {
            this.variantFieldProjectionSets = new HashMap<>(variantFieldProjection.size());
            for (Map.Entry<Integer, List<String>> entry : variantFieldProjection.entrySet()) {
                this.variantFieldProjectionSets.put(
                        entry.getKey(), new HashSet<>(entry.getValue()));
            }
        } else {
            this.variantFieldProjectionSets = null;
        }

        // Reset shredded projection cache when projection changes.
        this.cachedActualSchema = null;
        this.cachedShreddedProjection = null;
        this.cachedProjectedSchemaPrefix = null;
        this.cachedHasShredding = false;
    }

    /**
     * Project a single record batch to a subset of fields. This is used by the filter path where
     * batches are iterated individually rather than as a contiguous file region.
     *
     * @param batch the file channel log record batch to project
     * @return the projected bytes view
     */
    public BytesView projectRecordBatch(FileChannelLogRecordBatch batch) throws IOException {
        FileChannel channel = batch.fileRecords.channel();
        int position = batch.position();

        // Schema ID determines which projection mapping to use (handles schema evolution).
        logHeaderBuffer.rewind();
        readLogHeaderFullyOrFail(channel, logHeaderBuffer, position);
        logHeaderBuffer.rewind();
        byte magic = logHeaderBuffer.get(MAGIC_OFFSET);
        int recordBatchHeaderSize = recordBatchHeaderSize(magic);
        int batchSizeInBytes = LOG_OVERHEAD + logHeaderBuffer.getInt(LENGTH_OFFSET);
        short schemaId = logHeaderBuffer.getShort(schemaIdOffset(magic));

        ProjectionInfo currentProjection = getOrCreateProjectionInfo(schemaId);
        checkNotNull(currentProjection, "There is no projection registered yet.");

        MultiBytesView.Builder builder = MultiBytesView.builder();

        // Empty batches (header-only) can occur for CDC log batches with no changes;
        // return empty projection to preserve offset advancement.
        if (batchSizeInBytes == recordBatchHeaderSize) {
            return builder.build();
        }

        projectSingleBatch(channel, position, currentProjection, builder, Integer.MAX_VALUE);
        return builder.build();
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
            if (position > end - V0_RECORD_BATCH_HEADER_SIZE) {
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

            // Return empty batch to push forward log offset. The empty batch was generated when
            // build cdc log batch when there
            // is no cdc log generated for this kv batch. See the comments about the field
            // 'lastOffsetDelta' in DefaultLogRecordBatch.
            if (batchSizeInBytes == recordBatchHeaderSize) {
                builder.addBytes(channel, position, batchSizeInBytes);
                position += batchSizeInBytes;
                continue;
            }

            int newBatchSizeInBytes =
                    projectSingleBatch(channel, position, currentProjection, builder, maxBytes);
            if (newBatchSizeInBytes < 0) {
                // the projected batch exceeds the remaining budget, stop here
                return new BytesViewLogRecords(builder.build());
            }

            maxBytes -= newBatchSizeInBytes;
            position += batchSizeInBytes;
        }

        return new BytesViewLogRecords(builder.build());
    }

    /**
     * Project a single non-empty record batch and append the projected bytes to the builder.
     *
     * <p>The caller must have already read the log header into {@link #logHeaderBuffer} and
     * verified that the batch is non-empty (i.e., batchSizeInBytes != recordBatchHeaderSize).
     *
     * @param channel the file channel to read from
     * @param position the start position of the batch in the file
     * @param currentProjection the projection info for the current schema
     * @param builder the builder to append projected bytes to
     * @param maxBytes the maximum allowed projected batch size; returns -1 if exceeded
     * @return the projected batch size in bytes, or -1 if the projected size exceeds maxBytes
     */
    private int projectSingleBatch(
            FileChannel channel,
            int position,
            ProjectionInfo currentProjection,
            MultiBytesView.Builder builder,
            int maxBytes)
            throws IOException {
        logHeaderBuffer.rewind();
        byte magic = logHeaderBuffer.get(MAGIC_OFFSET);
        int recordBatchHeaderSize = recordBatchHeaderSize(magic);

        boolean isAppendOnly =
                (logHeaderBuffer.get(attributeOffset(magic)) & APPEND_ONLY_FLAG_MASK) > 0;

        // For V1+, skip statistics data between header and records
        int statisticsLength = 0;
        if (magic >= LOG_MAGIC_VALUE_V1) {
            statisticsLength = logHeaderBuffer.getInt(statisticsLengthOffset(magic));
        }
        int recordsStartOffset = recordBatchHeaderSize + statisticsLength;

        final int changeTypeBytes;
        final long arrowHeaderOffset;
        if (isAppendOnly) {
            changeTypeBytes = 0;
            arrowHeaderOffset = position + recordsStartOffset;
        } else {
            changeTypeBytes = logHeaderBuffer.getInt(recordsCountOffset(magic));
            arrowHeaderOffset = position + recordsStartOffset + changeTypeBytes;
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

        // Handle embedded Schema IPC prefix (present when variant shredding
        // is active). The writer serializes Schema + RecordBatch for shredded
        // batches. We must:
        //   (1) compute projection from the actual (shredded) schema,
        //   (2) skip past the Schema to the RecordBatch,
        //   (3) optionally include a projected Schema prefix in the output.
        byte[] projectedSchemaPrefix = null;
        ProjectionInfo effectiveProjection = currentProjection;
        long recordBatchBodyOffset;

        if (metadata.headerType() == MessageHeader.Schema) {
            Schema actualSchema = MessageSerializer.deserializeSchema(metadata);

            // Reuse cached projection if the shredded schema hasn't changed.
            // Within a fetch, consecutive batches almost always share the same
            // shredded schema, so this avoids expensive re-computation.
            if (actualSchema.equals(cachedActualSchema)) {
                effectiveProjection = cachedShreddedProjection;
                projectedSchemaPrefix = cachedHasShredding ? cachedProjectedSchemaPrefix : null;
            } else {
                effectiveProjection = createProjectionInfoFromActualSchema(actualSchema);
                boolean hasShredding =
                        projectedFieldsHaveShredding(
                                actualSchema, effectiveProjection.selectedFieldPositions);
                byte[] schemaPrefix = null;
                if (hasShredding) {
                    schemaPrefix = serializeProjectedSchemaPrefix(actualSchema);
                }
                // Update cache
                cachedActualSchema = actualSchema;
                cachedShreddedProjection = effectiveProjection;
                cachedProjectedSchemaPrefix = schemaPrefix;
                cachedHasShredding = hasShredding;
                projectedSchemaPrefix = schemaPrefix;
            }

            // Advance past the Schema IPC message to the RecordBatch
            int paddedMetadataSize = (arrowMetadataSize + 7) & ~7;
            long rbOffset = arrowHeaderOffset + ARROW_HEADER_SIZE + paddedMetadataSize;

            // Re-read for the RecordBatch IPC message
            arrowHeaderBuffer.rewind();
            readFullyOrFail(channel, arrowHeaderBuffer, rbOffset, "record batch arrow header");
            arrowHeaderBuffer.position(ARROW_IPC_METADATA_SIZE_OFFSET);
            arrowMetadataSize = arrowHeaderBuffer.getInt();

            resizeArrowMetadataBuffer(arrowMetadataSize);
            arrowMetadataBuffer.rewind();
            readFullyOrFail(
                    channel,
                    arrowMetadataBuffer,
                    rbOffset + ARROW_HEADER_SIZE,
                    "record batch metadata");
            arrowMetadataBuffer.rewind();
            metadata = Message.getRootAsMessage(arrowMetadataBuffer);

            recordBatchBodyOffset = rbOffset + ARROW_HEADER_SIZE + arrowMetadataSize;
        } else {
            recordBatchBodyOffset = arrowHeaderOffset + ARROW_HEADER_SIZE + arrowMetadataSize;
        }

        // Project the RecordBatch
        ProjectedArrowBatch projectedArrowBatch =
                projectArrowBatch(
                        metadata,
                        effectiveProjection.nodesProjection,
                        effectiveProjection.buffersProjection,
                        effectiveProjection.bufferCount);
        long arrowBodyLength = projectedArrowBatch.bodyLength();

        int schemaPrefixLen = projectedSchemaPrefix != null ? projectedSchemaPrefix.length : 0;
        int newBatchSizeInBytes =
                recordBatchHeaderSize
                        + changeTypeBytes
                        + schemaPrefixLen
                        + effectiveProjection.arrowMetadataLength
                        + (int) arrowBodyLength;

        if (newBatchSizeInBytes > maxBytes) {
            return -1;
        }

        // create new arrow batch metadata which already projected
        byte[] headerMetadata =
                serializeArrowRecordBatchMetadata(
                        projectedArrowBatch, arrowBodyLength, effectiveProjection.bodyCompression);
        checkState(
                headerMetadata.length == effectiveProjection.arrowMetadataLength,
                "Invalid metadata length");

        // update and copy log batch header
        logHeaderBuffer.position(LENGTH_OFFSET);
        logHeaderBuffer.putInt(newBatchSizeInBytes - LOG_OVERHEAD);

        // For V1+ format, clear statistics information since projection removes statistics
        LogRecordBatchFormat.clearStatisticsFromHeader(logHeaderBuffer, magic);

        logHeaderBuffer.rewind();
        byte[] logHeader = new byte[recordBatchHeaderSize];
        logHeaderBuffer.get(logHeader);

        // build log records
        builder.addBytes(logHeader);
        if (!isAppendOnly) {
            builder.addBytes(channel, position + recordsStartOffset, changeTypeBytes);
        }
        if (projectedSchemaPrefix != null) {
            builder.addBytes(projectedSchemaPrefix);
        }
        builder.addBytes(headerMetadata);
        final long bodyBaseOffset = recordBatchBodyOffset;
        projectedArrowBatch.buffers.forEach(
                b -> builder.addBytes(channel, bodyBaseOffset + b.getOffset(), (int) b.getSize()));

        return newBatchSizeInBytes;
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

    // ---- Variant sub-field projection helpers ----

    /**
     * After {@link #flattenFields} marks all descendants of selected fields as selected, this
     * method deselects typed_value children that are NOT in {@link #variantFieldProjection}.
     *
     * <p>Only top-level Variant fields are pruned in this first implementation. Nested pruning
     * needs a path-aware walk that can retain ancestors while pruning selected descendants.
     */
    private void applyVariantSubFieldProjection(
            List<Field> schemaFields,
            BitSet topLevelSelection,
            int[] bufferLayoutCount,
            int[] bufferPrefixSum,
            BitSet nodesProjection,
            BitSet buffersProjection) {
        if (variantFieldProjectionSets == null || variantFieldProjectionSets.isEmpty()) {
            return;
        }
        int nodeIdx = 0;
        for (int topIdx = 0; topIdx < schemaFields.size(); topIdx++) {
            Field topField = schemaFields.get(topIdx);
            int topFieldNodeCount = countFieldNodes(topField);

            if (!topLevelSelection.get(topIdx)) {
                nodeIdx += topFieldNodeCount;
                continue;
            }

            Set<String> projectedSet = variantFieldProjectionSets.get(topIdx);
            if (projectedSet == null || projectedSet.isEmpty()) {
                nodeIdx += topFieldNodeCount;
                continue;
            }

            // Walk into the Variant Struct field tree in DFS order.
            nodeIdx++; // skip the top-level Variant Struct node

            for (Field child : topField.getChildren()) {
                if ("typed_value".equals(child.getName())) {
                    nodeIdx++; // skip the typed_value Struct node itself
                    for (Field tvChild : child.getChildren()) {
                        int childCount = countFieldNodes(tvChild);
                        if (!projectedSet.contains(tvChild.getName())) {
                            deSelectNodeRange(
                                    nodeIdx,
                                    childCount,
                                    bufferLayoutCount,
                                    bufferPrefixSum,
                                    nodesProjection,
                                    buffersProjection);
                        }
                        nodeIdx += childCount;
                    }
                } else {
                    nodeIdx += countFieldNodes(child);
                }
            }
        }
    }

    /** Recursively count the total number of field nodes in a field's subtree. */
    private static int countFieldNodes(Field field) {
        int count = 1;
        for (Field child : field.getChildren()) {
            count += countFieldNodes(child);
        }
        return count;
    }

    /** Deselect a contiguous range of nodes (and their buffers) from the projection BitSets. */
    private static void deSelectNodeRange(
            int startNode,
            int count,
            int[] bufferLayoutCount,
            int[] bufferPrefixSum,
            BitSet nodesProjection,
            BitSet buffersProjection) {
        int bufStart = bufferPrefixSum[startNode];
        for (int i = startNode; i < startNode + count; i++) {
            nodesProjection.clear(i);
            for (int j = 0; j < bufferLayoutCount[i]; j++) {
                buffersProjection.clear(bufStart + j);
            }
            bufStart += bufferLayoutCount[i];
        }
    }

    /**
     * Filter a Variant field's typed_value children, keeping only those in the projection list.
     *
     * <p>The residual metadata/value children are intentionally kept in V1. They preserve fallback
     * behavior and avoid reshaping the top-level Variant Struct while the first implementation
     * focuses on pruning top-level typed_value children. TODO: revisit residual value pruning once
     * the Arrow projection path has dedicated coverage for that layout.
     */
    private static Field filterVariantTypedValueChildren(
            Field variantField, Set<String> projectedSubFields) {
        List<Field> newChildren = new ArrayList<>();
        for (Field child : variantField.getChildren()) {
            if ("typed_value".equals(child.getName())) {
                List<Field> filteredTvChildren = new ArrayList<>();
                for (Field tvChild : child.getChildren()) {
                    if (projectedSubFields.contains(tvChild.getName())) {
                        filteredTvChildren.add(tvChild);
                    }
                }
                newChildren.add(
                        new Field(child.getName(), child.getFieldType(), filteredTvChildren));
            } else {
                newChildren.add(child);
            }
        }
        return new Field(variantField.getName(), variantField.getFieldType(), newChildren);
    }

    /**
     * Read log header fully or fail with EOFException if there is no enough bytes to read a full
     * log header. This handles different log header size for magic v0, v1 and v2.
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
            } else if (magic == LOG_MAGIC_VALUE_V2 && size < V2_RECORD_BATCH_HEADER_SIZE) {
                throw new EOFException(
                        String.format(
                                "Failed to read v2 log header from file channel `%s`. Expected to read %d bytes, "
                                        + "but reached end of file after reading %d bytes. Started read from position %d.",
                                channel, V2_RECORD_BATCH_HEADER_SIZE, size, position));
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
        int schemaFieldCount = arrowSchema.getFields().size();

        // Filter projection indices to valid range for this schema version.
        // This handles schema evolution: data written before shredding doesn't have
        // shredded columns, so those indices are simply skipped.
        int[] effectivePositions = selectedFieldPositions;
        if (selectedFieldPositions.length > 0
                && selectedFieldPositions[selectedFieldPositions.length - 1] >= schemaFieldCount) {
            effectivePositions =
                    Arrays.stream(selectedFieldPositions)
                            .filter(i -> i < schemaFieldCount)
                            .toArray();
        }

        BitSet selection = toBitSet(schemaFieldCount, effectivePositions);
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

        Schema projectedArrowSchema = ArrowUtils.toArrowSchema(rowType.project(effectivePositions));
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
                effectivePositions);
    }

    /**
     * Compute a per-batch {@link ProjectionInfo} from the actual Arrow Schema embedded in a
     * shredded batch. Unlike {@link #createProjectionInfo} (which derives the schema from RowType),
     * this accounts for typed_value children in Variant columns.
     */
    private ProjectionInfo createProjectionInfoFromActualSchema(Schema actualSchema) {
        int schemaFieldCount = actualSchema.getFields().size();

        int[] effectivePositions = selectedFieldPositions;
        if (selectedFieldPositions.length > 0
                && selectedFieldPositions[selectedFieldPositions.length - 1] >= schemaFieldCount) {
            effectivePositions =
                    Arrays.stream(selectedFieldPositions)
                            .filter(i -> i < schemaFieldCount)
                            .toArray();
        }

        BitSet selection = toBitSet(schemaFieldCount, effectivePositions);
        List<Tuple2<Field, Boolean>> flattenedFields = new ArrayList<>();
        flattenFields(actualSchema.getFields(), selection, flattenedFields);
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

        // Pre-compute buffer prefix sum for O(1) offset lookups in deSelectNodeRange.
        int[] bufferPrefixSum = new int[totalFieldNodes + 1];
        for (int i = 0; i < totalFieldNodes; i++) {
            bufferPrefixSum[i + 1] = bufferPrefixSum[i] + bufferLayoutCount[i];
        }

        // Apply variant sub-field projection: deselect non-projected typed_value children
        applyVariantSubFieldProjection(
                actualSchema.getFields(),
                selection,
                bufferLayoutCount,
                bufferPrefixSum,
                nodesProjection,
                buffersProjection);

        List<Field> projectedFields = new ArrayList<>();
        for (int pos : effectivePositions) {
            Field field = actualSchema.getFields().get(pos);
            if (variantFieldProjectionSets != null && variantFieldProjectionSets.containsKey(pos)) {
                field = filterVariantTypedValueChildren(field, variantFieldProjectionSets.get(pos));
            }
            projectedFields.add(field);
        }
        Schema projectedSchema = new Schema(projectedFields);
        ArrowBodyCompression bodyCompression =
                CompressionUtil.createBodyCompression(compressionInfo.createCompressionCodec());
        int metadataLength =
                ArrowUtils.estimateArrowMetadataLength(projectedSchema, bodyCompression);
        return new ProjectionInfo(
                nodesProjection,
                buffersProjection,
                bufferIndex,
                metadataLength,
                bodyCompression,
                effectivePositions);
    }

    /**
     * Check whether any of the projected fields has a "typed_value" child, indicating that the
     * projected output needs a Schema IPC prefix for correct deserialization.
     */
    private static boolean projectedFieldsHaveShredding(
            Schema actualSchema, int[] selectedPositions) {
        for (int pos : selectedPositions) {
            if (pos < actualSchema.getFields().size()) {
                for (Field child : actualSchema.getFields().get(pos).getChildren()) {
                    if ("typed_value".equals(child.getName())) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Serialize the projected actual Schema as an Arrow IPC Schema message. This prefix is included
     * in the projected output so the reader can create a VectorSchemaRoot with the correct
     * structure (including typed_value children).
     */
    private byte[] serializeProjectedSchemaPrefix(Schema actualSchema) throws IOException {
        List<Field> projectedFields = new ArrayList<>();
        for (int pos : selectedFieldPositions) {
            if (pos < actualSchema.getFields().size()) {
                Field field = actualSchema.getFields().get(pos);
                if (variantFieldProjectionSets != null
                        && variantFieldProjectionSets.containsKey(pos)) {
                    field =
                            filterVariantTypedValueChildren(
                                    field, variantFieldProjectionSets.get(pos));
                }
                projectedFields.add(field);
            }
        }
        Schema projectedSchema = new Schema(projectedFields);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        MessageSerializer.serialize(new WriteChannel(Channels.newChannel(baos)), projectedSchema);
        return baos.toByteArray();
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
