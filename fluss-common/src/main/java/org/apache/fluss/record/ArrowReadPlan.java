/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.fluss.record;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.compression.ArrowCompressionFactory;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.row.ProjectedRow;
import org.apache.fluss.row.arrow.ArrowReader;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.TypeLayout;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.Field;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.ArrowUtils;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

/**
 * Reads Arrow record batches of one source schema. The loaded vectors, the buffers selected before
 * decompression and the output row mapping are all derived from the same selected source fields, so
 * they cannot disagree.
 *
 * <p>The plan owns a {@link VectorSchemaRoot} that is reused across batches: each {@link
 * #load(MemorySegment, int, int)} replaces the buffers of the previous one, and {@link #clear()}
 * releases them once a batch has been consumed.
 */
@Internal
@NotThreadSafe
public final class ArrowReadPlan implements AutoCloseable {
    private final BufferAllocator allocator;
    private final RowType loadedRowType;
    private final VectorSchemaRoot root;
    // null when every source field is loaded and no buffer needs to be skipped
    @Nullable private final int[] nodeIndexes;
    @Nullable private final int[] bufferIndexes;
    private final int nodeCount;
    private final int bufferCount;
    // null when loaded rows can be returned as-is
    @Nullable private final int[] outputMapping;

    /**
     * Creates a plan reading batches of {@code sourceType} into {@code allocator}.
     *
     * @param sourceType the row type of the serialized batch
     * @param outputToSource maps each output field to its source field (-1 if absent), or null if
     *     the output fields are the source fields
     * @param selectedFields the output fields read by the caller; unselected ones are never loaded
     */
    ArrowReadPlan(
            RowType sourceType,
            @Nullable int[] outputToSource,
            int[] selectedFields,
            BufferAllocator allocator) {
        int sourceFieldCount = sourceType.getFieldCount();
        int[] mapping = outputToSource;
        if (mapping == null) {
            mapping = new int[sourceFieldCount];
            Arrays.setAll(mapping, i -> i);
        }
        BitSet sourceFields = new BitSet(sourceFieldCount);
        for (int selectedField : selectedFields) {
            if (mapping[selectedField] >= 0) {
                sourceFields.set(mapping[selectedField]);
            }
        }
        int[] selectedSourceFields = sourceFields.stream().toArray();
        boolean pruneBuffers = selectedSourceFields.length < sourceFieldCount;

        this.allocator = allocator;
        this.loadedRowType = pruneBuffers ? sourceType.project(selectedSourceFields) : sourceType;
        this.outputMapping =
                outputToSource == null && !pruneBuffers
                        ? null
                        : toLoadedPositions(mapping, selectedSourceFields, sourceFieldCount);

        List<Field> flattenedFields = new ArrayList<>();
        BitSet selectedNodes = new BitSet();
        List<Field> fields = ArrowUtils.toArrowSchema(sourceType).getFields();
        for (int i = 0; i < fields.size(); i++) {
            int start = flattenedFields.size();
            flattenField(fields.get(i), flattenedFields);
            if (sourceFields.get(i)) {
                selectedNodes.set(start, flattenedFields.size());
            }
        }
        BitSet selectedBuffers = new BitSet();
        int bufferIndex = 0;
        for (int i = 0; i < flattenedFields.size(); i++) {
            int count = TypeLayout.getTypeBufferCount(flattenedFields.get(i).getType());
            if (selectedNodes.get(i)) {
                selectedBuffers.set(bufferIndex, bufferIndex + count);
            }
            bufferIndex += count;
        }
        this.nodeCount = flattenedFields.size();
        this.bufferCount = bufferIndex;
        this.nodeIndexes = pruneBuffers ? selectedNodes.stream().toArray() : null;
        this.bufferIndexes = pruneBuffers ? selectedBuffers.stream().toArray() : null;
        this.root = VectorSchemaRoot.create(ArrowUtils.toArrowSchema(loadedRowType), allocator);
    }

    /** Returns the row type of the loaded vectors: the selected source fields in source order. */
    public RowType getLoadedRowType() {
        return loadedRowType;
    }

    /**
     * Deserializes the Arrow batch and loads only the selected fields; buffers of unselected fields
     * are neither copied nor decompressed. Buffers of a previously loaded batch are replaced.
     */
    public ArrowReader load(MemorySegment segment, int arrowOffset, int arrowLength) {
        try (ArrowRecordBatch batch =
                nodeIndexes == null
                        ? ArrowUtils.deserializeArrowRecordBatch(
                                segment, arrowOffset, arrowLength, allocator)
                        : ArrowUtils.deserializeProjectedArrowRecordBatch(
                                segment,
                                arrowOffset,
                                arrowLength,
                                allocator,
                                nodeCount,
                                bufferCount,
                                nodeIndexes,
                                bufferIndexes)) {
            new FlussVectorLoader(root, ArrowCompressionFactory.INSTANCE).load(batch);
            return ArrowUtils.createArrowReader(root, loadedRowType);
        } catch (RuntimeException | Error e) {
            // The root is shared across batches, so partially loaded buffers must not linger.
            root.clear();
            throw e;
        }
    }

    /**
     * Creates a row view exposing loaded rows at the output positions expected by the caller, or
     * null if loaded rows already match them. Only selected output fields may be read; the others,
     * like fields absent from the source schema, are reported as null.
     */
    @Nullable
    public ProjectedRow createOutputRow() {
        return outputMapping == null ? null : ProjectedRow.from(outputMapping);
    }

    /** Releases the buffers of the last loaded batch while keeping the plan reusable. */
    public void clear() {
        root.clear();
    }

    @Override
    public void close() {
        root.close();
    }

    private static int[] toLoadedPositions(
            int[] outputToSource, int[] selectedSourceFields, int sourceFieldCount) {
        int[] sourceToLoaded = new int[sourceFieldCount];
        Arrays.fill(sourceToLoaded, -1);
        for (int i = 0; i < selectedSourceFields.length; i++) {
            sourceToLoaded[selectedSourceFields[i]] = i;
        }
        int[] outputToLoaded = new int[outputToSource.length];
        for (int i = 0; i < outputToSource.length; i++) {
            int sourceField = outputToSource[i];
            outputToLoaded[i] = sourceField < 0 ? -1 : sourceToLoaded[sourceField];
        }
        return outputToLoaded;
    }

    private static void flattenField(Field field, List<Field> flattenedFields) {
        flattenedFields.add(field);
        for (Field child : field.getChildren()) {
            flattenField(child, flattenedFields);
        }
    }
}
