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

package org.apache.fluss.utils;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.compression.UnshadedArrowCompressionFactory;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.record.UnshadedFlussVectorLoader;
import org.apache.fluss.types.RowType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/** Utilities for loading and projecting Arrow scan batches with unshaded Arrow classes. */
@Internal
public final class UnshadedArrowReadUtils {

    private UnshadedArrowReadUtils() {}

    /**
     * Converts a Fluss RowType to an unshaded Arrow Schema.
     *
     * <p>Converts via FlatBuffer binary serialization: shaded Schema → bytes → unshaded Schema.
     * Both shaded and unshaded Arrow use the same FlatBuffer wire format, so the binary
     * representation is identical regardless of Java package relocation.
     */
    public static Schema toArrowSchema(RowType rowType) {
        org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.Schema shadedSchema =
                ArrowUtils.toArrowSchema(rowType);
        return Schema.deserializeMessage(ByteBuffer.wrap(shadedSchema.serializeAsMessage()));
    }

    public static void loadArrowBatch(
            MemorySegment segment,
            int arrowOffset,
            int arrowLength,
            VectorSchemaRoot schemaRoot,
            BufferAllocator allocator) {
        ByteBuffer arrowBatchBuffer = segment.wrap(arrowOffset, arrowLength);
        try (ReadChannel channel =
                        new ReadChannel(new ByteBufferReadableChannel(arrowBatchBuffer));
                ArrowRecordBatch batch =
                        MessageSerializer.deserializeRecordBatch(channel, allocator)) {
            new UnshadedFlussVectorLoader(schemaRoot, UnshadedArrowCompressionFactory.INSTANCE)
                    .load(batch);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize ArrowRecordBatch.", e);
        }
    }

    /**
     * Projects a VectorSchemaRoot from old schema to target schema using a column projection
     * mapping.
     *
     * @param sourceRoot the source VectorSchemaRoot with old schema data
     * @param targetRowType the target RowType to project to
     * @param columnProjection mapping from target column index to source column index; -1 means
     *     column doesn't exist in source (filled with nulls)
     * @param allocator the allocator for creating new vectors
     * @return a new VectorSchemaRoot with the target schema
     */
    public static VectorSchemaRoot projectVectorSchemaRoot(
            VectorSchemaRoot sourceRoot,
            RowType targetRowType,
            int[] columnProjection,
            BufferAllocator allocator) {
        int rowCount = sourceRoot.getRowCount();
        Schema targetSchema = toArrowSchema(targetRowType);
        List<Field> targetFields = targetSchema.getFields();
        List<FieldVector> targetVectors = new ArrayList<>(columnProjection.length);
        try {
            for (int i = 0; i < columnProjection.length; i++) {
                if (columnProjection[i] < 0) {
                    // Column doesn't exist in source, fill with nulls
                    FieldVector targetVector = targetFields.get(i).createVector(allocator);
                    targetVector.allocateNew();
                    for (int rowId = 0; rowId < rowCount; rowId++) {
                        targetVector.setNull(rowId);
                    }
                    targetVector.setValueCount(rowCount);
                    targetVectors.add(targetVector);
                } else {
                    FieldVector sourceVector = sourceRoot.getVector(columnProjection[i]);
                    TransferPair transfer = sourceVector.getTransferPair(allocator);
                    transfer.splitAndTransfer(0, rowCount);
                    targetVectors.add((FieldVector) transfer.getTo());
                }
            }
        } catch (Exception e) {
            targetVectors.forEach(FieldVector::close);
            throw e;
        }
        return new VectorSchemaRoot(targetFields, targetVectors, rowCount);
    }
}
