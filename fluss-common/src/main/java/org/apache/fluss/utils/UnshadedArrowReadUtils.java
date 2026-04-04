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
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.nio.ByteBuffer;

/** Utilities for loading and projecting Arrow scan batches with unshaded Arrow classes. */
@Internal
public final class UnshadedArrowReadUtils {

    private UnshadedArrowReadUtils() {}

    public static Schema toArrowSchema(RowType rowType) {
        try {
            return Schema.fromJSON(ArrowUtils.toArrowSchema(rowType).toJson());
        } catch (IOException e) {
            throw new RuntimeException("Failed to convert Arrow schema to unshaded schema.", e);
        }
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

    public static VectorSchemaRoot projectVectorSchemaRoot(
            VectorSchemaRoot sourceRoot,
            RowType targetRowType,
            int[] columnProjection,
            BufferAllocator allocator) {
        int rowCount = sourceRoot.getRowCount();
        Schema targetSchema = toArrowSchema(targetRowType);
        VectorSchemaRoot targetRoot = VectorSchemaRoot.create(targetSchema, allocator);
        for (int i = 0; i < columnProjection.length; i++) {
            FieldVector targetVector = targetRoot.getVector(i);
            initFieldVector(targetVector, rowCount);
            if (columnProjection[i] < 0) {
                fillNullVector(targetVector, rowCount);
            } else {
                FieldVector sourceVector = sourceRoot.getVector(columnProjection[i]);
                for (int rowId = 0; rowId < rowCount; rowId++) {
                    targetVector.copyFromSafe(rowId, rowId, sourceVector);
                }
                targetVector.setValueCount(rowCount);
            }
        }
        targetRoot.setRowCount(rowCount);
        return targetRoot;
    }

    private static void fillNullVector(FieldVector fieldVector, int rowCount) {
        for (int i = 0; i < rowCount; i++) {
            fieldVector.setNull(i);
        }
        fieldVector.setValueCount(rowCount);
    }

    private static void initFieldVector(FieldVector fieldVector, int rowCount) {
        fieldVector.setInitialCapacity(rowCount);
        if (fieldVector instanceof BaseFixedWidthVector) {
            ((BaseFixedWidthVector) fieldVector).allocateNew(rowCount);
        } else if (fieldVector instanceof BaseVariableWidthVector) {
            ((BaseVariableWidthVector) fieldVector).allocateNew(rowCount);
        } else if (fieldVector instanceof ListVector) {
            ListVector listVector = (ListVector) fieldVector;
            listVector.allocateNew();
            FieldVector dataVector = listVector.getDataVector();
            if (dataVector != null) {
                initFieldVector(dataVector, rowCount);
            }
        } else {
            fieldVector.allocateNew();
        }
    }
}
