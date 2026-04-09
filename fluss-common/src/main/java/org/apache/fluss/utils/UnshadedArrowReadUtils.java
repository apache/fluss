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
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/** Utilities for loading and projecting Arrow scan batches with unshaded Arrow classes. */
@Internal
public final class UnshadedArrowReadUtils {

    private UnshadedArrowReadUtils() {}

    /** Converts a Fluss RowType to an unshaded Arrow Schema, reusing shaded ArrowUtils logic. */
    public static Schema toArrowSchema(RowType rowType) {
        org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.Schema shadedSchema =
                ArrowUtils.toArrowSchema(rowType);
        List<Field> fields = new ArrayList<>(shadedSchema.getFields().size());
        for (org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.Field f :
                shadedSchema.getFields()) {
            fields.add(convertField(f));
        }
        return new Schema(fields);
    }

    /** Recursively converts a shaded Arrow Field to an unshaded Arrow Field. */
    private static Field convertField(
            org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.Field shadedField) {
        ArrowType arrowType = convertArrowType(shadedField.getType());
        FieldType fieldType =
                new FieldType(shadedField.isNullable(), arrowType, null, shadedField.getMetadata());
        List<Field> children = null;
        if (shadedField.getChildren() != null && !shadedField.getChildren().isEmpty()) {
            children = new ArrayList<>(shadedField.getChildren().size());
            for (org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.Field child :
                    shadedField.getChildren()) {
                children.add(convertField(child));
            }
        }
        return new Field(shadedField.getName(), fieldType, children);
    }

    /** Converts a shaded ArrowType to an unshaded ArrowType. */
    private static ArrowType convertArrowType(
            org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.ArrowType shadedType) {
        switch (shadedType.getTypeID().getFlatbufID()) {
            case 2: // Int
                org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.ArrowType.Int
                        intType =
                                (org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo
                                                .ArrowType.Int)
                                        shadedType;
                return new ArrowType.Int(intType.getBitWidth(), intType.getIsSigned());
            case 3: // FloatingPoint
                org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.ArrowType
                                .FloatingPoint
                        fpType =
                                (org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo
                                                .ArrowType.FloatingPoint)
                                        shadedType;
                return new ArrowType.FloatingPoint(
                        FloatingPointPrecision.valueOf(fpType.getPrecision().name()));
            case 4: // Binary
                return ArrowType.Binary.INSTANCE;
            case 5: // Utf8
                return ArrowType.Utf8.INSTANCE;
            case 6: // Bool
                return ArrowType.Bool.INSTANCE;
            case 7: // Decimal
                org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.ArrowType.Decimal
                        decType =
                                (org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo
                                                .ArrowType.Decimal)
                                        shadedType;
                return new ArrowType.Decimal(
                        decType.getPrecision(), decType.getScale(), decType.getBitWidth());
            case 8: // Date
                org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.ArrowType.Date
                        dateType =
                                (org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo
                                                .ArrowType.Date)
                                        shadedType;
                return new ArrowType.Date(DateUnit.valueOf(dateType.getUnit().name()));
            case 9: // Time
                org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.ArrowType.Time
                        timeType =
                                (org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo
                                                .ArrowType.Time)
                                        shadedType;
                return new ArrowType.Time(
                        TimeUnit.valueOf(timeType.getUnit().name()), timeType.getBitWidth());
            case 10: // Timestamp
                org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.ArrowType.Timestamp
                        tsType =
                                (org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo
                                                .ArrowType.Timestamp)
                                        shadedType;
                return new ArrowType.Timestamp(
                        TimeUnit.valueOf(tsType.getUnit().name()), tsType.getTimezone());
            case 12: // List
                return ArrowType.List.INSTANCE;
            case 13: // Struct
                return ArrowType.Struct.INSTANCE;
            case 14: // Map
                org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.ArrowType.Map
                        mapType =
                                (org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo
                                                .ArrowType.Map)
                                        shadedType;
                return new ArrowType.Map(mapType.getKeysSorted());
            case 15: // FixedSizeBinary
                org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.ArrowType
                                .FixedSizeBinary
                        fsbType =
                                (org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo
                                                .ArrowType.FixedSizeBinary)
                                        shadedType;
                return new ArrowType.FixedSizeBinary(fsbType.getByteWidth());
            default:
                throw new UnsupportedOperationException(
                        "Unsupported Arrow type: " + shadedType.getTypeID());
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
                // TODO: Optimize this projection path with Arrow vector transfer/copy utilities
                // when we only need to reuse or reorder existing columns. The current row-by-row
                // copy is acceptable for now because this path is not a hot path and is mainly
                // used by the tiering service, which recreates scanners instead of scanning in a
                // tight loop.
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
