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
import org.apache.fluss.compression.ArrowCompressionFactory;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.row.arrow.ArrowReader;
import org.apache.fluss.row.arrow.vectors.ArrowArrayColumnVector;
import org.apache.fluss.row.arrow.vectors.ArrowBigIntColumnVector;
import org.apache.fluss.row.arrow.vectors.ArrowBinaryColumnVector;
import org.apache.fluss.row.arrow.vectors.ArrowBooleanColumnVector;
import org.apache.fluss.row.arrow.vectors.ArrowDateColumnVector;
import org.apache.fluss.row.arrow.vectors.ArrowDecimalColumnVector;
import org.apache.fluss.row.arrow.vectors.ArrowDoubleColumnVector;
import org.apache.fluss.row.arrow.vectors.ArrowFloatColumnVector;
import org.apache.fluss.row.arrow.vectors.ArrowIntColumnVector;
import org.apache.fluss.row.arrow.vectors.ArrowSmallIntColumnVector;
import org.apache.fluss.row.arrow.vectors.ArrowTimeColumnVector;
import org.apache.fluss.row.arrow.vectors.ArrowTimestampLtzColumnVector;
import org.apache.fluss.row.arrow.vectors.ArrowTimestampNtzColumnVector;
import org.apache.fluss.row.arrow.vectors.ArrowTinyIntColumnVector;
import org.apache.fluss.row.arrow.vectors.ArrowVarBinaryColumnVector;
import org.apache.fluss.row.arrow.vectors.ArrowVarCharColumnVector;
import org.apache.fluss.row.arrow.writers.ArrowArrayWriter;
import org.apache.fluss.row.arrow.writers.ArrowBigIntWriter;
import org.apache.fluss.row.arrow.writers.ArrowBinaryWriter;
import org.apache.fluss.row.arrow.writers.ArrowBooleanWriter;
import org.apache.fluss.row.arrow.writers.ArrowDateWriter;
import org.apache.fluss.row.arrow.writers.ArrowDecimalWriter;
import org.apache.fluss.row.arrow.writers.ArrowDoubleWriter;
import org.apache.fluss.row.arrow.writers.ArrowFieldWriter;
import org.apache.fluss.row.arrow.writers.ArrowFloatWriter;
import org.apache.fluss.row.arrow.writers.ArrowIntWriter;
import org.apache.fluss.row.arrow.writers.ArrowSmallIntWriter;
import org.apache.fluss.row.arrow.writers.ArrowTimeWriter;
import org.apache.fluss.row.arrow.writers.ArrowTimestampLtzWriter;
import org.apache.fluss.row.arrow.writers.ArrowTimestampNtzWriter;
import org.apache.fluss.row.arrow.writers.ArrowTinyIntWriter;
import org.apache.fluss.row.arrow.writers.ArrowVarBinaryWriter;
import org.apache.fluss.row.arrow.writers.ArrowVarCharWriter;
import org.apache.fluss.row.columnar.ColumnVector;
import org.apache.fluss.shaded.arrow.com.google.flatbuffers.FlatBufferBuilder;
import org.apache.fluss.shaded.arrow.org.apache.arrow.flatbuf.MessageHeader;
import org.apache.fluss.shaded.arrow.org.apache.arrow.flatbuf.RecordBatch;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.ArrowBuf;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.BigIntVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.BitVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.DateDayVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.DecimalVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.FieldVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.Float4Vector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.Float8Vector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.IntVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.SmallIntVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.TimeMicroVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.TimeMilliVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.TimeNanoVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.TimeSecVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.TimeStampVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.TinyIntVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.TypeLayout;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ValueVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VarBinaryVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VarCharVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VectorLoader;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.complex.ListVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.ArrowBodyCompression;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.ArrowBuffer;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.FBSerializables;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.DateUnit;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.TimeUnit;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.Types;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.Field;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.Schema;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.util.DataSizeRoundingUtil;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.BigIntType;
import org.apache.fluss.types.BinaryType;
import org.apache.fluss.types.BooleanType;
import org.apache.fluss.types.BytesType;
import org.apache.fluss.types.CharType;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeDefaultVisitor;
import org.apache.fluss.types.DateType;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.DoubleType;
import org.apache.fluss.types.FloatType;
import org.apache.fluss.types.IntType;
import org.apache.fluss.types.LocalZonedTimestampType;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.SmallIntType;
import org.apache.fluss.types.StringType;
import org.apache.fluss.types.TimeType;
import org.apache.fluss.types.TimestampType;
import org.apache.fluss.types.TinyIntType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.MessageSerializer.deserializeRecordBatch;
import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Utilities for Arrow. */
@Internal
public class ArrowUtils {

    /** Returns the Arrow schema of the specified type. */
    public static Schema toArrowSchema(RowType rowType) {
        List<Field> fields =
                rowType.getFields().stream()
                        .map(f -> toArrowField(f.getName(), f.getType()))
                        .collect(Collectors.toList());
        return new Schema(fields);
    }

    /**
     * Creates an {@link ArrowReader} for the specified memory segment and {@link VectorSchemaRoot}.
     */
    public static ArrowReader createArrowReader(
            MemorySegment segment,
            int arrowOffset,
            int arrowLength,
            VectorSchemaRoot schemaRoot,
            BufferAllocator allocator,
            RowType rowType) {
        ByteBuffer arrowBatchBuffer = segment.wrap(arrowOffset, arrowLength);
        try (ReadChannel channel =
                        new ReadChannel(new ByteBufferReadableChannel(arrowBatchBuffer));
                ArrowRecordBatch batch = deserializeRecordBatch(channel, allocator)) {
            VectorLoader vectorLoader =
                    new VectorLoader(schemaRoot, ArrowCompressionFactory.INSTANCE);
            vectorLoader.load(batch);
            List<ColumnVector> columnVectors = new ArrayList<>();
            List<FieldVector> fieldVectors = schemaRoot.getFieldVectors();
            for (int i = 0; i < fieldVectors.size(); i++) {
                columnVectors.add(
                        createArrowColumnVector(fieldVectors.get(i), rowType.getTypeAt(i)));
            }
            return new ArrowReader(schemaRoot, columnVectors.toArray(new ColumnVector[0]));
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize ArrowRecordBatch.", e);
        }
    }

    /**
     * Serialize metadata of a {@link ArrowRecordBatch} into write channel. This avoids to create an
     * instance of {@link ArrowRecordBatch}.
     *
     * @return the serialized size in bytes
     * @see MessageSerializer#serialize(WriteChannel, ArrowRecordBatch)
     * @see ArrowRecordBatch#writeTo(FlatBufferBuilder)
     */
    public static int serializeArrowRecordBatchMetadata(
            WriteChannel writeChannel,
            long numRecords,
            List<ArrowFieldNode> nodes,
            List<ArrowBuffer> buffersLayout,
            ArrowBodyCompression arrowBodyCompression,
            long arrowBodyLength)
            throws IOException {
        checkArgument(arrowBodyLength % 8 == 0, "batch is not aligned");
        FlatBufferBuilder builder = new FlatBufferBuilder();

        RecordBatch.startNodesVector(builder, nodes.size());
        int nodesOffset = FBSerializables.writeAllStructsToVector(builder, nodes);
        RecordBatch.startBuffersVector(builder, buffersLayout.size());
        int buffersOffset = FBSerializables.writeAllStructsToVector(builder, buffersLayout);
        int compressOffset = 0;
        if (arrowBodyCompression.getCodec() != NoCompressionCodec.COMPRESSION_TYPE) {
            compressOffset = arrowBodyCompression.writeTo(builder);
        }

        RecordBatch.startRecordBatch(builder);
        RecordBatch.addLength(builder, numRecords);
        RecordBatch.addNodes(builder, nodesOffset);
        RecordBatch.addBuffers(builder, buffersOffset);
        if (arrowBodyCompression.getCodec() != NoCompressionCodec.COMPRESSION_TYPE) {
            RecordBatch.addCompression(builder, compressOffset);
        }
        int batchOffset = RecordBatch.endRecordBatch(builder);
        ByteBuffer metadata =
                MessageSerializer.serializeMessage(
                        builder,
                        MessageHeader.RecordBatch,
                        batchOffset,
                        arrowBodyLength,
                        IpcOption.DEFAULT);

        return MessageSerializer.writeMessageBuffer(writeChannel, metadata.remaining(), metadata);
    }

    /** Estimates the size of {@link ArrowRecordBatch} metadata for the given schema. */
    public static int estimateArrowMetadataLength(
            Schema arrowSchema, ArrowBodyCompression bodyCompression) {
        List<Field> fields = flattenFields(arrowSchema.getFields());
        List<ArrowFieldNode> nodes = createFieldNodes(fields);
        List<ArrowBuffer> buffersLayout = createBuffersLayout(fields);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        WriteChannel writeChannel = new WriteChannel(Channels.newChannel(out));
        try {
            return ArrowUtils.serializeArrowRecordBatchMetadata(
                    writeChannel, 1L, nodes, buffersLayout, bodyCompression, 8L);
        } catch (IOException e) {
            throw new FlussRuntimeException("Failed to estimate Arrow metadata size", e);
        }
    }

    public static long estimateArrowBodyLength(VectorSchemaRoot root) {
        long bufferSize = 0;
        for (FieldVector vector : root.getFieldVectors()) {
            for (ArrowBuf buf : vector.getFieldBuffers()) {
                bufferSize += buf.readableBytes();
                bufferSize = DataSizeRoundingUtil.roundUpTo8Multiple(bufferSize);
            }
        }
        return bufferSize;
    }

    // ------------------------------------------------------------------------------------------

    private static List<Field> flattenFields(List<Field> fields) {
        List<Field> allFields = new ArrayList<>();
        for (Field f : fields) {
            allFields.add(f);
            allFields.addAll(flattenFields(f.getChildren()));
        }
        return allFields;
    }

    private static List<ArrowFieldNode> createFieldNodes(List<Field> fields) {
        List<ArrowFieldNode> fieldNodes = new ArrayList<>();
        for (Field ignored : fields) {
            // use dummy values for now, which is ok for just estimating the size
            fieldNodes.add(new ArrowFieldNode(1L, 1L));
        }
        return fieldNodes;
    }

    private static List<ArrowBuffer> createBuffersLayout(List<Field> fields) {
        List<ArrowBuffer> buffers = new ArrayList<>();
        for (Field f : fields) {
            int bufferLayoutCount = TypeLayout.getTypeBufferCount(f.getType());
            for (int i = 0; i < bufferLayoutCount; i++) {
                // use dummy values for now, which is ok for just estimating the size
                buffers.add(new ArrowBuffer(1L, 1L));
            }
        }
        return buffers;
    }

    public static ArrowFieldWriter createArrowFieldWriter(FieldVector vector, DataType dataType) {
        if (vector instanceof TinyIntVector) {
            return new ArrowTinyIntWriter((TinyIntVector) vector);
        } else if (vector instanceof SmallIntVector) {
            return new ArrowSmallIntWriter((SmallIntVector) vector);
        } else if (vector instanceof IntVector) {
            return new ArrowIntWriter((IntVector) vector);
        } else if (vector instanceof BigIntVector) {
            return new ArrowBigIntWriter((BigIntVector) vector);
        } else if (vector instanceof BitVector) {
            return new ArrowBooleanWriter((BitVector) vector);
        } else if (vector instanceof Float4Vector) {
            return new ArrowFloatWriter((Float4Vector) vector);
        } else if (vector instanceof Float8Vector) {
            return new ArrowDoubleWriter((Float8Vector) vector);
        } else if (vector instanceof VarCharVector) {
            return new ArrowVarCharWriter((VarCharVector) vector);
        } else if (vector instanceof FixedSizeBinaryVector) {
            return new ArrowBinaryWriter((FixedSizeBinaryVector) vector);
        } else if (vector instanceof VarBinaryVector) {
            return new ArrowVarBinaryWriter((VarBinaryVector) vector);
        } else if (vector instanceof DecimalVector) {
            DecimalVector decimalVector = (DecimalVector) vector;
            return new ArrowDecimalWriter(
                    decimalVector, getPrecision(decimalVector), decimalVector.getScale());
        } else if (vector instanceof DateDayVector) {
            return new ArrowDateWriter((DateDayVector) vector);
        } else if (vector instanceof TimeSecVector
                || vector instanceof TimeMilliVector
                || vector instanceof TimeMicroVector
                || vector instanceof TimeNanoVector) {
            return new ArrowTimeWriter(vector);
        } else if (vector instanceof TimeStampVector
                && ((ArrowType.Timestamp) vector.getField().getType()).getTimezone() == null) {
            int precision;
            if (dataType instanceof LocalZonedTimestampType) {
                precision = ((LocalZonedTimestampType) dataType).getPrecision();
                return new ArrowTimestampLtzWriter(vector, precision);
            } else {
                precision = ((TimestampType) dataType).getPrecision();
                return new ArrowTimestampNtzWriter(vector, precision);
            }
        } else if (vector instanceof ListVector && dataType instanceof ArrayType) {
            DataType elementType = ((ArrayType) dataType).getElementType();
            FieldVector elementFieldVector = ((ListVector) vector).getDataVector();
            return new ArrowArrayWriter(
                    vector, ArrowUtils.createArrowFieldWriter(elementFieldVector, elementType));
        } else {
            throw new UnsupportedOperationException(
                    String.format(
                            "Unsupported type %s. Map and Row types will be supported in Issue #1973 and #1974.",
                            dataType));
        }
    }

    private static ColumnVector createArrowColumnVector(ValueVector vector, DataType dataType) {
        if (vector instanceof TinyIntVector) {
            return new ArrowTinyIntColumnVector((TinyIntVector) vector);
        } else if (vector instanceof SmallIntVector) {
            return new ArrowSmallIntColumnVector((SmallIntVector) vector);
        } else if (vector instanceof IntVector) {
            return new ArrowIntColumnVector((IntVector) vector);
        } else if (vector instanceof BigIntVector) {
            return new ArrowBigIntColumnVector((BigIntVector) vector);
        } else if (vector instanceof BitVector) {
            return new ArrowBooleanColumnVector((BitVector) vector);
        } else if (vector instanceof Float4Vector) {
            return new ArrowFloatColumnVector((Float4Vector) vector);
        } else if (vector instanceof Float8Vector) {
            return new ArrowDoubleColumnVector((Float8Vector) vector);
        } else if (vector instanceof VarCharVector) {
            return new ArrowVarCharColumnVector((VarCharVector) vector);
        } else if (vector instanceof FixedSizeBinaryVector) {
            return new ArrowBinaryColumnVector((FixedSizeBinaryVector) vector);
        } else if (vector instanceof VarBinaryVector) {
            return new ArrowVarBinaryColumnVector((VarBinaryVector) vector);
        } else if (vector instanceof DecimalVector) {
            return new ArrowDecimalColumnVector((DecimalVector) vector);
        } else if (vector instanceof DateDayVector) {
            return new ArrowDateColumnVector((DateDayVector) vector);
        } else if (vector instanceof TimeSecVector
                || vector instanceof TimeMilliVector
                || vector instanceof TimeMicroVector
                || vector instanceof TimeNanoVector) {
            return new ArrowTimeColumnVector(vector);
        } else if (vector instanceof TimeStampVector
                && ((ArrowType.Timestamp) vector.getField().getType()).getTimezone() == null) {
            if (dataType instanceof LocalZonedTimestampType) {
                return new ArrowTimestampLtzColumnVector(vector);
            } else {
                return new ArrowTimestampNtzColumnVector(vector);
            }
        } else if (vector instanceof ListVector && dataType instanceof ArrayType) {
            DataType elementType = ((ArrayType) dataType).getElementType();
            ListVector listVector = (ListVector) vector;
            return new ArrowArrayColumnVector(
                    listVector,
                    ArrowUtils.createArrowColumnVector(listVector.getDataVector(), elementType));
        } else {
            throw new UnsupportedOperationException(
                    String.format(
                            "Unsupported type %s. Map and Row types will be supported in Issue #1973 and #1974.",
                            dataType));
        }
    }

    private static Field toArrowField(String fieldName, DataType logicalType) {
        FieldType fieldType =
                new FieldType(
                        logicalType.isNullable(),
                        logicalType.accept(DataTypeToArrowTypeConverter.INSTANCE),
                        null);
        List<Field> children = null;
        if (logicalType instanceof ArrayType) {
            children =
                    Collections.singletonList(
                            toArrowField("element", ((ArrayType) logicalType).getElementType()));
        } else if (logicalType instanceof RowType) {
            RowType rowType = (RowType) logicalType;
            children = new ArrayList<>(rowType.getFieldCount());
            for (DataField field : rowType.getFields()) {
                children.add(toArrowField(field.getName(), field.getType()));
            }
        } else if (logicalType instanceof MapType) {
            MapType mapType = (MapType) logicalType;
            Preconditions.checkArgument(
                    !mapType.getKeyType().isNullable(), "Map key type should be non-nullable");
            children =
                    Collections.singletonList(
                            new Field(
                                    "items",
                                    new FieldType(false, ArrowType.Struct.INSTANCE, null),
                                    Arrays.asList(
                                            toArrowField("key", mapType.getKeyType()),
                                            toArrowField("value", mapType.getValueType()))));
        }
        return new Field(fieldName, fieldType, children);
    }

    private static class DataTypeToArrowTypeConverter extends DataTypeDefaultVisitor<ArrowType> {

        private static final DataTypeToArrowTypeConverter INSTANCE =
                new DataTypeToArrowTypeConverter();

        @Override
        public ArrowType visit(TinyIntType tinyIntType) {
            return new ArrowType.Int(8, true);
        }

        @Override
        public ArrowType visit(SmallIntType smallIntType) {
            return new ArrowType.Int(2 * 8, true);
        }

        @Override
        public ArrowType visit(IntType intType) {
            return new ArrowType.Int(4 * 8, true);
        }

        @Override
        public ArrowType visit(BigIntType bigIntType) {
            return new ArrowType.Int(8 * 8, true);
        }

        @Override
        public ArrowType visit(BooleanType booleanType) {
            return ArrowType.Bool.INSTANCE;
        }

        @Override
        public ArrowType visit(FloatType floatType) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
        }

        @Override
        public ArrowType visit(DoubleType doubleType) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        }

        @Override
        public ArrowType visit(CharType varCharType) {
            return ArrowType.Utf8.INSTANCE;
        }

        @Override
        public ArrowType visit(StringType stringType) {
            return ArrowType.Utf8.INSTANCE;
        }

        @Override
        public ArrowType visit(BinaryType binaryType) {
            return new ArrowType.FixedSizeBinary(binaryType.getLength());
        }

        @Override
        public ArrowType visit(BytesType bytesType) {
            return ArrowType.Binary.INSTANCE;
        }

        @Override
        public ArrowType visit(DecimalType decimalType) {
            return new ArrowType.Decimal(decimalType.getPrecision(), decimalType.getScale());
        }

        @Override
        public ArrowType visit(DateType dateType) {
            return new ArrowType.Date(DateUnit.DAY);
        }

        @Override
        public ArrowType visit(TimeType timeType) {
            if (timeType.getPrecision() == 0) {
                return new ArrowType.Time(TimeUnit.SECOND, 32);
            } else if (timeType.getPrecision() >= 1 && timeType.getPrecision() <= 3) {
                return new ArrowType.Time(TimeUnit.MILLISECOND, 32);
            } else if (timeType.getPrecision() >= 4 && timeType.getPrecision() <= 6) {
                return new ArrowType.Time(TimeUnit.MICROSECOND, 64);
            } else {
                return new ArrowType.Time(TimeUnit.NANOSECOND, 64);
            }
        }

        @Override
        public ArrowType visit(LocalZonedTimestampType localZonedTimestampType) {
            if (localZonedTimestampType.getPrecision() == 0) {
                return new ArrowType.Timestamp(TimeUnit.SECOND, null);
            } else if (localZonedTimestampType.getPrecision() >= 1
                    && localZonedTimestampType.getPrecision() <= 3) {
                return new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
            } else if (localZonedTimestampType.getPrecision() >= 4
                    && localZonedTimestampType.getPrecision() <= 6) {
                return new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
            } else {
                return new ArrowType.Timestamp(TimeUnit.NANOSECOND, null);
            }
        }

        @Override
        public ArrowType visit(TimestampType timestampType) {
            if (timestampType.getPrecision() == 0) {
                return new ArrowType.Timestamp(TimeUnit.SECOND, null);
            } else if (timestampType.getPrecision() >= 1 && timestampType.getPrecision() <= 3) {
                return new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
            } else if (timestampType.getPrecision() >= 4 && timestampType.getPrecision() <= 6) {
                return new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
            } else {
                return new ArrowType.Timestamp(TimeUnit.NANOSECOND, null);
            }
        }

        @Override
        public ArrowType visit(ArrayType arrayType) {
            return Types.MinorType.LIST.getType();
        }

        @Override
        public ArrowType visit(MapType mapType) {
            return new ArrowType.Map(false);
        }

        @Override
        public ArrowType visit(RowType rowType) {
            return ArrowType.Struct.INSTANCE;
        }

        @Override
        protected ArrowType defaultMethod(DataType dataType) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Unsupported data type %s currently.", dataType.asSummaryString()));
        }
    }

    private static int getPrecision(DecimalVector decimalVector) {
        int precision = -1;
        try {
            java.lang.reflect.Field precisionField =
                    decimalVector.getClass().getDeclaredField("precision");
            precisionField.setAccessible(true);
            precision = (int) precisionField.get(decimalVector);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            // should not happen, ignore
        }
        return precision;
    }
}
