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

package com.alibaba.fluss.utils;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.compression.ArrowCompressionFactory;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.arrow.ArrowReader;
import com.alibaba.fluss.row.arrow.vectors.ArrowBigIntColumnVector;
import com.alibaba.fluss.row.arrow.vectors.ArrowBinaryColumnVector;
import com.alibaba.fluss.row.arrow.vectors.ArrowBooleanColumnVector;
import com.alibaba.fluss.row.arrow.vectors.ArrowDateColumnVector;
import com.alibaba.fluss.row.arrow.vectors.ArrowDecimalColumnVector;
import com.alibaba.fluss.row.arrow.vectors.ArrowDoubleColumnVector;
import com.alibaba.fluss.row.arrow.vectors.ArrowFloatColumnVector;
import com.alibaba.fluss.row.arrow.vectors.ArrowIntColumnVector;
import com.alibaba.fluss.row.arrow.vectors.ArrowSmallIntColumnVector;
import com.alibaba.fluss.row.arrow.vectors.ArrowTimeColumnVector;
import com.alibaba.fluss.row.arrow.vectors.ArrowTimestampLtzColumnVector;
import com.alibaba.fluss.row.arrow.vectors.ArrowTimestampNtzColumnVector;
import com.alibaba.fluss.row.arrow.vectors.ArrowTinyIntColumnVector;
import com.alibaba.fluss.row.arrow.vectors.ArrowVarBinaryColumnVector;
import com.alibaba.fluss.row.arrow.vectors.ArrowVarCharColumnVector;
import com.alibaba.fluss.row.arrow.writers.ArrowBigIntWriter;
import com.alibaba.fluss.row.arrow.writers.ArrowBinaryWriter;
import com.alibaba.fluss.row.arrow.writers.ArrowBooleanWriter;
import com.alibaba.fluss.row.arrow.writers.ArrowDateWriter;
import com.alibaba.fluss.row.arrow.writers.ArrowDecimalWriter;
import com.alibaba.fluss.row.arrow.writers.ArrowDoubleWriter;
import com.alibaba.fluss.row.arrow.writers.ArrowFieldWriter;
import com.alibaba.fluss.row.arrow.writers.ArrowFloatWriter;
import com.alibaba.fluss.row.arrow.writers.ArrowIntWriter;
import com.alibaba.fluss.row.arrow.writers.ArrowSmallIntWriter;
import com.alibaba.fluss.row.arrow.writers.ArrowTimeWriter;
import com.alibaba.fluss.row.arrow.writers.ArrowTimestampLtzWriter;
import com.alibaba.fluss.row.arrow.writers.ArrowTimestampNtzWriter;
import com.alibaba.fluss.row.arrow.writers.ArrowTinyIntWriter;
import com.alibaba.fluss.row.arrow.writers.ArrowVarBinaryWriter;
import com.alibaba.fluss.row.arrow.writers.ArrowVarCharWriter;
import com.alibaba.fluss.row.columnar.ColumnVector;
import com.alibaba.fluss.types.BigIntType;
import com.alibaba.fluss.types.BinaryType;
import com.alibaba.fluss.types.BooleanType;
import com.alibaba.fluss.types.BytesType;
import com.alibaba.fluss.types.CharType;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DataTypeDefaultVisitor;
import com.alibaba.fluss.types.DateType;
import com.alibaba.fluss.types.DecimalType;
import com.alibaba.fluss.types.DoubleType;
import com.alibaba.fluss.types.FloatType;
import com.alibaba.fluss.types.IntType;
import com.alibaba.fluss.types.LocalZonedTimestampType;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.types.SmallIntType;
import com.alibaba.fluss.types.StringType;
import com.alibaba.fluss.types.TimeType;
import com.alibaba.fluss.types.TimestampType;
import com.alibaba.fluss.types.TinyIntType;

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
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.Field;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.types.pojo.Schema;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.util.DataSizeRoundingUtil;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.MessageSerializer.deserializeRecordBatch;

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

    public static ArrowFieldWriter<InternalRow> createArrowFieldWriter(
            ValueVector vector, DataType dataType) {
        if (vector instanceof TinyIntVector) {
            return ArrowTinyIntWriter.forField((TinyIntVector) vector);
        } else if (vector instanceof SmallIntVector) {
            return ArrowSmallIntWriter.forField((SmallIntVector) vector);
        } else if (vector instanceof IntVector) {
            return ArrowIntWriter.forField((IntVector) vector);
        } else if (vector instanceof BigIntVector) {
            return ArrowBigIntWriter.forField((BigIntVector) vector);
        } else if (vector instanceof BitVector) {
            return ArrowBooleanWriter.forField((BitVector) vector);
        } else if (vector instanceof Float4Vector) {
            return ArrowFloatWriter.forField((Float4Vector) vector);
        } else if (vector instanceof Float8Vector) {
            return ArrowDoubleWriter.forField((Float8Vector) vector);
        } else if (vector instanceof VarCharVector) {
            return ArrowVarCharWriter.forField((VarCharVector) vector);
        } else if (vector instanceof FixedSizeBinaryVector) {
            return ArrowBinaryWriter.forField((FixedSizeBinaryVector) vector);
        } else if (vector instanceof VarBinaryVector) {
            return ArrowVarBinaryWriter.forField((VarBinaryVector) vector);
        } else if (vector instanceof DecimalVector) {
            DecimalVector decimalVector = (DecimalVector) vector;
            return ArrowDecimalWriter.forField(
                    decimalVector, getPrecision(decimalVector), decimalVector.getScale());
        } else if (vector instanceof DateDayVector) {
            return ArrowDateWriter.forField((DateDayVector) vector);
        } else if (vector instanceof TimeSecVector
                || vector instanceof TimeMilliVector
                || vector instanceof TimeMicroVector
                || vector instanceof TimeNanoVector) {
            return ArrowTimeWriter.forField(vector);
        } else if (vector instanceof TimeStampVector
                && ((ArrowType.Timestamp) vector.getField().getType()).getTimezone() == null) {
            int precision;
            if (dataType instanceof LocalZonedTimestampType) {
                precision = ((LocalZonedTimestampType) dataType).getPrecision();
                return ArrowTimestampLtzWriter.forField(vector, precision);
            } else {
                precision = ((TimestampType) dataType).getPrecision();
                return ArrowTimestampNtzWriter.forField(vector, precision);
            }
        } else {
            throw new UnsupportedOperationException(
                    String.format("Unsupported type %s.", dataType));
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
        } else {
            throw new UnsupportedOperationException(
                    String.format("Unsupported type %s.", dataType));
        }
    }

    private static Field toArrowField(String fieldName, DataType logicalType) {
        FieldType fieldType =
                new FieldType(
                        logicalType.isNullable(),
                        logicalType.accept(DataTypeToArrowTypeConverter.INSTANCE),
                        null);
        return new Field(fieldName, fieldType, null);
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
