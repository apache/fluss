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

package org.apache.fluss.lake.iceberg.tiering.writer;

import org.apache.fluss.record.ArrowBatchData;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.BigIntType;
import org.apache.fluss.types.BinaryType;
import org.apache.fluss.types.BooleanType;
import org.apache.fluss.types.BytesType;
import org.apache.fluss.types.CharType;
import org.apache.fluss.types.DataType;
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

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

/**
 * Helper class that reads Arrow vectors and writes Iceberg records through the existing TaskWriter.
 * This avoids the per-record LogRecord deserialization overhead by reading directly from Arrow
 * columnar memory.
 *
 * <p>This class is lazily loaded to avoid classloading Arrow classes when Arrow is not on the
 * classpath.
 */
public class IcebergArrowBatchHelper implements AutoCloseable {

    private final TaskWriter<Record> taskWriter;
    private final Schema icebergSchema;
    private final RowType flussRowType;
    private final int bucket;

    public IcebergArrowBatchHelper(
            TaskWriter<Record> taskWriter, Schema icebergSchema, RowType flussRowType, int bucket) {
        this.taskWriter = taskWriter;
        this.icebergSchema = icebergSchema;
        this.flussRowType = flussRowType;
        this.bucket = bucket;
    }

    /**
     * Writes an Arrow batch to Iceberg by reading vectors directly and producing GenericRecord
     * objects. System columns (__bucket, __offset, __timestamp) are computed from batch metadata.
     */
    public void writeArrowBatch(ArrowBatchData arrowBatchData) throws IOException {
        VectorSchemaRoot root = arrowBatchData.getVectorSchemaRoot();
        int rowCount = root.getRowCount();
        long baseOffset = arrowBatchData.getBaseLogOffset();
        long timestamp = arrowBatchData.getTimestamp();

        List<Types.NestedField> icebergFields = icebergSchema.columns();
        int userFieldCount = flussRowType.getFieldCount();

        // Pre-fetch vectors for user columns
        FieldVector[] vectors = new FieldVector[userFieldCount];
        for (int col = 0; col < userFieldCount; col++) {
            vectors[col] = root.getVector(col);
        }

        // Create field extractors based on Fluss types
        ArrowFieldExtractor[] extractors = new ArrowFieldExtractor[userFieldCount];
        for (int col = 0; col < userFieldCount; col++) {
            extractors[col] = createExtractor(flussRowType.getTypeAt(col), vectors[col]);
        }

        // Write records batch-style: iterate rows, build GenericRecord from vectors
        for (int row = 0; row < rowCount; row++) {
            GenericRecord record = GenericRecord.create(icebergSchema);

            // Set user columns from Arrow vectors
            for (int col = 0; col < userFieldCount; col++) {
                if (vectors[col].isNull(row)) {
                    record.set(col, null);
                } else {
                    record.set(col, extractors[col].extract(row));
                }
            }

            // Set system columns
            record.set(userFieldCount, bucket); // __bucket
            record.set(userFieldCount + 1, baseOffset + row); // __offset
            record.set(
                    userFieldCount + 2,
                    OffsetDateTime.ofInstant(
                            Instant.ofEpochMilli(timestamp), ZoneOffset.UTC)); // __timestamp

            taskWriter.write(record);
        }
    }

    @Override
    public void close() {
        // Nothing to close; the taskWriter is managed by the caller
    }

    // --- Arrow field extractors ---

    @FunctionalInterface
    private interface ArrowFieldExtractor {
        Object extract(int rowIndex);
    }

    private ArrowFieldExtractor createExtractor(DataType flussType, FieldVector vector) {
        if (flussType instanceof BooleanType) {
            BitVector v = (BitVector) vector;
            return row -> v.get(row) == 1;
        } else if (flussType instanceof TinyIntType) {
            TinyIntVector v = (TinyIntVector) vector;
            return row -> (int) v.get(row);
        } else if (flussType instanceof SmallIntType) {
            SmallIntVector v = (SmallIntVector) vector;
            return row -> (int) v.get(row);
        } else if (flussType instanceof IntType) {
            IntVector v = (IntVector) vector;
            return row -> v.get(row);
        } else if (flussType instanceof BigIntType) {
            BigIntVector v = (BigIntVector) vector;
            return row -> v.get(row);
        } else if (flussType instanceof FloatType) {
            Float4Vector v = (Float4Vector) vector;
            return row -> v.get(row);
        } else if (flussType instanceof DoubleType) {
            Float8Vector v = (Float8Vector) vector;
            return row -> v.get(row);
        } else if (flussType instanceof StringType || flussType instanceof CharType) {
            VarCharVector v = (VarCharVector) vector;
            return row -> new String(v.get(row), java.nio.charset.StandardCharsets.UTF_8);
        } else if (flussType instanceof BytesType || flussType instanceof BinaryType) {
            VarBinaryVector v = (VarBinaryVector) vector;
            return row -> ByteBuffer.wrap(v.get(row));
        } else if (flussType instanceof DecimalType) {
            DecimalVector v = (DecimalVector) vector;
            return row -> {
                BigDecimal decimal = v.getObject(row);
                return decimal;
            };
        } else if (flussType instanceof DateType) {
            DateDayVector v = (DateDayVector) vector;
            return row -> LocalDate.ofEpochDay(v.get(row));
        } else if (flussType instanceof TimeType) {
            // Fluss stores time as millis-of-day in INT
            if (vector instanceof TimeMilliVector) {
                TimeMilliVector v = (TimeMilliVector) vector;
                return row -> LocalTime.ofNanoOfDay((long) v.get(row) * 1_000_000L);
            } else {
                TimeMicroVector v = (TimeMicroVector) vector;
                return row -> LocalTime.ofNanoOfDay(v.get(row) * 1_000L);
            }
        } else if (flussType instanceof TimestampType) {
            TimestampType tsType = (TimestampType) flussType;
            if (tsType.getPrecision() <= 3) {
                TimeStampMilliVector v = (TimeStampMilliVector) vector;
                return row -> {
                    long millis = v.get(row);
                    return LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC);
                };
            } else {
                TimeStampMicroVector v = (TimeStampMicroVector) vector;
                return row -> {
                    long micros = v.get(row);
                    long seconds = micros / 1_000_000;
                    int nanos = (int) ((micros % 1_000_000) * 1_000);
                    return LocalDateTime.ofInstant(
                            Instant.ofEpochSecond(seconds, nanos), ZoneOffset.UTC);
                };
            }
        } else if (flussType instanceof LocalZonedTimestampType) {
            LocalZonedTimestampType ltzType = (LocalZonedTimestampType) flussType;
            if (ltzType.getPrecision() <= 3) {
                TimeStampMilliVector v = (TimeStampMilliVector) vector;
                return row -> {
                    long millis = v.get(row);
                    return OffsetDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC);
                };
            } else {
                TimeStampMicroVector v = (TimeStampMicroVector) vector;
                return row -> {
                    long micros = v.get(row);
                    long seconds = micros / 1_000_000;
                    int nanos = (int) ((micros % 1_000_000) * 1_000);
                    return OffsetDateTime.ofInstant(
                            Instant.ofEpochSecond(seconds, nanos), ZoneOffset.UTC);
                };
            }
        } else if (flussType instanceof ArrayType
                || flussType instanceof MapType
                || flussType instanceof RowType) {
            // Complex types: fall back to null for now.
            // TODO: implement nested type extraction from Arrow complex vectors
            return row -> null;
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported Arrow field extraction for type: "
                            + flussType.getClass().getSimpleName());
        }
    }
}
