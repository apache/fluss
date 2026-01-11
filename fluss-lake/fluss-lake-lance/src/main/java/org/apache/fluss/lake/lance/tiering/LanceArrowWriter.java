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

package org.apache.fluss.lake.lance.tiering;

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.BigIntType;
import org.apache.fluss.types.BinaryType;
import org.apache.fluss.types.BooleanType;
import org.apache.fluss.types.CharType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DateType;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.DoubleType;
import org.apache.fluss.types.FloatType;
import org.apache.fluss.types.IntType;
import org.apache.fluss.types.LocalZonedTimestampType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.SmallIntType;
import org.apache.fluss.types.TimeType;
import org.apache.fluss.types.TimestampType;
import org.apache.fluss.types.TinyIntType;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.math.BigDecimal;

/**
 * Lance Arrow writer that writes Fluss InternalRow to non-shaded Arrow VectorSchemaRoot.
 *
 * <p>This class directly writes to non-shaded Arrow vectors, which are compatible with Lance
 * library, avoiding the shaded/non-shaded type incompatibility issue.
 */
public class LanceArrowWriter {
    private final VectorSchemaRoot root;
    private final FieldWriter[] fieldWriters;
    private int recordsCount;

    private LanceArrowWriter(VectorSchemaRoot root, FieldWriter[] fieldWriters) {
        this.root = root;
        this.fieldWriters = fieldWriters;
        this.recordsCount = 0;
    }

    public static LanceArrowWriter create(VectorSchemaRoot root, RowType rowType) {
        FieldWriter[] fieldWriters = new FieldWriter[root.getFieldVectors().size()];

        for (int i = 0; i < fieldWriters.length; i++) {
            FieldVector vector = root.getVector(i);
            DataType dataType = rowType.getTypeAt(i);
            fieldWriters[i] = createFieldWriter(vector, dataType);
        }

        return new LanceArrowWriter(root, fieldWriters);
    }

    private static FieldWriter createFieldWriter(FieldVector vector, DataType dataType) {
        if (dataType instanceof TinyIntType) {
            return new TinyIntWriter((TinyIntVector) vector);
        } else if (dataType instanceof SmallIntType) {
            return new SmallIntWriter((SmallIntVector) vector);
        } else if (dataType instanceof IntType) {
            return new IntWriter((IntVector) vector);
        } else if (dataType instanceof BigIntType) {
            return new BigIntWriter((BigIntVector) vector);
        } else if (dataType instanceof BooleanType) {
            return new BooleanWriter((BitVector) vector);
        } else if (dataType instanceof FloatType) {
            return new FloatWriter((Float4Vector) vector);
        } else if (dataType instanceof DoubleType) {
            return new DoubleWriter((Float8Vector) vector);
        } else if (dataType instanceof CharType
                || dataType instanceof org.apache.fluss.types.StringType) {
            return new VarCharWriter((VarCharVector) vector);
        } else if (dataType instanceof org.apache.fluss.types.BinaryType
                && ((org.apache.fluss.types.BinaryType) dataType).getLength()
                        == org.apache.fluss.types.BinaryType.MAX_LENGTH) {
            return new VarBinaryWriter((VarBinaryVector) vector);
        } else if (dataType instanceof BinaryType) {
            return new BinaryWriter(
                    (FixedSizeBinaryVector) vector, ((BinaryType) dataType).getLength());
        } else if (dataType instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) dataType;
            return new DecimalWriter(
                    (DecimalVector) vector, decimalType.getPrecision(), decimalType.getScale());
        } else if (dataType instanceof DateType) {
            return new DateWriter((DateDayVector) vector);
        } else if (dataType instanceof TimeType) {
            return new TimeWriter(vector, ((TimeType) dataType).getPrecision());
        } else if (dataType instanceof LocalZonedTimestampType) {
            return new TimestampLtzWriter(
                    vector, ((LocalZonedTimestampType) dataType).getPrecision());
        } else if (dataType instanceof TimestampType) {
            return new TimestampNtzWriter(vector, ((TimestampType) dataType).getPrecision());
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported data type for Arrow conversion: " + dataType);
        }
    }

    public void writeRow(InternalRow row) {
        for (int i = 0; i < fieldWriters.length; i++) {
            if (row.isNullAt(i)) {
                fieldWriters[i].setNull(recordsCount);
            } else {
                fieldWriters[i].write(recordsCount, row, i);
            }
        }
        recordsCount++;
    }

    public void finish() {
        root.setRowCount(recordsCount);
        for (FieldWriter writer : fieldWriters) {
            writer.finish(recordsCount);
        }
    }

    public void reset() {
        recordsCount = 0;
        for (FieldWriter writer : fieldWriters) {
            writer.reset();
        }
    }

    public int getRecordsCount() {
        return recordsCount;
    }

    private abstract static class FieldWriter {
        protected final FieldVector vector;

        FieldWriter(FieldVector vector) {
            this.vector = vector;
        }

        abstract void write(int rowIndex, InternalRow row, int ordinal);

        void setNull(int rowIndex) {
            vector.setNull(rowIndex);
        }

        void finish(int valueCount) {
            vector.setValueCount(valueCount);
        }

        void reset() {
            vector.reset();
        }
    }

    private static class TinyIntWriter extends FieldWriter {
        TinyIntWriter(TinyIntVector vector) {
            super(vector);
        }

        @Override
        void write(int rowIndex, InternalRow row, int ordinal) {
            ((TinyIntVector) vector).setSafe(rowIndex, row.getByte(ordinal));
        }
    }

    private static class SmallIntWriter extends FieldWriter {
        SmallIntWriter(SmallIntVector vector) {
            super(vector);
        }

        @Override
        void write(int rowIndex, InternalRow row, int ordinal) {
            ((SmallIntVector) vector).setSafe(rowIndex, row.getShort(ordinal));
        }
    }

    private static class IntWriter extends FieldWriter {
        IntWriter(IntVector vector) {
            super(vector);
        }

        @Override
        void write(int rowIndex, InternalRow row, int ordinal) {
            ((IntVector) vector).setSafe(rowIndex, row.getInt(ordinal));
        }
    }

    private static class BigIntWriter extends FieldWriter {
        BigIntWriter(BigIntVector vector) {
            super(vector);
        }

        @Override
        void write(int rowIndex, InternalRow row, int ordinal) {
            ((BigIntVector) vector).setSafe(rowIndex, row.getLong(ordinal));
        }
    }

    private static class BooleanWriter extends FieldWriter {
        BooleanWriter(BitVector vector) {
            super(vector);
        }

        @Override
        void write(int rowIndex, InternalRow row, int ordinal) {
            ((BitVector) vector).setSafe(rowIndex, row.getBoolean(ordinal) ? 1 : 0);
        }
    }

    private static class FloatWriter extends FieldWriter {
        FloatWriter(Float4Vector vector) {
            super(vector);
        }

        @Override
        void write(int rowIndex, InternalRow row, int ordinal) {
            ((Float4Vector) vector).setSafe(rowIndex, row.getFloat(ordinal));
        }
    }

    private static class DoubleWriter extends FieldWriter {
        DoubleWriter(Float8Vector vector) {
            super(vector);
        }

        @Override
        void write(int rowIndex, InternalRow row, int ordinal) {
            ((Float8Vector) vector).setSafe(rowIndex, row.getDouble(ordinal));
        }
    }

    private static class VarCharWriter extends FieldWriter {
        VarCharWriter(VarCharVector vector) {
            super(vector);
        }

        @Override
        void write(int rowIndex, InternalRow row, int ordinal) {
            BinaryString str = row.getString(ordinal);
            byte[] bytes = str.toBytes();
            ((VarCharVector) vector).setSafe(rowIndex, bytes);
        }
    }

    private static class VarBinaryWriter extends FieldWriter {
        VarBinaryWriter(VarBinaryVector vector) {
            super(vector);
        }

        @Override
        void write(int rowIndex, InternalRow row, int ordinal) {
            // VarBinary is variable length, use MAX_LENGTH
            byte[] bytes = row.getBinary(ordinal, BinaryType.MAX_LENGTH);
            ((VarBinaryVector) vector).setSafe(rowIndex, bytes);
        }
    }

    private static class BinaryWriter extends FieldWriter {
        private final int byteWidth;

        BinaryWriter(FixedSizeBinaryVector vector, int byteWidth) {
            super(vector);
            this.byteWidth = byteWidth;
        }

        @Override
        void write(int rowIndex, InternalRow row, int ordinal) {
            byte[] bytes = row.getBinary(ordinal, byteWidth);
            ((FixedSizeBinaryVector) vector).setSafe(rowIndex, bytes);
        }
    }

    private static class DecimalWriter extends FieldWriter {
        private final int precision;
        private final int scale;

        DecimalWriter(DecimalVector vector, int precision, int scale) {
            super(vector);
            this.precision = precision;
            this.scale = scale;
        }

        @Override
        void write(int rowIndex, InternalRow row, int ordinal) {
            Decimal decimal = row.getDecimal(ordinal, precision, scale);
            BigDecimal bigDecimal = decimal.toBigDecimal();
            ((DecimalVector) vector).setSafe(rowIndex, bigDecimal);
        }
    }

    private static class DateWriter extends FieldWriter {
        DateWriter(DateDayVector vector) {
            super(vector);
        }

        @Override
        void write(int rowIndex, InternalRow row, int ordinal) {
            int days = row.getInt(ordinal);
            ((DateDayVector) vector).setSafe(rowIndex, days);
        }
    }

    private static class TimeWriter extends FieldWriter {
        TimeWriter(FieldVector vector, int precision) {
            super(vector);
        }

        @Override
        void write(int rowIndex, InternalRow row, int ordinal) {
            int millis = row.getInt(ordinal);

            if (vector instanceof TimeSecVector) {
                ((TimeSecVector) vector).setSafe(rowIndex, millis / 1000);
            } else if (vector instanceof TimeMilliVector) {
                ((TimeMilliVector) vector).setSafe(rowIndex, millis);
            } else if (vector instanceof TimeMicroVector) {
                ((TimeMicroVector) vector).setSafe(rowIndex, millis * 1000L);
            } else if (vector instanceof TimeNanoVector) {
                ((TimeNanoVector) vector).setSafe(rowIndex, millis * 1000000L);
            }
        }
    }

    private static class TimestampLtzWriter extends FieldWriter {
        private final int precision;

        TimestampLtzWriter(FieldVector vector, int precision) {
            super(vector);
            this.precision = precision;
        }

        @Override
        void write(int rowIndex, InternalRow row, int ordinal) {
            TimestampLtz timestamp = row.getTimestampLtz(ordinal, precision);
            long millis = timestamp.getEpochMillisecond();

            if (vector instanceof org.apache.arrow.vector.TimeStampMicroTZVector) {
                ((org.apache.arrow.vector.TimeStampMicroTZVector) vector)
                        .setSafe(rowIndex, millis * 1000);
            } else if (vector instanceof org.apache.arrow.vector.TimeStampNanoTZVector) {
                long nanos = millis * 1000000 + timestamp.getNanoOfMillisecond();
                ((org.apache.arrow.vector.TimeStampNanoTZVector) vector).setSafe(rowIndex, nanos);
            }
        }
    }

    private static class TimestampNtzWriter extends FieldWriter {
        private final int precision;

        TimestampNtzWriter(FieldVector vector, int precision) {
            super(vector);
            this.precision = precision;
        }

        @Override
        void write(int rowIndex, InternalRow row, int ordinal) {
            TimestampNtz timestamp = row.getTimestampNtz(ordinal, precision);
            long millis = timestamp.getMillisecond();

            if (vector instanceof org.apache.arrow.vector.TimeStampMicroVector) {
                ((org.apache.arrow.vector.TimeStampMicroVector) vector)
                        .setSafe(rowIndex, millis * 1000);
            } else if (vector instanceof org.apache.arrow.vector.TimeStampNanoVector) {
                long nanos = millis * 1000000 + timestamp.getNanoOfMillisecond();
                ((org.apache.arrow.vector.TimeStampNanoVector) vector).setSafe(rowIndex, nanos);
            }
        }
    }
}
