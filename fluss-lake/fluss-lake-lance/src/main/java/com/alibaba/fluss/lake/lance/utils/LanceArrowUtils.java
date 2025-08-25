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

package com.alibaba.fluss.lake.lance.utils;

import com.alibaba.fluss.lake.lance.writers.ArrowBigIntWriter;
import com.alibaba.fluss.lake.lance.writers.ArrowBinaryWriter;
import com.alibaba.fluss.lake.lance.writers.ArrowBooleanWriter;
import com.alibaba.fluss.lake.lance.writers.ArrowDateWriter;
import com.alibaba.fluss.lake.lance.writers.ArrowDecimalWriter;
import com.alibaba.fluss.lake.lance.writers.ArrowDoubleWriter;
import com.alibaba.fluss.lake.lance.writers.ArrowFieldWriter;
import com.alibaba.fluss.lake.lance.writers.ArrowFloatWriter;
import com.alibaba.fluss.lake.lance.writers.ArrowIntWriter;
import com.alibaba.fluss.lake.lance.writers.ArrowSmallIntWriter;
import com.alibaba.fluss.lake.lance.writers.ArrowTimeWriter;
import com.alibaba.fluss.lake.lance.writers.ArrowTimestampLtzWriter;
import com.alibaba.fluss.lake.lance.writers.ArrowTimestampNtzWriter;
import com.alibaba.fluss.lake.lance.writers.ArrowTinyIntWriter;
import com.alibaba.fluss.lake.lance.writers.ArrowVarBinaryWriter;
import com.alibaba.fluss.lake.lance.writers.ArrowVarCharWriter;
import com.alibaba.fluss.row.InternalRow;
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

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;
import java.util.stream.Collectors;

/** Utilities for Arrow. */
public class LanceArrowUtils {
    /** Returns the Arrow schema of the specified type. */
    public static Schema toArrowSchema(RowType rowType) {
        List<Field> fields =
                rowType.getFields().stream()
                        .map(f -> toArrowField(f.getName(), f.getType()))
                        .collect(Collectors.toList());
        return new Schema(fields);
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
            return ArrowType.Decimal.createDecimal(
                    decimalType.getPrecision(), decimalType.getScale(), null);
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
        return decimalVector.getPrecision();
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
}
