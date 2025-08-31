/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.connector.spark.utils;

import com.alibaba.fluss.types.BigIntType;
import com.alibaba.fluss.types.BinaryType;
import com.alibaba.fluss.types.BooleanType;
import com.alibaba.fluss.types.BytesType;
import com.alibaba.fluss.types.CharType;
import com.alibaba.fluss.types.DataField;
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

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.UserDefinedType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/** Utils for spark {@link DataType}. */
public class SparkTypeUtils {

    private SparkTypeUtils() {}

    public static StructType fromFlussRowType(RowType type) {
        return (StructType) fromFlussType(type);
    }

    public static DataType fromFlussType(com.alibaba.fluss.types.DataType type) {
        return type.accept(FlussToSparkTypeVisitor.INSTANCE);
    }

    public static com.alibaba.fluss.types.DataType toFlussType(DataType dataType) {
        return SparkToFlussTypeVisitor.visit(dataType);
    }

    private static class FlussToSparkTypeVisitor extends DataTypeDefaultVisitor<DataType> {

        private static final FlussToSparkTypeVisitor INSTANCE = new FlussToSparkTypeVisitor();

        @Override
        public DataType visit(CharType charType) {
            return new org.apache.spark.sql.types.CharType(charType.getLength());
        }

        @Override
        public DataType visit(StringType stringType) {
            return DataTypes.StringType;
        }

        @Override
        public DataType visit(BooleanType booleanType) {
            return DataTypes.BooleanType;
        }

        @Override
        public DataType visit(BinaryType binaryType) {
            return DataTypes.BinaryType;
        }

        @Override
        public DataType visit(DecimalType decimalType) {
            return DataTypes.createDecimalType(decimalType.getPrecision(), decimalType.getScale());
        }

        @Override
        public DataType visit(TinyIntType tinyIntType) {
            return DataTypes.ByteType;
        }

        @Override
        public DataType visit(SmallIntType smallIntType) {
            return DataTypes.ShortType;
        }

        @Override
        public DataType visit(IntType intType) {
            return DataTypes.IntegerType;
        }

        @Override
        public DataType visit(FloatType floatType) {
            return DataTypes.FloatType;
        }

        @Override
        public DataType visit(DoubleType doubleType) {
            return DataTypes.DoubleType;
        }

        @Override
        public DataType visit(DateType dateType) {
            return DataTypes.DateType;
        }

        @Override
        public DataType visit(TimestampType timestampType) {
            return DataTypes.TimestampNTZType;
        }

        @Override
        public DataType visit(LocalZonedTimestampType localZonedTimestampType) {
            return DataTypes.TimestampType;
        }

        /**
         * For simplicity, as a temporary solution, we directly convert the non-null attribute to
         * nullable on the Spark side.
         */
        @Override
        public DataType visit(RowType rowType) {
            List<StructField> fields = new ArrayList<>(rowType.getFieldCount());
            for (DataField field : rowType.getFields()) {
                StructField structField =
                        DataTypes.createStructField(
                                field.getName(), field.getType().accept(this), true);
                structField =
                        field.getDescription().map(structField::withComment).orElse(structField);

                fields.add(structField);
            }
            return DataTypes.createStructType(fields);
        }

        @Override
        public DataType visit(BigIntType bigIntType) {
            return DataTypes.LongType;
        }

        @Override
        public DataType visit(TimeType timeType) {
            return DataTypes.IntegerType;
        }

        @Override
        public DataType visit(BytesType bytesType) {
            return DataTypes.BinaryType;
        }

        @Override
        protected DataType defaultMethod(com.alibaba.fluss.types.DataType dataType) {
            throw new UnsupportedOperationException("Unsupported type: " + dataType);
        }
    }

    private static class SparkToFlussTypeVisitor {

        static com.alibaba.fluss.types.DataType visit(DataType type) {
            return visit(type, new SparkToFlussTypeVisitor());
        }

        static com.alibaba.fluss.types.DataType visit(
                DataType type, SparkToFlussTypeVisitor visitor) {
            if (type instanceof StructType) {
                StructField[] fields = ((StructType) type).fields();
                List<com.alibaba.fluss.types.DataType> fieldResults =
                        new ArrayList<>(fields.length);

                for (StructField field : fields) {
                    fieldResults.add(visit(field.dataType(), visitor));
                }

                return visitor.struct((StructType) type, fieldResults);

            } else if (type instanceof UserDefinedType) {
                throw new UnsupportedOperationException("User-defined types are not supported");

            } else {
                return visitor.atomic(type);
            }
        }

        public com.alibaba.fluss.types.DataType struct(
                StructType struct, List<com.alibaba.fluss.types.DataType> fieldResults) {
            StructField[] fields = struct.fields();
            List<DataField> newFields = new ArrayList<>(fields.length);
            for (int i = 0; i < fields.length; i += 1) {
                StructField field = fields[i];
                com.alibaba.fluss.types.DataType fieldType =
                        fieldResults.get(i).copy(field.nullable());
                String comment = field.getComment().getOrElse(() -> null);
                newFields.add(new DataField(field.name(), fieldType, comment));
            }

            return new RowType(newFields);
        }

        public com.alibaba.fluss.types.DataType atomic(DataType atomic) {
            if (atomic instanceof org.apache.spark.sql.types.BooleanType) {
                return new BooleanType();
            } else if (atomic instanceof org.apache.spark.sql.types.ByteType) {
                return new TinyIntType();
            } else if (atomic instanceof org.apache.spark.sql.types.ShortType) {
                return new SmallIntType();
            } else if (atomic instanceof org.apache.spark.sql.types.IntegerType) {
                return new IntType();
            } else if (atomic instanceof LongType) {
                return new BigIntType();
            } else if (atomic instanceof org.apache.spark.sql.types.FloatType) {
                return new FloatType();
            } else if (atomic instanceof org.apache.spark.sql.types.DoubleType) {
                return new DoubleType();
            } else if (atomic instanceof org.apache.spark.sql.types.StringType) {
                return new StringType();
            } else if (atomic instanceof org.apache.spark.sql.types.CharType) {
                return new CharType(((org.apache.spark.sql.types.CharType) atomic).length());
            } else if (atomic instanceof org.apache.spark.sql.types.DateType) {
                return new DateType();
            } else if (atomic instanceof org.apache.spark.sql.types.TimestampType) {
                // spark only support 6 digits of precision
                return new LocalZonedTimestampType(6);
            } else if (atomic instanceof org.apache.spark.sql.types.TimestampNTZType) {
                // spark only support 6 digits of precision
                return new TimestampType(6);
            } else if (atomic instanceof org.apache.spark.sql.types.DecimalType) {
                return new DecimalType(
                        ((org.apache.spark.sql.types.DecimalType) atomic).precision(),
                        ((org.apache.spark.sql.types.DecimalType) atomic).scale());
            } else if (atomic instanceof org.apache.spark.sql.types.BinaryType) {
                return new BytesType();
            }

            throw new UnsupportedOperationException(
                    "Not a supported type: " + atomic.catalogString());
        }
    }

    public static RowType project(RowType inputType, int[] mapping) {
        List<DataField> fields = inputType.getFields();
        return new RowType(
                Arrays.stream(mapping).mapToObj(fields::get).collect(Collectors.toList()));
    }

    public static RowType project(RowType inputType, List<String> names) {
        List<DataField> fields = inputType.getFields();
        List<String> fieldNames =
                fields.stream().map(DataField::getName).collect(Collectors.toList());
        return new RowType(
                names.stream()
                        .map(k -> fields.get(fieldNames.indexOf(k)))
                        .collect(Collectors.toList()));
    }
}
