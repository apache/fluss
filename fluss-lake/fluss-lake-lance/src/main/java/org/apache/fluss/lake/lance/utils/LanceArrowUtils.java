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

package org.apache.fluss.lake.lance.utils;

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
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.SmallIntType;
import org.apache.fluss.types.StringType;
import org.apache.fluss.types.TimeType;
import org.apache.fluss.types.TimestampType;
import org.apache.fluss.types.TinyIntType;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Utilities for converting Fluss RowType to non-shaded Arrow Schema. This is needed because Lance
 * requires non-shaded Arrow API.
 */
public class LanceArrowUtils {

    /** Property suffix for configuring a fixed-size list Arrow type on array columns. */
    public static final String FIXED_SIZE_LIST_SIZE_SUFFIX = ".arrow.fixed-size-list.size";

    /** Returns the non-shaded Arrow schema of the specified Fluss RowType. */
    public static Schema toArrowSchema(RowType rowType) {
        return toArrowSchema(rowType, Collections.emptyMap());
    }

    /**
     * Returns the non-shaded Arrow schema of the specified Fluss RowType, using table properties to
     * determine whether array columns should use FixedSizeList instead of List.
     *
     * <p>When a table property {@code <column>.arrow.fixed-size-list.size} is set, the
     * corresponding ARRAY column will be emitted as {@code FixedSizeList<element>(size)} instead of
     * {@code List<element>}.
     */
    public static Schema toArrowSchema(RowType rowType, Map<String, String> tableProperties) {
        List<Field> fields =
                rowType.getFields().stream()
                        .map(f -> toArrowField(f.getName(), f.getType(), tableProperties))
                        .collect(Collectors.toList());
        return new Schema(fields);
    }

    private static Field toArrowField(
            String fieldName, DataType logicalType, Map<String, String> tableProperties) {
        ArrowType arrowType;
        if (logicalType instanceof ArrayType && tableProperties != null) {
            String sizeStr = tableProperties.get(fieldName + FIXED_SIZE_LIST_SIZE_SUFFIX);
            if (sizeStr != null) {
                int listSize = -1;
                try {
                    listSize = Integer.parseInt(sizeStr);
                } catch (NumberFormatException ignored) {
                    // Not really ignored, IllegalArgumentEx still thrown below.
                    // This removes duplicate boilerplates for throwing IAE
                }

                checkArgument(
                        listSize > 0,
                        "Invalid value '%s' for property '%s'. Expected a positive integer.",
                        sizeStr,
                        fieldName + FIXED_SIZE_LIST_SIZE_SUFFIX);
                arrowType = new ArrowType.FixedSizeList(listSize);
            } else {
                arrowType = toArrowType(logicalType);
            }
        } else {
            arrowType = toArrowType(logicalType);
        }
        FieldType fieldType = new FieldType(logicalType.isNullable(), arrowType, null);
        List<Field> children = null;
        if (logicalType instanceof ArrayType) {
            children =
                    Collections.singletonList(
                            toArrowField(
                                    "element",
                                    ((ArrayType) logicalType).getElementType(),
                                    tableProperties));
        }
        return new Field(fieldName, fieldType, children);
    }

    private static ArrowType toArrowType(DataType dataType) {
        if (dataType instanceof TinyIntType) {
            return new ArrowType.Int(8, true);
        } else if (dataType instanceof SmallIntType) {
            return new ArrowType.Int(16, true);
        } else if (dataType instanceof IntType) {
            return new ArrowType.Int(32, true);
        } else if (dataType instanceof BigIntType) {
            return new ArrowType.Int(64, true);
        } else if (dataType instanceof BooleanType) {
            return ArrowType.Bool.INSTANCE;
        } else if (dataType instanceof FloatType) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
        } else if (dataType instanceof DoubleType) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        } else if (dataType instanceof CharType || dataType instanceof StringType) {
            return ArrowType.Utf8.INSTANCE;
        } else if (dataType instanceof BinaryType) {
            BinaryType binaryType = (BinaryType) dataType;
            return new ArrowType.FixedSizeBinary(binaryType.getLength());
        } else if (dataType instanceof BytesType) {
            return ArrowType.Binary.INSTANCE;
        } else if (dataType instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) dataType;
            return ArrowType.Decimal.createDecimal(
                    decimalType.getPrecision(), decimalType.getScale(), null);
        } else if (dataType instanceof DateType) {
            return new ArrowType.Date(DateUnit.DAY);
        } else if (dataType instanceof TimeType) {
            TimeType timeType = (TimeType) dataType;
            if (timeType.getPrecision() == 0) {
                return new ArrowType.Time(TimeUnit.SECOND, 32);
            } else if (timeType.getPrecision() >= 1 && timeType.getPrecision() <= 3) {
                return new ArrowType.Time(TimeUnit.MILLISECOND, 32);
            } else if (timeType.getPrecision() >= 4 && timeType.getPrecision() <= 6) {
                return new ArrowType.Time(TimeUnit.MICROSECOND, 64);
            } else {
                return new ArrowType.Time(TimeUnit.NANOSECOND, 64);
            }
        } else if (dataType instanceof LocalZonedTimestampType) {
            LocalZonedTimestampType timestampType = (LocalZonedTimestampType) dataType;
            if (timestampType.getPrecision() == 0) {
                return new ArrowType.Timestamp(TimeUnit.SECOND, null);
            } else if (timestampType.getPrecision() >= 1 && timestampType.getPrecision() <= 3) {
                return new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
            } else if (timestampType.getPrecision() >= 4 && timestampType.getPrecision() <= 6) {
                return new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
            } else {
                return new ArrowType.Timestamp(TimeUnit.NANOSECOND, null);
            }
        } else if (dataType instanceof TimestampType) {
            TimestampType timestampType = (TimestampType) dataType;
            if (timestampType.getPrecision() == 0) {
                return new ArrowType.Timestamp(TimeUnit.SECOND, null);
            } else if (timestampType.getPrecision() >= 1 && timestampType.getPrecision() <= 3) {
                return new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
            } else if (timestampType.getPrecision() >= 4 && timestampType.getPrecision() <= 6) {
                return new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
            } else {
                return new ArrowType.Timestamp(TimeUnit.NANOSECOND, null);
            }
        } else if (dataType instanceof ArrayType) {
            return ArrowType.List.INSTANCE;
        } else {
            throw new UnsupportedOperationException(
                    String.format(
                            "Unsupported data type %s currently.", dataType.asSummaryString()));
        }
    }
}
