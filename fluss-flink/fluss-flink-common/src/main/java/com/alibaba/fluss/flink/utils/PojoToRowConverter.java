/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.flink.utils;

import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DataTypeRoot;
import com.alibaba.fluss.types.DecimalType;
import com.alibaba.fluss.types.RowType;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Utility class for converting Java POJOs to Fluss's {@link InternalRow} format.
 *
 * <p>This utility uses Flink's POJO type information to map fields from POJOs to InternalRow based
 * on a given schema.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * // Create a converter
 * PojoToRowConverter<Order> converter =
 *     new PojoToRowConverter<>(Order.class, rowType);
 *
 * // Convert a POJO to GenericRow
 * Order order = new Order(1001L, 5001L, 10, "123 Mumbai");
 * GenericRow row = converter.convert(order);
 * }</pre>
 *
 * <p>Note: Nested POJO fields are not supported in the current implementation.
 *
 * @param <T> The POJO type to convert
 */
public class PojoToRowConverter<T> {
    private static final Logger LOG = LoggerFactory.getLogger(PojoToRowConverter.class);

    private static final Map<DataTypeRoot, Set<Class<?>>> SUPPORTED_TYPES = new HashMap<>();

    static {
        SUPPORTED_TYPES.put(DataTypeRoot.BOOLEAN, orderedSet(Boolean.class, boolean.class));
        SUPPORTED_TYPES.put(DataTypeRoot.TINYINT, orderedSet(Byte.class, byte.class));
        SUPPORTED_TYPES.put(DataTypeRoot.SMALLINT, orderedSet(Short.class, short.class));
        SUPPORTED_TYPES.put(DataTypeRoot.INTEGER, orderedSet(Integer.class, int.class));
        SUPPORTED_TYPES.put(DataTypeRoot.BIGINT, orderedSet(Long.class, long.class));
        SUPPORTED_TYPES.put(DataTypeRoot.FLOAT, orderedSet(Float.class, float.class));
        SUPPORTED_TYPES.put(DataTypeRoot.DOUBLE, orderedSet(Double.class, double.class));
        SUPPORTED_TYPES.put(
                DataTypeRoot.CHAR, orderedSet(String.class, Character.class, char.class));
        SUPPORTED_TYPES.put(
                DataTypeRoot.STRING, orderedSet(String.class, Character.class, char.class));
        SUPPORTED_TYPES.put(DataTypeRoot.BINARY, orderedSet(byte[].class));
        SUPPORTED_TYPES.put(DataTypeRoot.BYTES, orderedSet(byte[].class));
        SUPPORTED_TYPES.put(DataTypeRoot.DECIMAL, orderedSet(BigDecimal.class));
        SUPPORTED_TYPES.put(DataTypeRoot.DATE, orderedSet(LocalDate.class));
        SUPPORTED_TYPES.put(DataTypeRoot.TIME_WITHOUT_TIME_ZONE, orderedSet(LocalTime.class));
        SUPPORTED_TYPES.put(
                DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE, orderedSet(LocalDateTime.class));
        SUPPORTED_TYPES.put(
                DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                orderedSet(Instant.class, OffsetDateTime.class));
        // Add more supported types as needed

    }

    /** Interface for field conversion from POJO field to Fluss InternalRow field. */
    private interface FieldConverter {
        Object convert(Object obj) throws IllegalAccessException;
    }

    private final Class<T> pojoClass;
    private final RowType rowType;
    private final PojoTypeInfo<T> pojoTypeInfo;
    private final FieldConverter[] fieldConverters;

    /**
     * Creates a new converter for the specified POJO class and row type.
     *
     * @param pojoClass The class of POJOs to convert
     * @param rowType The row schema to use for conversion
     */
    @SuppressWarnings("unchecked")
    public PojoToRowConverter(Class<T> pojoClass, RowType rowType) {
        this.pojoClass = pojoClass;
        this.rowType = rowType;

        // Use Flink's POJO analysis
        this.pojoTypeInfo = (PojoTypeInfo<T>) Types.POJO(pojoClass);

        // Create converters for each field
        this.fieldConverters = createFieldConverters();
    }

    /** Creates field converters for each field in the schema. */
    private FieldConverter[] createFieldConverters() {
        FieldConverter[] converters = new FieldConverter[rowType.getFieldCount()];

        for (int i = 0; i < rowType.getFieldCount(); i++) {
            String fieldName = rowType.getFieldNames().get(i);
            DataType fieldType = rowType.getTypeAt(i);

            // Find field in POJO type info
            int pojoFieldPos = pojoTypeInfo.getFieldIndex(fieldName);
            if (pojoFieldPos >= 0) {
                PojoField pojoField = pojoTypeInfo.getPojoFieldAt(pojoFieldPos);
                TypeInformation<?> pojoFieldType = pojoField.getTypeInformation();

                // Check if field is a nested POJO
                if (pojoFieldType instanceof PojoTypeInfo) {
                    throw new UnsupportedOperationException(
                            "Nested POJO fields are not supported yet. Field: "
                                    + pojoField.getField().getName()
                                    + " in class "
                                    + pojoClass.getName());
                }

                // Check if the field type is supported
                if (!SUPPORTED_TYPES.containsKey(fieldType.getTypeRoot())) {
                    throw new UnsupportedOperationException(
                            "Unsupported field type "
                                    + fieldType.getTypeRoot()
                                    + " for field "
                                    + pojoField.getField().getName());
                } else if (!SUPPORTED_TYPES
                        .get(fieldType.getTypeRoot())
                        .contains(pojoFieldType.getTypeClass())) {
                    throw new UnsupportedOperationException(
                            "Field Java type "
                                    + pojoFieldType.getTypeClass()
                                    + " for field "
                                    + pojoField.getField().getName()
                                    + " is not supported, the supported Java types are "
                                    + SUPPORTED_TYPES.get(fieldType.getTypeRoot()));
                }

                // Create the appropriate converter for this field
                converters[i] = createConverterForField(fieldType, pojoField.getField());
            } else {
                // Field not found in POJO
                LOG.warn(
                        "Field '{}' not found in POJO class {}. Will return null for this field.",
                        fieldName,
                        pojoClass.getName());
                converters[i] = obj -> null;
            }
        }

        return converters;
    }

    /**
     * Creates a field converter for a specific field based on its data type.
     *
     * @param fieldType The Fluss data type
     * @param field The Java reflection field
     * @return A converter for this field
     */
    private FieldConverter createConverterForField(DataType fieldType, Field field) {
        field.setAccessible(true);

        switch (fieldType.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case BINARY:
            case BYTES:
                return field::get;
            case CHAR:
            case STRING:
                return obj -> {
                    Object value = field.get(obj);
                    return value == null ? null : BinaryString.fromString(value.toString());
                };
            case DECIMAL:
                return obj -> {
                    Object value = field.get(obj);
                    if (value == null) {
                        return null;
                    }
                    if (value instanceof BigDecimal) {
                        DecimalType decimalType = (DecimalType) fieldType;
                        return Decimal.fromBigDecimal(
                                (BigDecimal) value,
                                decimalType.getPrecision(),
                                decimalType.getScale());
                    } else {
                        LOG.warn(
                                "Field {} is not a BigDecimal. Cannot convert to DecimalData.",
                                field.getName());
                        return null;
                    }
                };
            case DATE:
                return obj -> {
                    Object value = field.get(obj);
                    if (value == null) {
                        return null;
                    }
                    if (value instanceof LocalDate) {
                        return (int) ((LocalDate) value).toEpochDay();
                    } else {
                        LOG.warn(
                                "Field {} is not a LocalDate. Cannot convert to int days.",
                                field.getName());
                        return null;
                    }
                };
            case TIME_WITHOUT_TIME_ZONE:
                return obj -> {
                    Object value = field.get(obj);
                    if (value == null) {
                        return null;
                    }
                    if (value instanceof LocalTime) {
                        LocalTime localTime = (LocalTime) value;
                        return (int) (localTime.toNanoOfDay() / 1_000_000);
                    } else {
                        LOG.warn(
                                "Field {} is not a LocalTime. Cannot convert to int millis.",
                                field.getName());
                        return null;
                    }
                };
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return obj -> {
                    Object value = field.get(obj);
                    if (value == null) {
                        return null;
                    }
                    if (value instanceof LocalDateTime) {
                        return TimestampNtz.fromLocalDateTime((LocalDateTime) value);
                    } else {
                        LOG.warn(
                                "Field {} is not a LocalDateTime. Cannot convert to TimestampData.",
                                field.getName());
                        return null;
                    }
                };
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return obj -> {
                    Object value = field.get(obj);
                    if (value == null) {
                        return null;
                    }
                    if (value instanceof Instant) {
                        return TimestampLtz.fromInstant((Instant) value);
                    } else if (value instanceof OffsetDateTime) {
                        OffsetDateTime offsetDateTime = (OffsetDateTime) value;
                        return TimestampLtz.fromInstant(offsetDateTime.toInstant());
                    } else {
                        LOG.warn(
                                "Field {} is not an Instant or OffsetDateTime. Cannot convert to TimestampData.",
                                field.getName());
                        return null;
                    }
                };
            default:
                LOG.warn(
                        "Unsupported type {} for field {}. Will use null for it.",
                        fieldType.getTypeRoot(),
                        field.getName());
                return obj -> null;
        }
    }

    /**
     * Converts a POJO to a GenericRow object according to the schema.
     *
     * @param pojo The POJO to convert
     * @return The converted GenericRow, or null if the input is null
     */
    public GenericRow convert(T pojo) {
        if (pojo == null) {
            return null;
        }

        GenericRow row = new GenericRow(rowType.getFieldCount());

        for (int i = 0; i < fieldConverters.length; i++) {
            Object value = null;
            try {
                value = fieldConverters[i].convert(pojo);
            } catch (IllegalAccessException e) {
                LOG.warn(
                        "Failed to access field {} in POJO class {}.",
                        rowType.getFieldNames().get(i),
                        pojoClass.getName(),
                        e);
            }
            row.setField(i, value);
        }

        return row;
    }

    /**
     * Utility method to create an ordered {@link LinkedHashSet} containing the specified Java type
     * classes.
     *
     * <p>The returned set maintains the insertion order of the provided classes.
     *
     * @param javaTypes The Java type classes to include in the set. May be one or more classes.
     * @return A new {@link LinkedHashSet} containing the given classes, preserving their order.
     */
    private static LinkedHashSet<Class<?>> orderedSet(Class<?>... javaTypes) {
        LinkedHashSet<Class<?>> linkedHashSet = new LinkedHashSet<>();
        linkedHashSet.addAll(Arrays.asList(javaTypes));
        return linkedHashSet;
    }
}
