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

package com.alibaba.fluss.client.utils;

import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DataTypeRoot;
import com.alibaba.fluss.types.DecimalType;
import com.alibaba.fluss.types.LocalZonedTimestampType;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.types.TimestampType;
import com.alibaba.fluss.utils.MapUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Helper class for converting Java POJOs to Fluss's {@link InternalRow} format and vice versa.
 *
 * <p>This utility uses reflection to map fields from POJOs to InternalRow and back based on a given
 * schema. It includes caching mechanisms to avoid repeated reflection operations for the same POJO
 * types.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * // Create a converter
 * ConverterUtils<Order> converter = new ConverterUtils<>(Order.class, rowType);
 *
 * // Convert a POJO to GenericRow
 * Order order = new Order(1001L, 5001L, 10, "123 Athens");
 * GenericRow row = converter.toRow(order);
 *
 * // Convert a GenericRow back to POJO
 * Order convertedOrder = converter.fromRow(row);
 * }</pre>
 *
 * <p>Note: Nested POJO fields are not supported in the current implementation.
 *
 * @param <T> The POJO type to convert
 */
public class ConverterUtils<T> {
    private static final Logger LOG = LoggerFactory.getLogger(ConverterUtils.class);

    /** Cache for converters to avoid repeated reflection operations. */
    private static final ConcurrentHashMap<CacheKey, ConverterUtils<?>> CONVERTER_CACHE =
            MapUtils.newConcurrentHashMap();

    /** Map of supported Java types for each DataTypeRoot. */
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

        // TODO: Add more types when https://github.com/apache/fluss/issues/816 is merged
    }

    /** Interface for field conversion from POJO field to Fluss InternalRow field. */
    private interface FieldToRowConverter {
        Object convert(Object obj) throws IllegalAccessException;
    }

    /** Interface for field conversion from Fluss InternalRow field to POJO field. */
    private interface RowToFieldConverter {
        Object convert(InternalRow row, int pos) throws IllegalAccessException;
    }

    private final Class<T> pojoClass;
    private final RowType rowType;
    private final FieldToRowConverter[] fieldToRowConverters;
    private final RowToFieldConverter[] rowToFieldConverters;
    private final Field[] pojoFields;
    private final Constructor<T> defaultConstructor;

    /**
     * Creates a new converter for the specified POJO class and row type.
     *
     * @param pojoClass The class of POJOs to convert
     * @param rowType The row schema to use for conversion
     */
    @SuppressWarnings("unchecked")
    public ConverterUtils(Class<T> pojoClass, RowType rowType) {
        this.pojoClass = pojoClass;
        this.rowType = rowType;

        // Create converters for each field
        this.pojoFields = new Field[rowType.getFieldCount()];
        this.fieldToRowConverters = createFieldToRowConverters();
        this.rowToFieldConverters = createRowToFieldConverters();

        // Get the default constructor for creating new instances
        try {
            this.defaultConstructor = pojoClass.getDeclaredConstructor();
            this.defaultConstructor.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException(
                    "POJO class " + pojoClass.getName() + " must have a default constructor", e);
        }
    }

    /**
     * Gets a cached converter for the specified POJO class and row type, or creates a new one if
     * not found in the cache.
     *
     * @param pojoClass The class of POJOs to convert
     * @param rowType The row schema to use for conversion
     * @param <T> The POJO type
     * @return A converter for the specified POJO class and row type
     */
    @SuppressWarnings("unchecked")
    public static <T> ConverterUtils<T> getConverter(Class<T> pojoClass, RowType rowType) {
        CacheKey key = new CacheKey(pojoClass, rowType);
        return (ConverterUtils<T>)
                CONVERTER_CACHE.computeIfAbsent(key, k -> new ConverterUtils<>(pojoClass, rowType));
    }

    /** Creates field converters for converting from POJO to Row for each field in the schema. */
    private FieldToRowConverter[] createFieldToRowConverters() {
        FieldToRowConverter[] converters = new FieldToRowConverter[rowType.getFieldCount()];

        for (int i = 0; i < rowType.getFieldCount(); i++) {
            String fieldName = rowType.getFieldNames().get(i);
            DataType fieldType = rowType.getTypeAt(i);

            // Find field in POJO class
            Field field = findField(pojoClass, fieldName);
            if (field != null) {
                pojoFields[i] = field;

                // Check if the field type is supported
                if (!SUPPORTED_TYPES.containsKey(fieldType.getTypeRoot())) {
                    throw new UnsupportedOperationException(
                            "Unsupported field type "
                                    + fieldType.getTypeRoot()
                                    + " for field "
                                    + field.getName());
                }

                // Create the appropriate converter for this field
                converters[i] = createFieldToRowConverter(fieldType, field);
            } else {
                LOG.warn(
                        "Field '{}' not found in POJO class {}. Will return null for this field.",
                        fieldName,
                        pojoClass.getName());
                converters[i] = obj -> null;
            }
        }

        return converters;
    }

    /** Creates field converters for converting from Row to POJO for each field in the schema. */
    private RowToFieldConverter[] createRowToFieldConverters() {
        RowToFieldConverter[] converters = new RowToFieldConverter[rowType.getFieldCount()];

        for (int i = 0; i < rowType.getFieldCount(); i++) {
            DataType fieldType = rowType.getTypeAt(i);
            Field field = pojoFields[i];

            if (field != null) {
                // Create the appropriate converter for this field
                converters[i] = createRowToFieldConverter(fieldType, field);
            } else {
                // Field not found in POJO
                converters[i] = (row, pos) -> null;
            }
        }

        return converters;
    }

    /**
     * Finds a field in the given class or its superclasses.
     *
     * @param clazz The class to search
     * @param fieldName The name of the field to find
     * @return The field, or null if not found
     */
    @Nullable
    private Field findField(Class<?> clazz, String fieldName) {
        Class<?> currentClass = clazz;
        while (currentClass != null) {
            try {
                Field field = currentClass.getDeclaredField(fieldName);
                field.setAccessible(true);
                return field;
            } catch (NoSuchFieldException e) {
                currentClass = currentClass.getSuperclass();
            }
        }
        return null;
    }

    /**
     * Creates a field converter for converting from POJO to Row for a specific field based on its
     * data type.
     *
     * @param fieldType The Fluss data type
     * @param field The Java reflection field
     * @return A converter for this field
     */
    private FieldToRowConverter createFieldToRowConverter(DataType fieldType, Field field) {
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
                        throw new IllegalArgumentException(
                                String.format("Field %s is not a BigDecimal. Cannot convert to DecimalData.",
                                        field.getName()));
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
     * Creates a field converter for converting from Row to POJO for a specific field based on its
     * data type.
     *
     * @param fieldType The Fluss data type
     * @param field The Java reflection field
     * @return A converter for this field
     */
    private RowToFieldConverter createRowToFieldConverter(DataType fieldType, Field field) {
        field.setAccessible(true);
        Class<?> fieldClass = field.getType();

        switch (fieldType.getTypeRoot()) {
            case BOOLEAN:
                return (row, pos) -> row.isNullAt(pos) ? null : row.getBoolean(pos);
            case TINYINT:
                return (row, pos) -> row.isNullAt(pos) ? null : row.getByte(pos);
            case SMALLINT:
                return (row, pos) -> row.isNullAt(pos) ? null : row.getShort(pos);
            case INTEGER:
                return (row, pos) -> row.isNullAt(pos) ? null : row.getInt(pos);
            case BIGINT:
                return (row, pos) -> row.isNullAt(pos) ? null : row.getLong(pos);
            case FLOAT:
                return (row, pos) -> row.isNullAt(pos) ? null : row.getFloat(pos);
            case DOUBLE:
                return (row, pos) -> row.isNullAt(pos) ? null : row.getDouble(pos);
            case CHAR:
            case STRING:
                return (row, pos) -> {
                    if (row.isNullAt(pos)) {
                        return null;
                    }
                    BinaryString binaryString = row.getString(pos);
                    return binaryString.toString();
                };
            case BINARY:
            case BYTES:
                return (row, pos) -> row.isNullAt(pos) ? null : row.getBytes(pos);
            case DECIMAL:
                return (row, pos) -> {
                    if (row.isNullAt(pos)) {
                        return null;
                    }
                    DecimalType decimalType = (DecimalType) fieldType;
                    Decimal decimal =
                            row.getDecimal(pos, decimalType.getPrecision(), decimalType.getScale());
                    return decimal.toBigDecimal();
                };
            case DATE:
                return (row, pos) -> {
                    if (row.isNullAt(pos)) {
                        return null;
                    }
                    int days = row.getInt(pos);
                    return LocalDate.ofEpochDay(days);
                };
            case TIME_WITHOUT_TIME_ZONE:
                return (row, pos) -> {
                    if (row.isNullAt(pos)) {
                        return null;
                    }
                    int millis = row.getInt(pos);
                    return LocalTime.ofNanoOfDay(millis * 1_000_000L);
                };
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (row, pos) -> {
                    if (row.isNullAt(pos)) {
                        return null;
                    }
                    TimestampNtz timestampNtz = row.getTimestampNtz(pos, getPrecision(fieldType));
                    return timestampNtz.toLocalDateTime();
                };
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return (row, pos) -> {
                    if (row.isNullAt(pos)) {
                        return null;
                    }
                    TimestampLtz timestampLtz = row.getTimestampLtz(pos, getPrecision(fieldType));
                    if (fieldClass == Instant.class) {
                        return timestampLtz.toInstant();
                    } else if (fieldClass == OffsetDateTime.class) {
                        return OffsetDateTime.ofInstant(timestampLtz.toInstant(), ZoneOffset.UTC);
                    } else {
                        LOG.warn(
                                "Field {} is not an Instant or OffsetDateTime. Cannot convert from TimestampData.",
                                field.getName());
                        return null;
                    }
                };
            default:
                LOG.warn(
                        "Unsupported type {} for field {}. Will use null for it.",
                        fieldType.getTypeRoot(),
                        field.getName());
                return (row, pos) -> null;
        }
    }

    /**
     * Converts a POJO to a GenericRow object according to the schema.
     *
     * @param pojo The POJO to convert
     * @return The converted GenericRow, or null if the input is null
     */
    public GenericRow toRow(T pojo) {
        if (pojo == null) {
            return null;
        }

        GenericRow row = new GenericRow(rowType.getFieldCount());

        for (int i = 0; i < fieldToRowConverters.length; i++) {
            Object value = null;
            try {
                value = fieldToRowConverters[i].convert(pojo);
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
     * Converts an InternalRow to a POJO object according to the schema.
     *
     * @param row The InternalRow to convert
     * @return The converted POJO, or null if the input is null
     */
    public T fromRow(InternalRow row) {
        if (row == null) {
            return null;
        }

        try {
            T pojo = defaultConstructor.newInstance();

            for (int i = 0; i < rowToFieldConverters.length; i++) {
                if (pojoFields[i] != null) {
                    try {
                        Object value = rowToFieldConverters[i].convert(row, i);
                        if (value != null) {
                            pojoFields[i].set(pojo, value);
                        }
                    } catch (IllegalAccessException e) {
                        LOG.warn(
                                "Failed to set field {} in POJO class {}.",
                                rowType.getFieldNames().get(i),
                                pojoClass.getName(),
                                e);
                    }
                }
            }

            return pojo;
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            LOG.error("Failed to create instance of POJO class {}.", pojoClass.getName(), e);
            return null;
        }
    }

    /**
     * Gets the precision of a data type.
     *
     * @param dataType The data type
     * @return The precision
     */
    private static int getPrecision(DataType dataType) {
        switch (dataType.getTypeRoot()) {
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return ((TimestampType) dataType).getPrecision();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return ((LocalZonedTimestampType) dataType).getPrecision();
            case DECIMAL:
                return ((DecimalType) dataType).getPrecision();
            default:
                return 0;
        }
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

    /** Key for caching converters. */
    private static class CacheKey {
        private final Class<?> pojoClass;
        private final RowType rowType;

        public CacheKey(Class<?> pojoClass, RowType rowType) {
            this.pojoClass = pojoClass;
            this.rowType = rowType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CacheKey cacheKey = (CacheKey) o;
            return pojoClass.equals(cacheKey.pojoClass) && rowType.equals(cacheKey.rowType);
        }

        @Override
        public int hashCode() {
            return 31 * pojoClass.hashCode() + rowType.hashCode();
        }
    }
}
