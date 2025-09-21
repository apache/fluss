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

package org.apache.fluss.client.utils;

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeChecks;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.RowType;

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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Internal helper for converting Java POJOs to Fluss {@link InternalRow} and back.
 *
 * <p>This utility uses reflection to map fields between a POJO and an InternalRow according to a
 * provided {@link RowType} schema. It does not cache converters; each call to {@link
 * #getConverter(Class, RowType)} creates a new instance.
 *
 * <p>Notes: - Nested POJOs are not supported. - If a row contains null for a field that maps to a
 * primitive type in the POJO, that field will not be set (it keeps the Java default). Prefer boxed
 * types if nulls are expected.
 *
 * <p>This class is intended for internal use only.
 *
 * @param <T> The POJO type to convert
 */
public final class PojoConverterUtils<T> {

    /** Map of supported Java types for each DataTypeRoot. */
    private static final Map<DataTypeRoot, Set<Class<?>>> SUPPORTED_TYPES = new HashMap<>();

    private final Class<T> pojoClass;
    private final RowType rowType;
    private final FieldToRowConverter[] fieldToRowConverters;
    private final RowToFieldConverter[] rowToFieldConverters;
    private final Field[] pojoFields;
    private final Constructor<T> defaultConstructor;

    static {
        SUPPORTED_TYPES.put(DataTypeRoot.BOOLEAN, setOf(Boolean.class, boolean.class));
        SUPPORTED_TYPES.put(DataTypeRoot.TINYINT, setOf(Byte.class, byte.class));
        SUPPORTED_TYPES.put(DataTypeRoot.SMALLINT, setOf(Short.class, short.class));
        SUPPORTED_TYPES.put(DataTypeRoot.INTEGER, setOf(Integer.class, int.class));
        SUPPORTED_TYPES.put(DataTypeRoot.BIGINT, setOf(Long.class, long.class));
        SUPPORTED_TYPES.put(DataTypeRoot.FLOAT, setOf(Float.class, float.class));
        SUPPORTED_TYPES.put(DataTypeRoot.DOUBLE, setOf(Double.class, double.class));
        SUPPORTED_TYPES.put(DataTypeRoot.CHAR, setOf(String.class, Character.class, char.class));
        SUPPORTED_TYPES.put(DataTypeRoot.STRING, setOf(String.class, Character.class, char.class));
        SUPPORTED_TYPES.put(DataTypeRoot.BINARY, setOf(byte[].class));
        SUPPORTED_TYPES.put(DataTypeRoot.BYTES, setOf(byte[].class));
        SUPPORTED_TYPES.put(DataTypeRoot.DECIMAL, setOf(BigDecimal.class));
        SUPPORTED_TYPES.put(DataTypeRoot.DATE, setOf(LocalDate.class));
        SUPPORTED_TYPES.put(DataTypeRoot.TIME_WITHOUT_TIME_ZONE, setOf(LocalTime.class));
        SUPPORTED_TYPES.put(DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE, setOf(LocalDateTime.class));
        SUPPORTED_TYPES.put(
                DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                setOf(Instant.class, OffsetDateTime.class));

        // TODO: Add more types when https://github.com/apache/fluss/issues/816 is merged
    }

    /** Interface for field conversion from POJO field to Fluss InternalRow field. */
    @FunctionalInterface
    private interface FieldToRowConverter {
        Object convert(Object obj) throws Exception;
    }

    /** Interface for field conversion from Fluss InternalRow field to POJO field. */
    @FunctionalInterface
    private interface RowToFieldConverter {
        Object convert(InternalRow row, int pos) throws Exception;
    }

    /**
     * A converter that assumes the source field value is non-null when converting from POJO field
     * value to row field value.
     */
    @FunctionalInterface
    private interface NotNullFieldToRowConverter {
        Object convert(Object nonNullFieldValue) throws Exception;
    }

    /**
     * A converter that assumes the row value at a given position is non-null when converting to a
     * POJO field value.
     */
    @FunctionalInterface
    private interface NotNullRowToFieldConverter {
        Object convert(InternalRow row, int pos) throws Exception;
    }

    /** Wraps a non-null field-to-row converter with null handling and reflective field access. */
    private static FieldToRowConverter nullableFieldConverter(
            Field field, NotNullFieldToRowConverter nonNullConverter) {
        field.setAccessible(true);
        return obj -> {
            Object value = field.get(obj);
            if (value == null) {
                return null;
            }
            return nonNullConverter.convert(value);
        };
    }

    /** Wraps a non-null row-to-field converter with row null checks. */
    private static RowToFieldConverter nullableRowConverter(
            NotNullRowToFieldConverter nonNullConverter) {
        return (row, pos) -> row.isNullAt(pos) ? null : nonNullConverter.convert(row, pos);
    }

    /**
     * Creates a new converter for the specified POJO class and row type.
     *
     * @param pojoClass The class of POJOs to convert
     * @param rowType The row schema to use for conversion
     */
    @SuppressWarnings("unchecked")
    private PojoConverterUtils(Class<T> pojoClass, RowType rowType) {
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
     * Creates a converter for the specified POJO class and row type.
     *
     * @param pojoClass The class of POJOs to convert
     * @param rowType The row schema to use for conversion
     * @param <T> The POJO type
     * @return A converter for the specified POJO class and row type
     */
    @SuppressWarnings("unchecked")
    public static <T> PojoConverterUtils<T> getConverter(Class<T> pojoClass, RowType rowType) {
        return new PojoConverterUtils<>(pojoClass, rowType);
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
                    throw new IllegalArgumentException(
                            "Unsupported field type "
                                    + fieldType.getTypeRoot()
                                    + " for field "
                                    + field.getName());
                }

                // Validate Java field type compatibility with Fluss type
                Set<Class<?>> supported = SUPPORTED_TYPES.get(fieldType.getTypeRoot());
                Class<?> actual = field.getType();
                if (supported == null || !supported.contains(actual)) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Field '%s' in POJO %s has Java type %s which is incompatible with Fluss type %s. Supported Java types: %s",
                                    field.getName(),
                                    pojoClass.getName(),
                                    actual.getName(),
                                    fieldType.getTypeRoot(),
                                    supported));
                }
                // Create the appropriate converter for this field
                converters[i] = createFieldToRowConverter(fieldType, field);
            } else {
                throw new IllegalArgumentException(
                        String.format(
                                "Field '%s' not found in POJO class %s.",
                                fieldName, pojoClass.getName()));
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
                // Validate Java field type compatibility with Fluss type
                Set<Class<?>> supported = SUPPORTED_TYPES.get(fieldType.getTypeRoot());
                Class<?> actual = field.getType();
                if (supported == null || !supported.contains(actual)) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Field '%s' in POJO %s has Java type %s which is incompatible with Fluss type %s. Supported Java types: %s",
                                    field.getName(),
                                    pojoClass.getName(),
                                    actual.getName(),
                                    fieldType.getTypeRoot(),
                                    supported));
                }
                // Create the appropriate converter for this field
                converters[i] = createRowToFieldConverter(fieldType, field);
            } else {
                // Field not found in POJO
                String fieldName = rowType.getFieldNames().get(i);
                throw new IllegalArgumentException(
                        String.format(
                                "Field '%s' not found in POJO class %s.",
                                fieldName, pojoClass.getName()));
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
                return nullableFieldConverter(field, v -> v);
            case CHAR:
            case STRING:
                return nullableFieldConverter(
                        field,
                        v -> {
                            String s;
                            if (v instanceof Character) {
                                s = String.valueOf((Character) v);
                            } else if (v instanceof String) {
                                s = (String) v;
                            } else {
                                s = v.toString();
                            }
                            if (fieldType.getTypeRoot() == DataTypeRoot.CHAR && s.length() != 1) {
                                throw new IllegalArgumentException(
                                        String.format(
                                                "Field %s expects exactly one character for CHAR type, got length %d.",
                                                field.getName(), s.length()));
                            }
                            return BinaryString.fromString(s);
                        });
            case DECIMAL:
                return nullableFieldConverter(
                        field,
                        v -> {
                            if (v instanceof BigDecimal) {
                                DecimalType decimalType = (DecimalType) fieldType;
                                return Decimal.fromBigDecimal(
                                        (BigDecimal) v,
                                        decimalType.getPrecision(),
                                        decimalType.getScale());
                            } else {
                                throw new IllegalArgumentException(
                                        String.format(
                                                "Field %s is not a BigDecimal. Cannot convert to DecimalData.",
                                                field.getName()));
                            }
                        });
            case DATE:
                return nullableFieldConverter(
                        field,
                        v -> {
                            if (v instanceof LocalDate) {
                                return (int) ((LocalDate) v).toEpochDay();
                            } else {
                                throw new IllegalArgumentException(
                                        String.format(
                                                "Field %s is not a LocalDate. Cannot convert to int days.",
                                                field.getName()));
                            }
                        });
            case TIME_WITHOUT_TIME_ZONE:
                return nullableFieldConverter(
                        field,
                        v -> {
                            if (v instanceof LocalTime) {
                                LocalTime localTime = (LocalTime) v;
                                return (int) (localTime.toNanoOfDay() / 1_000_000);
                            } else {
                                throw new IllegalArgumentException(
                                        String.format(
                                                "Field %s is not a LocalTime. Cannot convert to int millis.",
                                                field.getName()));
                            }
                        });
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return nullableFieldConverter(
                        field,
                        v -> {
                            if (v instanceof LocalDateTime) {
                                return TimestampNtz.fromLocalDateTime((LocalDateTime) v);
                            } else {
                                throw new IllegalArgumentException(
                                        String.format(
                                                "Field %s is not a LocalDateTime. Cannot convert to TimestampNtz.",
                                                field.getName()));
                            }
                        });
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return nullableFieldConverter(
                        field,
                        v -> {
                            if (v instanceof Instant) {
                                return TimestampLtz.fromInstant((Instant) v);
                            } else if (v instanceof OffsetDateTime) {
                                OffsetDateTime offsetDateTime = (OffsetDateTime) v;
                                return TimestampLtz.fromInstant(offsetDateTime.toInstant());
                            } else {
                                throw new IllegalArgumentException(
                                        String.format(
                                                "Field %s is not an Instant or OffsetDateTime. Cannot convert to TimestampLtz.",
                                                field.getName()));
                            }
                        });
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "Unsupported type %s for field %s.",
                                fieldType.getTypeRoot(), field.getName()));
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
                return nullableRowConverter((row, pos) -> row.getBoolean(pos));
            case TINYINT:
                return nullableRowConverter((row, pos) -> row.getByte(pos));
            case SMALLINT:
                return nullableRowConverter((row, pos) -> row.getShort(pos));
            case INTEGER:
                return nullableRowConverter((row, pos) -> row.getInt(pos));
            case BIGINT:
                return nullableRowConverter((row, pos) -> row.getLong(pos));
            case FLOAT:
                return nullableRowConverter((row, pos) -> row.getFloat(pos));
            case DOUBLE:
                return nullableRowConverter((row, pos) -> row.getDouble(pos));
            case CHAR:
            case STRING:
                return nullableRowConverter(
                        (row, pos) -> {
                            BinaryString binaryString = row.getString(pos);
                            String value = binaryString.toString();
                            if (fieldClass == String.class) {
                                if (fieldType.getTypeRoot() == DataTypeRoot.CHAR
                                        && value.length() != 1) {
                                    throw new IllegalArgumentException(
                                            String.format(
                                                    "Field %s expects exactly one character for CHAR type, got length %d.",
                                                    field.getName(), value.length()));
                                }
                                return value;
                            } else if (fieldClass == Character.class || fieldClass == char.class) {
                                if (value.isEmpty()) {
                                    throw new IllegalArgumentException(
                                            String.format(
                                                    "Field %s expects Character/char, but the string value is empty.",
                                                    field.getName()));
                                }
                                if (fieldType.getTypeRoot() == DataTypeRoot.CHAR
                                        && value.length() != 1) {
                                    throw new IllegalArgumentException(
                                            String.format(
                                                    "Field %s expects exactly one character for CHAR type, got length %d.",
                                                    field.getName(), value.length()));
                                }
                                return value.charAt(0);
                            } else {
                                // Defensive check: should be prevented by validation elsewhere.
                                throw new IllegalArgumentException(
                                        String.format(
                                                "Field %s is not a String or Character/char. Cannot convert from string.",
                                                field.getName()));
                            }
                        });
            case BINARY:
            case BYTES:
                return nullableRowConverter((row, pos) -> row.getBytes(pos));
            case DECIMAL:
                return nullableRowConverter(
                        (row, pos) -> {
                            DecimalType decimalType = (DecimalType) fieldType;
                            Decimal decimal =
                                    row.getDecimal(
                                            pos,
                                            decimalType.getPrecision(),
                                            decimalType.getScale());
                            return decimal.toBigDecimal();
                        });
            case DATE:
                return nullableRowConverter(
                        (row, pos) -> {
                            int days = row.getInt(pos);
                            return LocalDate.ofEpochDay(days);
                        });
            case TIME_WITHOUT_TIME_ZONE:
                return nullableRowConverter(
                        (row, pos) -> {
                            int millis = row.getInt(pos);
                            return LocalTime.ofNanoOfDay(millis * 1_000_000L);
                        });
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                {
                    final int precision = DataTypeChecks.getPrecision(fieldType);
                    return nullableRowConverter(
                            (row, pos) -> {
                                TimestampNtz timestampNtz = row.getTimestampNtz(pos, precision);
                                return timestampNtz.toLocalDateTime();
                            });
                }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                {
                    final int precision = DataTypeChecks.getPrecision(fieldType);
                    return nullableRowConverter(
                            (row, pos) -> {
                                TimestampLtz timestampLtz = row.getTimestampLtz(pos, precision);
                                if (fieldClass == Instant.class) {
                                    return timestampLtz.toInstant();
                                } else if (fieldClass == OffsetDateTime.class) {
                                    return OffsetDateTime.ofInstant(
                                            timestampLtz.toInstant(), ZoneOffset.UTC);
                                } else {
                                    throw new IllegalArgumentException(
                                            String.format(
                                                    "Field %s is not an Instant or OffsetDateTime. Cannot convert from TimestampLtz.",
                                                    field.getName()));
                                }
                            });
                }
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "Unsupported type %s for field %s.",
                                fieldType.getTypeRoot(), field.getName()));
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
            } catch (Exception e) {
                if (e instanceof IllegalArgumentException) {
                    throw (IllegalArgumentException) e;
                }
                throw new IllegalStateException(
                        String.format(
                                "Failed to access field %s in POJO class %s.",
                                rowType.getFieldNames().get(i), pojoClass.getName()),
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
                    } catch (Exception e) {
                        if (e instanceof IllegalArgumentException) {
                            throw (IllegalArgumentException) e;
                        }
                        throw new IllegalStateException(
                                String.format(
                                        "Failed to set field %s in POJO class %s.",
                                        rowType.getFieldNames().get(i), pojoClass.getName()),
                                e);
                    }
                }
            }

            return pojo;
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            String message =
                    String.format(
                            "Failed to instantiate POJO class %s using the default constructor. "
                                    + "Ensure the class has an accessible no-arg constructor that does not throw. "
                                    + "Cause: %s",
                            pojoClass.getName(), e.getMessage());
            throw new IllegalStateException(message, e);
        }
    }

    /**
     * Utility method to create a Set containing the specified Java type classes.
     *
     * @param javaTypes The Java type classes to include in the set. May be one or more classes.
     * @return A new Set containing the given classes.
     */
    private static Set<Class<?>> setOf(Class<?>... javaTypes) {
        return new HashSet<>(Arrays.asList(javaTypes));
    }
}
