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

import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DecimalType;
import com.alibaba.fluss.types.RowType;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

/**
 * Utility class for converting Java POJOs to Flink's {@link RowData} format.
 *
 * <p>This utility uses Flink's POJO type information to map fields from POJOs to RowData based on a
 * given schema. It follows Flink's pattern for data conversion.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * // Create a converter
 * PojoToRowDataConverter<Order> converter =
 *     new PojoToRowDataConverter<>(Order.class, rowType);
 *
 * // Convert a POJO to RowData
 * Order order = new Order(1001L, 5001L, 10, "123 Mumbai");
 * RowData rowData = converter.convert(order);
 * }</pre>
 *
 * <p>Note: Nested POJO fields are not supported in the current implementation.
 *
 * @param <T> The POJO type to convert
 */
public class PojoToRowDataConverter<T> {
    private static final Logger LOG = LoggerFactory.getLogger(PojoToRowDataConverter.class);

    /** Interface for field conversion from POJO field to RowData field. */
    private interface FieldConverter {
        Object convert(Object obj);
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
    public PojoToRowDataConverter(Class<T> pojoClass, RowType rowType) {
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
                } else {
                    // Create the appropriate converter for this field
                    converters[i] = createConverterForField(fieldType, pojoField.getField());
                }
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
                return obj -> {
                    try {
                        return field.get(obj);
                    } catch (IllegalAccessException e) {
                        LOG.error("Failed to access boolean field: {}", field.getName(), e);
                        return null;
                    }
                };
            case TINYINT:
                return obj -> {
                    try {
                        return field.get(obj);
                    } catch (IllegalAccessException e) {
                        LOG.error("Failed to access byte field: {}", field.getName(), e);
                        return null;
                    }
                };
            case SMALLINT:
                return obj -> {
                    try {
                        return field.get(obj);
                    } catch (IllegalAccessException e) {
                        LOG.error("Failed to access short field: {}", field.getName(), e);
                        return null;
                    }
                };
            case INTEGER:
                return obj -> {
                    try {
                        return field.get(obj);
                    } catch (IllegalAccessException e) {
                        LOG.error("Failed to access int field: {}", field.getName(), e);
                        return null;
                    }
                };
            case BIGINT:
                return obj -> {
                    try {
                        return field.get(obj);
                    } catch (IllegalAccessException e) {
                        LOG.error("Failed to access long field: {}", field.getName(), e);
                        return null;
                    }
                };
            case FLOAT:
                return obj -> {
                    try {
                        return field.get(obj);
                    } catch (IllegalAccessException e) {
                        LOG.error("Failed to access float field: {}", field.getName(), e);
                        return null;
                    }
                };
            case DOUBLE:
                return obj -> {
                    try {
                        return field.get(obj);
                    } catch (IllegalAccessException e) {
                        LOG.error("Failed to access double field: {}", field.getName(), e);
                        return null;
                    }
                };
            case CHAR:
            case STRING:
                return obj -> {
                    try {
                        Object value = field.get(obj);
                        return value == null ? null : StringData.fromString(value.toString());
                    } catch (IllegalAccessException e) {
                        LOG.error("Failed to access string field: {}", field.getName(), e);
                        return null;
                    }
                };
            case BINARY:
            case BYTES:
                return obj -> {
                    try {
                        return field.get(obj);
                    } catch (IllegalAccessException e) {
                        LOG.error("Failed to access binary field: {}", field.getName(), e);
                        return null;
                    }
                };
            case DECIMAL:
                return obj -> {
                    try {
                        Object value = field.get(obj);
                        if (value == null) {
                            return null;
                        }
                        if (value instanceof BigDecimal) {
                            DecimalType decimalType = (DecimalType) fieldType;
                            return DecimalData.fromBigDecimal(
                                    (BigDecimal) value,
                                    decimalType.getPrecision(),
                                    decimalType.getScale());
                        } else {
                            LOG.warn(
                                    "Field {} is not a BigDecimal. Cannot convert to DecimalData.",
                                    field.getName());
                            return null;
                        }
                    } catch (IllegalAccessException e) {
                        LOG.error("Failed to access decimal field: {}", field.getName(), e);
                        return null;
                    }
                };
            case DATE:
                return obj -> {
                    try {
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
                    } catch (IllegalAccessException e) {
                        LOG.error("Failed to access date field: {}", field.getName(), e);
                        return null;
                    }
                };
            case TIME_WITHOUT_TIME_ZONE:
                return obj -> {
                    try {
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
                    } catch (IllegalAccessException e) {
                        LOG.error("Failed to access time field: {}", field.getName(), e);
                        return null;
                    }
                };
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return obj -> {
                    try {
                        Object value = field.get(obj);
                        if (value == null) {
                            return null;
                        }
                        if (value instanceof LocalDateTime) {
                            return TimestampData.fromLocalDateTime((LocalDateTime) value);
                        } else {
                            LOG.warn(
                                    "Field {} is not a LocalDateTime. Cannot convert to TimestampData.",
                                    field.getName());
                            return null;
                        }
                    } catch (IllegalAccessException e) {
                        LOG.error("Failed to access timestamp field: {}", field.getName(), e);
                        return null;
                    }
                };
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return obj -> {
                    try {
                        Object value = field.get(obj);
                        if (value == null) {
                            return null;
                        }
                        if (value instanceof Instant) {
                            return TimestampData.fromInstant((Instant) value);
                        } else if (value instanceof OffsetDateTime) {
                            OffsetDateTime offsetDateTime = (OffsetDateTime) value;
                            return TimestampData.fromInstant(offsetDateTime.toInstant());
                        } else {
                            LOG.warn(
                                    "Field {} is not an Instant or OffsetDateTime. Cannot convert to TimestampData.",
                                    field.getName());
                            return null;
                        }
                    } catch (IllegalAccessException e) {
                        LOG.error(
                                "Failed to access timestamp with tz field: {}", field.getName(), e);
                        return null;
                    }
                };
            default:
                LOG.warn(
                        "Unsupported type {} for field {}. Will try to return as-is.",
                        fieldType.getTypeRoot(),
                        field.getName());
                return obj -> {
                    try {
                        return field.get(obj);
                    } catch (IllegalAccessException e) {
                        LOG.error("Failed to access field: {}", field.getName(), e);
                        return null;
                    }
                };
        }
    }

    /**
     * Converts a POJO to a RowData object according to the schema.
     *
     * @param pojo The POJO to convert
     * @return The converted RowData, or null if the input is null
     */
    public RowData convert(T pojo) {
        if (pojo == null) {
            return null;
        }

        GenericRowData rowData = new GenericRowData(rowType.getFieldCount());

        for (int i = 0; i < fieldConverters.length; i++) {
            rowData.setField(i, fieldConverters[i].convert(pojo));
        }

        return rowData;
    }
}
