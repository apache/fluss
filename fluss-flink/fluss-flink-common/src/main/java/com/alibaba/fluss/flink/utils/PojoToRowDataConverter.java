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
import com.alibaba.fluss.types.RowType;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for converting Java POJOs to Flink's {@link RowData} format.
 *
 * <p>This utility uses reflection to map fields from POJOs to RowData based on a given schema. It
 * can be used by custom serialization schemas to simplify the implementation of the serialization
 * logic.
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
 * @param <T> The POJO type to convert
 */
public class PojoToRowDataConverter<T> {
    private static final Logger LOG = LoggerFactory.getLogger(PojoToRowDataConverter.class);

    private final Class<T> pojoClass;
    private final RowType rowType;
    private final Map<String, Field> fieldMap = new HashMap<>();

    /**
     * Creates a new converter for the specified POJO class and row type.
     *
     * @param pojoClass The class of POJOs to convert
     * @param rowType The row schema to use for conversion
     */
    public PojoToRowDataConverter(Class<T> pojoClass, RowType rowType) {
        this.pojoClass = pojoClass;
        this.rowType = rowType;
        initializeFieldMap();
    }

    private void initializeFieldMap() {
        Class<?> currentClass = pojoClass;
        while (currentClass != null) {
            for (Field field : currentClass.getDeclaredFields()) {
                if (!Modifier.isStatic(field.getModifiers())
                        && !Modifier.isTransient(field.getModifiers())) {
                    field.setAccessible(true);
                    fieldMap.put(field.getName(), field);
                }
            }
            currentClass = currentClass.getSuperclass();
        }
    }

    /**
     * Converts a POJO to a RowData object according to the schema.
     *
     * @param pojo The POJO to convert
     * @return The converted RowData, or null if the input is null
     * @throws IllegalAccessException If a field cannot be accessed
     */
    public RowData convert(T pojo) throws IllegalAccessException {
        if (pojo == null) {
            return null;
        }

        GenericRowData rowData = new GenericRowData(rowType.getFieldCount());

        for (int i = 0; i < rowType.getFieldCount(); i++) {
            String fieldName = rowType.getFieldNames().get(i);
            DataType fieldType = rowType.getTypeAt(i);

            Field field = fieldMap.get(fieldName);

            if (field != null) {
                Object fieldValue = field.get(pojo);
                rowData.setField(i, convertToRowDataField(fieldValue, fieldType));
            } else {
                rowData.setField(i, null);
            }
        }

        return rowData;
    }

    private Object convertToRowDataField(Object value, DataType fieldType) {
        if (value == null) {
            return null;
        }

        switch (fieldType.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                return value;
            case CHAR:
            case STRING:
                return StringData.fromString(value.toString());
            case BINARY:
            case BYTES:
                if (value instanceof byte[]) {
                    return value;
                }
                break;
            case DECIMAL:
                if (value instanceof BigDecimal) {
                    com.alibaba.fluss.types.DecimalType decimalType =
                            (com.alibaba.fluss.types.DecimalType) fieldType;
                    return DecimalData.fromBigDecimal(
                            (BigDecimal) value, decimalType.getPrecision(), decimalType.getScale());
                }
                break;
            case DATE:
                if (value instanceof LocalDate) {
                    return (int) ((LocalDate) value).toEpochDay();
                }
                break;
            case TIME_WITHOUT_TIME_ZONE:
                if (value instanceof LocalTime) {
                    LocalTime localTime = (LocalTime) value;
                    return (int) (localTime.toNanoOfDay() / 1_000_000);
                }
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                if (value instanceof LocalDateTime) {
                    return TimestampData.fromLocalDateTime((LocalDateTime) value);
                }
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (value instanceof Instant) {
                    return TimestampData.fromInstant((Instant) value);
                } else if (value instanceof OffsetDateTime) {
                    OffsetDateTime offsetDateTime = (OffsetDateTime) value;
                    return TimestampData.fromInstant(offsetDateTime.toInstant());
                }
                break;
        }

        // Default fallback - try to return as is with a warning
        LOG.warn(
                "Using direct value for field of type {} as no suitable conversion found",
                fieldType);
        return value;
    }
}
