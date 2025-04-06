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

package com.alibaba.fluss.flink.sink.serializer;

import com.alibaba.fluss.annotation.PublicEvolving;

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

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/**
 * A serialization schema that converts POJOs to {@link RowData} for writing to Fluss.
 *
 * <p>This implementation automatically maps POJO fields to table columns based on name matching. It
 * uses reflection to access POJO fields and convert them to the appropriate Flink internal data
 * structures.
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * FlussSink<User> sink = FlussSink.<User>builder()
 *     .setBootstrapServers("localhost:9092")
 *     .setDatabase("fluss")
 *     .setTable("users")
 *     .setSerializer(new PojoSerializationSchema<>(User.class))
 *     .build();
 * }</pre>
 *
 * @param <T> The POJO type to serialize.
 * @since 0.7
 */
@PublicEvolving
public class PojoSerializationSchema<T> implements FlussSerializationSchema<T> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(PojoSerializationSchema.class);

    private final Class<T> pojoClass;
    private com.alibaba.fluss.types.RowType rowType;
    private transient PojoToRowDataConverter<T> converter;

    /**
     * Creates a schema for serializing POJOs to RowData.
     *
     * @param pojoClass The class of POJOs to be serialized
     */
    public PojoSerializationSchema(Class<T> pojoClass) {
        this.pojoClass = checkNotNull(pojoClass, "POJO class must not be null");
    }

    /**
     * Creates a schema for serializing POJOs to RowData.
     *
     * @param pojoClass The class of POJOs to be serialized
     * @param rowType The target row type
     */
    public PojoSerializationSchema(Class<T> pojoClass, com.alibaba.fluss.types.RowType rowType) {
        this.pojoClass = checkNotNull(pojoClass, "POJO class must not be null");
        this.rowType = rowType;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        if (rowType == null) {
            rowType = context.getRowSchema();
            if (rowType == null) {
                throw new IllegalStateException(
                        "Row schema must be provided either in constructor or via context");
            }
        }

        converter = new PojoToRowDataConverter<>(pojoClass, rowType);
        LOG.info(
                "Initialized POJO serialization schema for class {} with schema {}",
                pojoClass.getName(),
                rowType);
    }

    @Override
    public RowData serialize(T value) throws Exception {
        if (value == null) {
            return null;
        }

        if (converter == null) {
            throw new IllegalStateException(
                    "Converter not initialized. The open() method must be called before serializing records.");
        }

        return converter.convert(value);
    }

    /** Converter that transforms POJOs into Flink's RowData using reflection. */
    private static class PojoToRowDataConverter<T> {
        private final Class<T> pojoClass;
        private final com.alibaba.fluss.types.RowType rowType;
        private final Map<String, Field> fieldMap = new HashMap<>();

        public PojoToRowDataConverter(Class<T> pojoClass, com.alibaba.fluss.types.RowType rowType) {
            this.pojoClass = pojoClass;
            this.rowType = rowType;
            initializeFieldMap();
        }

        // Map once during initialization, we can quickly look up the right Field object for each
        // field name during conversion.
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

        public RowData convert(T pojo) throws IllegalAccessException {
            GenericRowData rowData = new GenericRowData(rowType.getFieldCount());

            for (int i = 0; i < rowType.getFieldCount(); i++) {
                String fieldName = rowType.getFieldNames().get(i);
                com.alibaba.fluss.types.DataType fieldType = rowType.getTypeAt(i);

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

        private Object convertToRowDataField(
                Object value, com.alibaba.fluss.types.DataType fieldType) {
            if (value == null) {
                return null;
            }

            // Convert Java types to Flink internal types based on the type root
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
                                (BigDecimal) value,
                                decimalType.getPrecision(),
                                decimalType.getScale());
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
}
