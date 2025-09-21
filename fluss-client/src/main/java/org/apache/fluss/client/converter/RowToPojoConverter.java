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
package org.apache.fluss.client.converter;

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeChecks;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

/**
 * Converter for scanner path: converts InternalRow (possibly projected) to POJO, leaving
 * non-projected fields as null on the POJO. Validation is done against the full table schema.
 */
public final class RowToPojoConverter<T> {

    private final PojoType<T> pojoType;
    private final RowType tableSchema;
    private final RowType projection;
    private final RowToField[] rowReaders;

    private RowToPojoConverter(PojoType<T> pojoType, RowType tableSchema, RowType projection) {
        this.pojoType = pojoType;
        this.tableSchema = tableSchema;
        this.projection = projection;
        ConverterCommons.validatePojoMatchesTable(pojoType, tableSchema);
        ConverterCommons.validateProjectionSubset(projection, tableSchema);
        this.rowReaders = createRowReaders();
    }

    public static <T> RowToPojoConverter<T> of(
            Class<T> pojoClass, RowType tableSchema, RowType projection) {
        return new RowToPojoConverter<>(PojoType.of(pojoClass), tableSchema, projection);
    }

    public T fromRow(@Nullable InternalRow row) {
        if (row == null) return null;
        try {
            T pojo = pojoType.getDefaultConstructor().newInstance();
            for (int i = 0; i < rowReaders.length; i++) {
                if (!row.isNullAt(i)) {
                    Object v = rowReaders[i].convert(row, i);
                    PojoType.Property prop =
                            pojoType.getProperty(projection.getFieldNames().get(i));
                    if (v != null) {
                        prop.write(pojo, v);
                    }
                }
            }
            return pojo;
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            String message =
                    String.format(
                            "Failed to instantiate POJO class %s using the public default constructor. Cause: %s",
                            pojoType.getPojoClass().getName(), e.getMessage());
            throw new IllegalStateException(message, e);
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to set field on POJO class " + pojoType.getPojoClass().getName(), e);
        }
    }

    private RowToField[] createRowReaders() {
        RowToField[] arr = new RowToField[projection.getFieldCount()];
        for (int i = 0; i < projection.getFieldCount(); i++) {
            String name = projection.getFieldNames().get(i);
            DataType type = projection.getTypeAt(i);
            PojoType.Property prop = requireProperty(name);
            ConverterCommons.validateCompatibility(type, prop);
            arr[i] = createRowReader(type, prop);
        }
        return arr;
    }

    private PojoType.Property requireProperty(String fieldName) {
        PojoType.Property p = pojoType.getProperty(fieldName);
        if (p == null) {
            throw new IllegalArgumentException(
                    "Field '"
                            + fieldName
                            + "' not found in POJO class "
                            + pojoType.getPojoClass().getName()
                            + ".");
        }
        return p;
    }

    private static RowToField createRowReader(DataType fieldType, PojoType.Property prop) {
        switch (fieldType.getTypeRoot()) {
            case BOOLEAN:
                return (row, pos) -> row.getBoolean(pos);
            case TINYINT:
                return (row, pos) -> row.getByte(pos);
            case SMALLINT:
                return (row, pos) -> row.getShort(pos);
            case INTEGER:
                return (row, pos) -> row.getInt(pos);
            case BIGINT:
                return (row, pos) -> row.getLong(pos);
            case FLOAT:
                return (row, pos) -> row.getFloat(pos);
            case DOUBLE:
                return (row, pos) -> row.getDouble(pos);
            case CHAR:
            case STRING:
                return (row, pos) -> {
                    BinaryString s = row.getString(pos);
                    String v = s.toString();
                    if (prop.type == String.class) {
                        if (fieldType.getTypeRoot() == DataTypeRoot.CHAR && v.length() != 1) {
                            throw new IllegalArgumentException(
                                    String.format(
                                            "Field %s expects exactly one character for CHAR type, got length %d.",
                                            prop.name, v.length()));
                        }
                        return v;
                    } else if (prop.type == Character.class) {
                        if (v.isEmpty()) {
                            throw new IllegalArgumentException(
                                    String.format(
                                            "Field %s expects Character, but the string value is empty.",
                                            prop.name));
                        }
                        if (fieldType.getTypeRoot() == DataTypeRoot.CHAR && v.length() != 1) {
                            throw new IllegalArgumentException(
                                    String.format(
                                            "Field %s expects exactly one character for CHAR type, got length %d.",
                                            prop.name, v.length()));
                        }
                        return v.charAt(0);
                    }
                    throw new IllegalArgumentException(
                            String.format(
                                    "Field %s is not a String or Character. Cannot convert from string.",
                                    prop.name));
                };
            case BINARY:
            case BYTES:
                return (row, pos) -> row.getBytes(pos);
            case DECIMAL:
                return (row, pos) -> {
                    DecimalType dt = (DecimalType) fieldType;
                    Decimal d = row.getDecimal(pos, dt.getPrecision(), dt.getScale());
                    return d.toBigDecimal();
                };
            case DATE:
                return (row, pos) -> LocalDate.ofEpochDay(row.getInt(pos));
            case TIME_WITHOUT_TIME_ZONE:
                return (row, pos) -> LocalTime.ofNanoOfDay(row.getInt(pos) * 1_000_000L);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                {
                    final int precision = DataTypeChecks.getPrecision(fieldType);
                    return (row, pos) -> {
                        TimestampNtz t = row.getTimestampNtz(pos, precision);
                        return t.toLocalDateTime();
                    };
                }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                {
                    final int precision = DataTypeChecks.getPrecision(fieldType);
                    return (row, pos) -> {
                        TimestampLtz t = row.getTimestampLtz(pos, precision);
                        if (prop.type == Instant.class) {
                            return t.toInstant();
                        } else if (prop.type == OffsetDateTime.class) {
                            return OffsetDateTime.ofInstant(t.toInstant(), ZoneOffset.UTC);
                        }
                        throw new IllegalArgumentException(
                                String.format(
                                        "Field %s is not an Instant or OffsetDateTime. Cannot convert from TimestampData.",
                                        prop.name));
                    };
                }
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unsupported field type %s for field %s.",
                                fieldType.getTypeRoot(), prop.name));
        }
    }

    private interface RowToField {
        Object convert(InternalRow row, int pos) throws Exception;
    }
}
