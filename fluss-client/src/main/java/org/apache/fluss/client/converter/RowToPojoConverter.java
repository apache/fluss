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

import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeChecks;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

/**
 * Converter for scanner path: converts InternalRow (possibly projected) to POJO, leaving
 * non-projected fields as null on the POJO. Validation is done against the full table schema.
 */
public final class RowToPojoConverter<T> {

    private final PojoType<T> pojoType;
    private final RowType tableSchema;
    private final RowType projection;
    private final List<String> projectionFieldNames;
    private final RowToField[] rowReaders;

    private RowToPojoConverter(PojoType<T> pojoType, RowType tableSchema, RowType projection) {
        this.pojoType = pojoType;
        this.tableSchema = tableSchema;
        this.projection = projection;
        this.projectionFieldNames = projection.getFieldNames();
        ConverterCommons.validatePojoMatchesTable(pojoType, tableSchema);
        ConverterCommons.validateProjectionSubset(projection, tableSchema);
        this.rowReaders = createRowReaders();
    }

    public static <T> RowToPojoConverter<T> of(
            Class<T> pojoClass, RowType tableSchema, RowType projection) {
        return new RowToPojoConverter<>(PojoType.of(pojoClass), tableSchema, projection);
    }

    public T fromRow(@Nullable InternalRow row) {
        if (row == null) {
            return null;
        }
        try {
            T pojo = pojoType.getDefaultConstructor().newInstance();
            for (int i = 0; i < rowReaders.length; i++) {
                if (!row.isNullAt(i)) {
                    Object v = rowReaders[i].convert(row, i);
                    PojoType.Property prop = pojoType.getProperty(projectionFieldNames.get(i));
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
            String name = projectionFieldNames.get(i);
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
                return InternalRow::getBoolean;
            case TINYINT:
                return InternalRow::getByte;
            case SMALLINT:
                return InternalRow::getShort;
            case INTEGER:
                return InternalRow::getInt;
            case BIGINT:
                return InternalRow::getLong;
            case FLOAT:
                return InternalRow::getFloat;
            case DOUBLE:
                return InternalRow::getDouble;
            case CHAR:
            case STRING:
                return (row, pos) ->
                        FlussTypeToPojoTypeConverter.convertTextValue(
                                fieldType, prop.name, prop.type, row.getString(pos));
            case BINARY:
            case BYTES:
                return InternalRow::getBytes;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) fieldType;
                return (row, pos) ->
                        FlussTypeToPojoTypeConverter.convertDecimalValue(
                                row.getDecimal(
                                        pos, decimalType.getPrecision(), decimalType.getScale()));
            case DATE:
                return (row, pos) -> FlussTypeToPojoTypeConverter.convertDateValue(row.getInt(pos));
            case TIME_WITHOUT_TIME_ZONE:
                return (row, pos) -> FlussTypeToPojoTypeConverter.convertTimeValue(row.getInt(pos));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                {
                    final int precision = DataTypeChecks.getPrecision(fieldType);
                    return (row, pos) ->
                            FlussTypeToPojoTypeConverter.convertTimestampNtzValue(
                                    row.getTimestampNtz(pos, precision));
                }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                {
                    final int precision = DataTypeChecks.getPrecision(fieldType);
                    return (row, pos) ->
                            FlussTypeToPojoTypeConverter.convertTimestampLtzValue(
                                    row.getTimestampLtz(pos, precision), prop.name, prop.type);
                }
            case ARRAY:
                ArrayType arrayType = (ArrayType) fieldType;
                Class<?> componentType = prop.type.getComponentType();
                return (row, pos) -> {
                    InternalArray array = row.getArray(pos);
                    return array == null
                            ? null
                            : new FlussArrayToPojoArray(
                                            array,
                                            arrayType.getElementType(),
                                            prop.name,
                                            prop.type.getComponentType())
                                    .convertArray();
                };
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
