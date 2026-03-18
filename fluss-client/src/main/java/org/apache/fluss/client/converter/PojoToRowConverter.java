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

import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeChecks;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

/**
 * Converter for writer path: converts POJO instances to Fluss InternalRow according to a (possibly
 * projected) RowType. Validation is done against the full table schema.
 */
public final class PojoToRowConverter<T> {

    private final PojoType<T> pojoType;
    private final RowType tableSchema;
    private final RowType projection;
    private final List<String> projectionFieldNames;
    private final FieldToRow[] fieldConverters; // index corresponds to projection position

    private PojoToRowConverter(PojoType<T> pojoType, RowType tableSchema, RowType projection) {
        this.pojoType = pojoType;
        this.tableSchema = tableSchema;
        this.projection = projection;
        this.projectionFieldNames = projection.getFieldNames();
        // For writer path, allow POJO to be a superset of the projection. It must contain all
        // projected fields.
        ConverterCommons.validatePojoMatchesProjection(pojoType, projection);
        ConverterCommons.validateProjectionSubset(projection, tableSchema);
        this.fieldConverters = createFieldConverters();
    }

    public static <T> PojoToRowConverter<T> of(
            Class<T> pojoClass, RowType tableSchema, RowType projection) {
        return new PojoToRowConverter<>(PojoType.of(pojoClass), tableSchema, projection);
    }

    public GenericRow toRow(@Nullable T pojo) {
        if (pojo == null) {
            return null;
        }
        GenericRow row = new GenericRow(projection.getFieldCount());
        for (int i = 0; i < fieldConverters.length; i++) {
            Object v;
            try {
                v = fieldConverters[i].readAndConvert(pojo);
            } catch (RuntimeException re) {
                throw re;
            } catch (Exception e) {
                throw new IllegalStateException(
                        "Failed to access field '"
                                + projectionFieldNames.get(i)
                                + "' from POJO "
                                + pojoType.getPojoClass().getName(),
                        e);
            }
            row.setField(i, v);
        }
        return row;
    }

    private FieldToRow[] createFieldConverters() {
        FieldToRow[] arr = new FieldToRow[projection.getFieldCount()];
        for (int i = 0; i < projection.getFieldCount(); i++) {
            String fieldName = projectionFieldNames.get(i);
            DataType fieldType = projection.getTypeAt(i);
            PojoType.Property prop = requireProperty(fieldName);
            ConverterCommons.validateCompatibility(fieldType, prop);
            arr[i] = createFieldConverter(prop, fieldType);
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

    private static FieldToRow createFieldConverter(PojoType.Property prop, DataType fieldType) {
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
                return prop::read;
            case CHAR:
            case STRING:
                return (obj) ->
                        PojoTypeToFlussTypeConverter.convertTextValue(
                                fieldType, prop.name, prop.read(obj));
            case DECIMAL:
                return (obj) ->
                        PojoTypeToFlussTypeConverter.convertDecimalValue(
                                (DecimalType) fieldType, prop.name, prop.read(obj));
            case DATE:
                return (obj) ->
                        PojoTypeToFlussTypeConverter.convertDateValue(prop.name, prop.read(obj));
            case TIME_WITHOUT_TIME_ZONE:
                return (obj) ->
                        PojoTypeToFlussTypeConverter.convertTimeValue(prop.name, prop.read(obj));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                {
                    final int precision = DataTypeChecks.getPrecision(fieldType);
                    return (obj) ->
                            PojoTypeToFlussTypeConverter.convertTimestampNtzValue(
                                    precision, prop.name, prop.read(obj));
                }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                {
                    final int precision = DataTypeChecks.getPrecision(fieldType);
                    return (obj) ->
                            PojoTypeToFlussTypeConverter.convertTimestampLtzValue(
                                    precision, prop.name, prop.read(obj));
                }
            case ARRAY:
                return (obj) ->
                        new PojoArrayToFlussArray(prop.read(obj), fieldType, prop.name)
                                .convertArray();
            case MAP:
                return (obj) ->
                        new PojoMapToFlussMap(
                                        (Map<?, ?>) prop.read(obj), (MapType) fieldType, prop.name)
                                .convertMap();
            case ROW:
                {
                    RowType nestedRowType = (RowType) fieldType;
                    @SuppressWarnings("unchecked")
                    PojoToRowConverter<Object> nestedConverter =
                            PojoToRowConverter.of(
                                    (Class<Object>) prop.type, nestedRowType, nestedRowType);
                    return (obj) -> {
                        Object nested = prop.read(obj);
                        return nested == null ? null : nestedConverter.toRow(nested);
                    };
                }
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unsupported field type %s for field %s.",
                                fieldType.getTypeRoot(), prop.name));
        }
    }

    private interface FieldToRow {
        Object readAndConvert(Object pojo) throws Exception;
    }
}
