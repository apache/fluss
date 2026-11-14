/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.fluss.row;

import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.BooleanNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.NullNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.DateTimeUtils;

import java.io.Serializable;
import java.util.Base64;
import java.util.List;

import static org.apache.fluss.types.DataTypeChecks.getPrecision;

/**
 * Converters for rendering fields of an {@link InternalRow} as human-readable strings, used by
 * {@link InternalRow#createFieldToString(DataType, int)}.
 *
 * <p>Rendering a field happens in one of three contexts:
 *
 * <ul>
 *   <li>a top-level scalar field is rendered as a plain string (no quotes);
 *   <li>a top-level ARRAY/MAP/ROW field is rendered as JSON text;
 *   <li>a leaf value inside a nested structure is rendered as a typed JSON node, so that numbers
 *       and booleans keep their JSON types (e.g. {@code [1,"a"]} and not {@code ["1","a"]}).
 * </ul>
 */
final class FieldToStringConverters {

    /**
     * Serializes JSON node trees with {@code WRITE_BIGDECIMAL_AS_PLAIN} enabled, so that small
     * DECIMAL values render plain (e.g. {@code 0.00000025}) instead of in scientific notation (e.g.
     * {@code 2.5E-7}), matching the top-level rendering of {@link Decimal#toString()}.
     */
    private static final ObjectMapper JSON_MAPPER =
            new ObjectMapper().configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);

    private FieldToStringConverters() {}

    private static String toJsonString(JsonNode node) {
        try {
            return JSON_MAPPER.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new FlussRuntimeException("Failed to render JSON node as string", e);
        }
    }

    static InternalRow.FieldToString createFieldToString(DataType fieldType, int fieldPos) {
        // copy(true) forces the null-checking wrapper regardless of the declared nullability.
        final InternalRow.FieldGetter fieldGetter =
                InternalRow.createFieldGetter(fieldType.copy(true), fieldPos);
        final ValueToString valueToString = createValueToString(fieldType);
        return row -> {
            try {
                Object value = fieldGetter.getFieldOrNull(row);
                return value == null ? null : valueToString.convert(value);
            } catch (RuntimeException e) {
                throw new IllegalArgumentException(
                        String.format(
                                "Failed to render field at position %d (%s).", fieldPos, fieldType),
                        e);
            }
        };
    }

    /** Converts a non-null internal-representation value to its display string. */
    private interface ValueToString extends Serializable {

        String convert(Object value);
    }

    /** Converts a non-null internal-representation value to a typed JSON node. */
    private interface ValueToNode extends Serializable {

        JsonNode toNode(Object value);
    }

    private static ValueToString createValueToString(DataType fieldType) {
        // ordered by type root definition
        switch (fieldType.getTypeRoot()) {
            case CHAR:
            case STRING:
            case BOOLEAN:
                // Callers pass non-null values only: null fields are handled by the wrapper
                // lambda in createFieldToString before this point.
                return Object::toString;
            case BINARY:
            case BYTES:
                return value -> Base64.getEncoder().encodeToString((byte[]) value);
            case DECIMAL:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                return Object::toString;
            case DATE:
                return value -> DateTimeUtils.formatDate((Integer) value);
            case TIME_WITHOUT_TIME_ZONE:
                {
                    // the internal representation holds milliseconds of the day, so the value
                    // never carries more than 3 fractional digits: for p > 3 the trailing-zero
                    // trimming of the formatter keeps the output to at most 3 digits (.120
                    // renders as .12)
                    final int p = getPrecision(fieldType);
                    return value -> DateTimeUtils.formatTime((Integer) value, p);
                }
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                {
                    final int p = getPrecision(fieldType);
                    return value -> DateTimeUtils.formatTimestampNtz((TimestampNtz) value, p);
                }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                {
                    final int p = getPrecision(fieldType);
                    return value -> DateTimeUtils.formatTimestampLtz((TimestampLtz) value, p);
                }
            case ARRAY:
            case MAP:
            case ROW:
                {
                    // complex types are rendered as JSON
                    final ValueToNode valueToNode = createValueToNode(fieldType);
                    return value -> toJsonString(valueToNode.toNode(value));
                }
            default:
                throw new IllegalArgumentException("Illegal type: " + fieldType);
        }
    }

    private static ValueToNode createValueToNode(DataType fieldType) {
        // ordered by type root definition
        switch (fieldType.getTypeRoot()) {
            case CHAR:
            case STRING:
                return textNodeOf(fieldType);
            case BOOLEAN:
                return value -> BooleanNode.valueOf((Boolean) value);
            case BINARY:
            case BYTES:
                return textNodeOf(fieldType);
            case DECIMAL:
                return value ->
                        JsonNodeFactory.instance.numberNode(((Decimal) value).toBigDecimal());
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                // integral types are rendered as JSON numbers, uniformly as long
                return value -> JsonNodeFactory.instance.numberNode(((Number) value).longValue());
            case FLOAT:
                return value -> JsonNodeFactory.instance.numberNode((Float) value);
            case DOUBLE:
                return value -> JsonNodeFactory.instance.numberNode((Double) value);
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return textNodeOf(fieldType);
            case ARRAY:
                return arrayToNode((ArrayType) fieldType);
            case MAP:
                return mapToNode((MapType) fieldType);
            case ROW:
                return rowToNode((RowType) fieldType);
            default:
                throw new IllegalArgumentException("Illegal type: " + fieldType);
        }
    }

    private static ValueToNode textNodeOf(DataType fieldType) {
        final ValueToString scalarToString = createValueToString(fieldType);
        return value -> JsonNodeFactory.instance.textNode(scalarToString.convert(value));
    }

    private static ValueToNode arrayToNode(ArrayType arrayType) {
        final DataType elementType = arrayType.getElementType();
        final InternalArray.ElementGetter elementGetter =
                InternalArray.createElementGetter(elementType.copy(true));
        final ValueToNode elementToNode = createValueToNode(elementType);
        return value -> {
            InternalArray array = (InternalArray) value;
            ArrayNode node = JsonNodeFactory.instance.arrayNode();
            for (int i = 0; i < array.size(); i++) {
                try {
                    Object element = elementGetter.getElementOrNull(array, i);
                    node.add(
                            element == null
                                    ? NullNode.getInstance()
                                    : elementToNode.toNode(element));
                } catch (RuntimeException e) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Failed to render array element %d (%s).", i, elementType),
                            e);
                }
            }
            return node;
        };
    }

    private static ValueToNode mapToNode(MapType mapType) {
        final DataType keyType = mapType.getKeyType();
        final DataType valueType = mapType.getValueType();
        final InternalArray.ElementGetter keyGetter =
                InternalArray.createElementGetter(keyType.copy(true));
        final ValueToString keyToString = createValueToString(keyType);
        final InternalArray.ElementGetter valueGetter =
                InternalArray.createElementGetter(valueType.copy(true));
        final ValueToNode valueToNode = createValueToNode(valueType);
        return value -> {
            InternalMap map = (InternalMap) value;
            InternalArray keys = map.keyArray();
            InternalArray values = map.valueArray();
            ObjectNode node = JsonNodeFactory.instance.objectNode();
            for (int i = 0; i < map.size(); i++) {
                try {
                    Object key = keyGetter.getElementOrNull(keys, i);
                    Object mapValue = valueGetter.getElementOrNull(values, i);
                    node.set(
                            key == null ? "null" : keyToString.convert(key),
                            mapValue == null
                                    ? NullNode.getInstance()
                                    : valueToNode.toNode(mapValue));
                } catch (RuntimeException e) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Failed to render map entry %d (key: %s, value: %s).",
                                    i, keyType, valueType),
                            e);
                }
            }
            return node;
        };
    }

    private static ValueToNode rowToNode(RowType rowType) {
        final List<String> fieldNames = rowType.getFieldNames();
        final int numFields = rowType.getFieldCount();
        final InternalRow.FieldGetter[] fieldGetters = new InternalRow.FieldGetter[numFields];
        final ValueToNode[] fieldToNodes = new ValueToNode[numFields];
        for (int i = 0; i < numFields; i++) {
            fieldGetters[i] = InternalRow.createFieldGetter(rowType.getTypeAt(i).copy(true), i);
            fieldToNodes[i] = createValueToNode(rowType.getTypeAt(i));
        }
        return value -> {
            InternalRow row = (InternalRow) value;
            ObjectNode node = JsonNodeFactory.instance.objectNode();
            for (int i = 0; i < numFields; i++) {
                String fieldName = fieldNames.get(i);
                try {
                    Object field = fieldGetters[i].getFieldOrNull(row);
                    node.set(
                            fieldName,
                            field == null ? NullNode.getInstance() : fieldToNodes[i].toNode(field));
                } catch (RuntimeException e) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Failed to render field '%s' (%s).",
                                    fieldName, rowType.getTypeAt(i)),
                            e);
                }
            }
            return node;
        };
    }
}
