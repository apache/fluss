/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.kafka.format.json;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.KafkaValueConversionLayout;
import org.apache.fluss.kafka.format.KafkaFieldDecoder;
import org.apache.fluss.kafka.schema.KafkaTopicSchemaException;
import org.apache.fluss.kafka.transcode.KafkaRecordEncodingException;
import org.apache.fluss.metadata.KafkaFieldProjection;
import org.apache.fluss.metadata.KafkaTableSchema;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonToken;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.DoubleNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.FloatNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** Decodes a strict UTF-8 JSON object according to the projected Fluss fields. */
@Internal
public final class JsonKafkaFieldDecoder implements KafkaFieldDecoder {
    // Jackson counts both the JSON root and a container at business depth zero. An empty ROW
    // at the maximum business depth is still a container. The nested physical ROW is the root.
    static final int MAX_JSON_NESTING_DEPTH = KafkaTableSchema.MAX_JSON_SCHEMA_NESTING_DEPTH + 2;
    static final int MAX_CONTAINER_ELEMENTS = JsonToFlussConverters.MAX_CONTAINER_ELEMENTS;
    private static final ObjectMapper OBJECT_MAPPER = createObjectMapper();

    private static final DataType JSON_STRING_TYPE = DataTypes.STRING();

    private final KafkaFieldProjection projection;
    private final boolean rescueEnabled;
    private final @Nullable RowType nestedType;
    private final Map<String, Integer> projectedFields = new HashMap<>();
    private final JsonToFlussConverter[] converters;

    /** Creates a JSON decoder and validates every projected field type. */
    public JsonKafkaFieldDecoder(KafkaFieldProjection projection) {
        this(projection, KafkaValueConversionLayout.FLAT);
    }

    /** Creates a decoder whose nested physical ROW represents the JSON object root. */
    public JsonKafkaFieldDecoder(
            KafkaFieldProjection projection, @Nullable KafkaValueConversionLayout layout) {
        this(projection, layout, false);
    }

    /** Creates a decoder with rescue enabled only by the validated fixed system column. */
    public JsonKafkaFieldDecoder(KafkaTableSchema schema) {
        this(
                schema.valueProjection(),
                schema.valueConversionLayout(),
                schema.rescuePosition() >= 0);
    }

    private JsonKafkaFieldDecoder(
            KafkaFieldProjection projection,
            @Nullable KafkaValueConversionLayout layout,
            boolean rescueEnabled) {
        this.rescueEnabled = rescueEnabled;
        if (layout == KafkaValueConversionLayout.NESTED) {
            if (projection.size() != 1
                    || !KafkaTableSchema.VALUE_COLUMN.equals(projection.nameAt(0))
                    || !(projection.dataTypeAt(0) instanceof RowType)) {
                throw new KafkaTopicSchemaException(
                        "Nested Kafka JSON requires exactly one _kafka_value ROW.");
            }
            nestedType = (RowType) projection.dataTypeAt(0);
            List<Integer> positions = new ArrayList<>();
            for (int i = 0; i < nestedType.getFieldCount(); i++) {
                positions.add(i);
            }
            this.projection = new KafkaFieldProjection(nestedType, positions);
        } else {
            nestedType = null;
            this.projection = projection;
        }
        converters = new JsonToFlussConverter[this.projection.size()];
        for (int i = 0; i < this.projection.size(); i++) {
            String name = this.projection.nameAt(i);
            if (KafkaTableSchema.isReservedName(name)) {
                throw new KafkaTopicSchemaException(
                        "Reserved name '" + name + "' cannot be a Kafka JSON business field.");
            }
            if (projectedFields.put(name, i) != null) {
                throw new KafkaTopicSchemaException("Duplicate Kafka JSON field '" + name + "'.");
            }
            converters[i] = JsonToFlussConverters.create(this.projection.dataTypeAt(i));
        }
    }

    @Override
    public Object[] decode(@Nullable byte[] bytes) {
        if (bytes == null) {
            return nullValues();
        }
        try (Reader reader =
                        new InputStreamReader(
                                new ByteArrayInputStream(bytes),
                                StandardCharsets.UTF_8
                                        .newDecoder()
                                        .onMalformedInput(CodingErrorAction.REPORT)
                                        .onUnmappableCharacter(CodingErrorAction.REPORT));
                JsonParser parser = OBJECT_MAPPER.getFactory().createParser(reader)) {
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                throw new KafkaRecordEncodingException(
                        "Kafka JSON record value must have an object root.");
            }
            Object[] values =
                    new Object[projection.size() + (rescueEnabled && nestedType == null ? 1 : 0)];
            ObjectNode rescuedFields = rescueEnabled ? OBJECT_MAPPER.createObjectNode() : null;
            JsonNode[] nestedRescues = rescueEnabled ? new JsonNode[projection.size()] : null;
            boolean[] present = new boolean[projection.size()];
            int fieldCount = 0;
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                if (++fieldCount > MAX_CONTAINER_ELEMENTS) {
                    throw new KafkaRecordEncodingException(
                            "Kafka JSON object exceeds the maximum field count of "
                                    + MAX_CONTAINER_ELEMENTS
                                    + ".");
                }
                String fieldName = parser.currentName();
                String path = JsonPath.field(JsonPath.ROOT, fieldName);
                validateUnicode(fieldName, path);
                Integer position = projectedFields.get(fieldName);
                if (position == null || KafkaTableSchema.isReservedName(fieldName)) {
                    if (!rescueEnabled) {
                        throw new KafkaRecordEncodingException(
                                "Invalid Kafka record value at "
                                        + path
                                        + (KafkaTableSchema.isReservedName(fieldName)
                                                ? ": reserved field."
                                                : ": unknown field."));
                    }
                    parser.nextToken();
                    rescuedFields.set(fieldName, readValue(parser, null, path));
                    continue;
                }
                parser.nextToken();
                JsonNode value = readValue(parser, projection.dataTypeAt(position), path);
                if (rescueEnabled) {
                    nestedRescues[position] =
                            extractNestedUnknownFields(
                                    value, projection.dataTypeAt(position), path);
                }
                values[position] = converters[position].convert(value, path);
                present[position] = true;
            }
            if (parser.nextToken() != null) {
                throw new KafkaRecordEncodingException(
                        "Kafka record value is not valid strict UTF-8 JSON: trailing content.");
            }
            for (int i = 0; i < projection.size(); i++) {
                if (!present[i]) {
                    values[i] =
                            converters[i].convert(
                                    null, JsonPath.field(JsonPath.ROOT, projection.nameAt(i)));
                }
            }
            Object[] decoded = values;
            if (nestedType != null) {
                decoded = new Object[rescueEnabled ? 2 : 1];
                decoded[0] = GenericRow.of(values);
            }
            if (rescueEnabled) {
                // Root members retain input order; nested unknowns follow projection order.
                for (int i = 0; i < projection.size(); i++) {
                    if (nestedRescues[i] != null) {
                        rescuedFields.set(projection.nameAt(i), nestedRescues[i]);
                    }
                }
                decoded[decoded.length - 1] =
                        rescuedFields.isEmpty()
                                ? null
                                : BinaryString.fromString(rescuedFields.toString());
            }
            return decoded;
        } catch (KafkaRecordEncodingException e) {
            throw e;
        } catch (IOException | RuntimeException e) {
            throw new KafkaRecordEncodingException(
                    "Kafka record value is not valid strict UTF-8 JSON.", e);
        }
    }

    private JsonNode readValue(JsonParser parser, @Nullable DataType dataType, String path)
            throws IOException {
        if (dataType == null) {
            return readBoundedTree(parser, null, path);
        }
        if (parser.currentToken() == JsonToken.START_OBJECT
                || parser.currentToken() == JsonToken.START_ARRAY) {
            switch (dataType.getTypeRoot()) {
                case ROW:
                case ARRAY:
                case MAP:
                    break;
                default:
                    throw new KafkaRecordEncodingException(
                            "Invalid Kafka record value at " + path + ": expected a JSON scalar.");
            }
        }
        if (parser.currentToken().isNumeric()) {
            // Parse floating-point targets from the original token: BigDecimal loses negative
            // zero, and parsing FLOAT through a double can round twice. Other numeric targets
            // retain exact Jackson integer/BigDecimal nodes and their strict conversion rules.
            switch (dataType.getTypeRoot()) {
                case FLOAT:
                    return FloatNode.valueOf(Float.parseFloat(parser.getText()));
                case DOUBLE:
                    return DoubleNode.valueOf(Double.parseDouble(parser.getText()));
                default:
                    break;
            }
        }
        return readBoundedTree(parser, dataType, path);
    }

    private JsonNode readBoundedTree(JsonParser parser, @Nullable DataType dataType, String path)
            throws IOException {
        // Bound every container while consuming tokens, before allocating excess children.
        // Read each child with its schema so numeric tokens retain their original precision
        // and floating-point sign. The parser also bounds depth and rejects duplicate fields.
        switch (parser.currentToken()) {
            case START_ARRAY:
                if (dataType != null && !(dataType instanceof ArrayType)) {
                    throw new KafkaRecordEncodingException(
                            "Invalid Kafka record value at " + path + ": expected a JSON object.");
                }
                DataType elementType =
                        dataType == null ? null : ((ArrayType) dataType).getElementType();
                ArrayNode array = OBJECT_MAPPER.createArrayNode();
                while (parser.nextToken() != JsonToken.END_ARRAY) {
                    checkContainerSize(array.size() + 1, path);
                    array.add(readValue(parser, elementType, JsonPath.index(path, array.size())));
                }
                return array;
            case START_OBJECT:
                if (dataType instanceof ArrayType) {
                    throw new KafkaRecordEncodingException(
                            "Invalid Kafka record value at " + path + ": expected a JSON array.");
                }
                Map<String, DataType> rowFields = new HashMap<>();
                if (dataType instanceof RowType) {
                    for (DataField field : ((RowType) dataType).getFields()) {
                        rowFields.put(field.getName(), field.getType());
                    }
                }
                ObjectNode object = OBJECT_MAPPER.createObjectNode();
                while (parser.nextToken() != JsonToken.END_OBJECT) {
                    checkContainerSize(object.size() + 1, path);
                    String fieldName = parser.currentName();
                    String fieldPath = JsonPath.field(path, fieldName);
                    validateUnicode(fieldName, fieldPath);
                    DataType fieldType =
                            dataType instanceof MapType
                                    ? ((MapType) dataType).getValueType()
                                    : rowFields.get(fieldName);
                    if (fieldType == null && dataType != null && !rescueEnabled) {
                        throw new KafkaRecordEncodingException(
                                "Invalid Kafka record value at " + fieldPath + ": unknown field.");
                    }
                    parser.nextToken();
                    object.set(fieldName, readValue(parser, fieldType, fieldPath));
                }
                return object;
            default:
                JsonNode value = OBJECT_MAPPER.readTree(parser);
                if (value.isTextual()) {
                    validateUnicode(value.textValue(), path);
                }
                // Jackson can produce a DoubleNode on overflow even in BigDecimal mode.
                // Never turn a rescued numeric token into the JSON string "Infinity".
                if ((value.isDouble() || value.isFloat())
                        && !Double.isFinite(value.doubleValue())) {
                    throw new KafkaRecordEncodingException(
                            "Invalid Kafka record value at "
                                    + path
                                    + ": non-finite or overflowing number.");
                }
                return value;
        }
    }

    private static void validateUnicode(String value, String path) {
        JsonToFlussConverters.validateUnicode(value, path, JSON_STRING_TYPE);
    }

    private static void checkContainerSize(int size, String path) {
        if (size > MAX_CONTAINER_ELEMENTS) {
            throw new KafkaRecordEncodingException(
                    "Invalid Kafka record value at "
                            + path
                            + ": container size exceeds "
                            + MAX_CONTAINER_ELEMENTS
                            + ".");
        }
    }

    private static JsonNode extractNestedUnknownFields(
            JsonNode node, DataType dataType, String path) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (dataType instanceof RowType && node.isObject()) {
            return extractRowUnknownFields((ObjectNode) node, (RowType) dataType, path);
        }
        if (dataType instanceof ArrayType && node.isArray()) {
            return extractArrayUnknownFields((ArrayNode) node, (ArrayType) dataType, path);
        }
        if (dataType instanceof MapType && node.isObject()) {
            return extractMapValueUnknownFields((ObjectNode) node, (MapType) dataType, path);
        }
        return null;
    }

    private static JsonNode extractRowUnknownFields(ObjectNode node, RowType rowType, String path) {
        checkContainerSize(node.size(), path);
        ObjectNode rescuedFields = OBJECT_MAPPER.createObjectNode();
        List<String> fieldsToRemove = new ArrayList<>();
        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            int fieldPosition = rowType.getFieldIndex(field.getKey());
            if (fieldPosition < 0) {
                rescuedFields.set(field.getKey(), field.getValue());
                fieldsToRemove.add(field.getKey());
            } else {
                JsonNode nestedRescue =
                        extractNestedUnknownFields(
                                field.getValue(),
                                rowType.getTypeAt(fieldPosition),
                                JsonPath.field(path, field.getKey()));
                if (nestedRescue != null) {
                    rescuedFields.set(field.getKey(), nestedRescue);
                }
            }
        }
        for (String fieldName : fieldsToRemove) {
            node.remove(fieldName);
        }
        return rescuedFields.isEmpty() ? null : rescuedFields;
    }

    private static JsonNode extractArrayUnknownFields(
            ArrayNode node, ArrayType arrayType, String path) {
        checkContainerSize(node.size(), path);
        ArrayNode rescuedElements = OBJECT_MAPPER.createArrayNode();
        boolean hasRescuedElement = false;
        int elementPosition = 0;
        for (JsonNode element : node) {
            JsonNode rescuedElement =
                    extractNestedUnknownFields(
                            element,
                            arrayType.getElementType(),
                            JsonPath.index(path, elementPosition));
            if (rescuedElement == null) {
                rescuedElements.addNull();
            } else {
                rescuedElements.add(rescuedElement);
                hasRescuedElement = true;
            }
            elementPosition++;
        }
        return hasRescuedElement ? rescuedElements : null;
    }

    private static JsonNode extractMapValueUnknownFields(
            ObjectNode node, MapType mapType, String path) {
        checkContainerSize(node.size(), path);
        ObjectNode rescuedValues = OBJECT_MAPPER.createObjectNode();
        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            JsonNode rescuedValue =
                    extractNestedUnknownFields(
                            field.getValue(),
                            mapType.getValueType(),
                            JsonPath.field(path, field.getKey()));
            if (rescuedValue != null) {
                rescuedValues.set(field.getKey(), rescuedValue);
            }
        }
        return rescuedValues.isEmpty() ? null : rescuedValues;
    }

    private Object[] nullValues() {
        if (nestedType != null) {
            if (!nestedType.isNullable()) {
                throw new KafkaRecordEncodingException(
                        "Kafka null value cannot populate NOT NULL field '_kafka_value'.");
            }
            return new Object[rescueEnabled ? 2 : 1];
        }
        Object[] values = new Object[projection.size() + (rescueEnabled ? 1 : 0)];
        for (int i = 0; i < projection.size(); i++) {
            DataType dataType = projection.dataTypeAt(i);
            if (!dataType.isNullable()) {
                throw new KafkaRecordEncodingException(
                        "Kafka null value cannot populate NOT NULL field '"
                                + projection.nameAt(i)
                                + "'.");
            }
        }
        return values;
    }

    private static ObjectMapper createObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.getFactory()
                .setStreamReadConstraints(
                        StreamReadConstraints.builder()
                                .maxNestingDepth(MAX_JSON_NESTING_DEPTH)
                                .maxNumberLength(StreamReadConstraints.DEFAULT_MAX_NUM_LEN)
                                .maxStringLength(StreamReadConstraints.DEFAULT_MAX_STRING_LEN)
                                .build());
        mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
        mapper.enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
        // Trailing content is checked after the root object, not after individual field values.
        return mapper;
    }
}
