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

package com.alibaba.fluss.utils.json;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.metadata.TableException;
import com.alibaba.fluss.types.ArrayType;
import com.alibaba.fluss.types.DataField;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DataTypeRoot;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.DecimalType;
import com.alibaba.fluss.types.LocalZonedTimestampType;
import com.alibaba.fluss.types.MapType;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.types.TimeType;
import com.alibaba.fluss.types.TimestampType;

import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.alibaba.fluss.types.DataTypeChecks.getLength;

/** Json serializer and deserializer for {@link DataType}. */
@Internal
public class DataTypeJsonSerde implements JsonSerializer<DataType>, JsonDeserializer<DataType> {

    public static final DataTypeJsonSerde INSTANCE = new DataTypeJsonSerde();

    // Common fields
    static final String FIELD_NAME_TYPE_NAME = "type";
    static final String FIELD_NAME_NULLABLE = "nullable";

    // CHAR, VARCHAR, BINARY, VARBINARY
    static final String FIELD_NAME_LENGTH = "length";

    // TIMESTAMP_WITHOUT_TIME_ZONE, TIMESTAMP_WITH_LOCAL_TIME_ZONE, DECIMAL
    static final String FIELD_NAME_PRECISION = "precision";

    // DECIMAL
    static final String FILED_NAME_SCALE = "scale";

    // ARRAY
    static final String FIELD_NAME_ELEMENT_TYPE = "element_type";

    // MAP
    static final String FIELD_NAME_KEY_TYPE = "key_type";
    static final String FIELD_NAME_VALUE_TYPE = "value_type";

    // ROW
    static final String FIELD_NAME_FIELDS = "fields";
    static final String FIELD_NAME_FIELD_NAME = "name";
    static final String FIELD_NAME_FIELD_TYPE = "field_type";
    static final String FIELD_NAME_FIELD_DESCRIPTION = "description";

    @Override
    public void serialize(DataType dataType, JsonGenerator generator) throws IOException {
        serializeTypeWithGenericSerialization(dataType, generator);
    }

    @Override
    public DataType deserialize(JsonNode node) {
        return deserializeWithExtendedSerialization(node);
    }

    // --------------------------------------------------------------------------------------------
    // Generic Serialization
    // --------------------------------------------------------------------------------------------

    private static void serializeTypeWithGenericSerialization(
            DataType dataType, JsonGenerator jsonGenerator) throws IOException {
        jsonGenerator.writeStartObject();

        jsonGenerator.writeStringField(FIELD_NAME_TYPE_NAME, dataType.getTypeRoot().name());
        if (!dataType.isNullable()) {
            jsonGenerator.writeBooleanField(FIELD_NAME_NULLABLE, false);
        }

        switch (dataType.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DATE:
            case BYTES:
            case STRING:
                // do nothing.
                break;
            case CHAR:
            case BINARY:
                jsonGenerator.writeNumberField(FIELD_NAME_LENGTH, getLength(dataType));
                break;
            case DECIMAL:
                final DecimalType decimalType = (DecimalType) dataType;
                serializeDecimal(decimalType.getPrecision(), decimalType.getScale(), jsonGenerator);
                break;
            case TIME_WITHOUT_TIME_ZONE:
                final TimeType timeType = (TimeType) dataType;
                jsonGenerator.writeNumberField(FIELD_NAME_PRECISION, timeType.getPrecision());
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final TimestampType timestampType = (TimestampType) dataType;
                serializeTimestamp(timestampType.getPrecision(), jsonGenerator);
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final LocalZonedTimestampType localZonedTimestampType =
                        (LocalZonedTimestampType) dataType;
                serializeTimestamp(localZonedTimestampType.getPrecision(), jsonGenerator);
                break;
            case ARRAY:
                serializeCollection(((ArrayType) dataType).getElementType(), jsonGenerator);
                break;
            case MAP:
                serializeMap((MapType) dataType, jsonGenerator);
                break;
            case ROW:
                serializeRow((RowType) dataType, jsonGenerator);
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unable to serialize logical type '%s'. Please check the documentation for supported types.",
                                dataType.asSummaryString()));
        }

        jsonGenerator.writeEndObject();
    }

    private static void serializeTimestamp(int precision, JsonGenerator jsonGenerator)
            throws IOException {
        jsonGenerator.writeNumberField(FIELD_NAME_PRECISION, precision);
    }

    private static void serializeDecimal(int precision, int scale, JsonGenerator jsonGenerator)
            throws IOException {
        jsonGenerator.writeNumberField(FIELD_NAME_PRECISION, precision);
        jsonGenerator.writeNumberField(FILED_NAME_SCALE, scale);
    }

    private static void serializeCollection(DataType elementType, JsonGenerator jsonGenerator)
            throws IOException {
        jsonGenerator.writeFieldName(FIELD_NAME_ELEMENT_TYPE);
        serializeInternal(elementType, jsonGenerator);
    }

    private static void serializeMap(MapType mapType, JsonGenerator jsonGenerator)
            throws IOException {
        jsonGenerator.writeFieldName(FIELD_NAME_KEY_TYPE);
        serializeInternal(mapType.getKeyType(), jsonGenerator);
        jsonGenerator.writeFieldName(FIELD_NAME_VALUE_TYPE);
        serializeInternal(mapType.getValueType(), jsonGenerator);
    }

    private static void serializeInternal(DataType datatype, JsonGenerator jsonGenerator)
            throws IOException {
        serializeTypeWithGenericSerialization(datatype, jsonGenerator);
    }

    private static void serializeRow(RowType dataType, JsonGenerator jsonGenerator)
            throws IOException {
        jsonGenerator.writeArrayFieldStart(FIELD_NAME_FIELDS);
        for (DataField dataField : dataType.getFields()) {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField(FIELD_NAME_FIELD_NAME, dataField.getName());
            jsonGenerator.writeFieldName(FIELD_NAME_FIELD_TYPE);
            serializeInternal(dataField.getType(), jsonGenerator);
            if (dataField.getDescription().isPresent()) {
                jsonGenerator.writeStringField(
                        FIELD_NAME_FIELD_DESCRIPTION, dataField.getDescription().get());
            }
            jsonGenerator.writeEndObject();
        }
        jsonGenerator.writeEndArray();
    }

    // --------------------------------------------------------------------------------------------
    // Generic Deserialization
    // --------------------------------------------------------------------------------------------

    private static DataType deserializeWithExtendedSerialization(JsonNode logicalTypeNode) {
        final DataType dataType = deserializeFromRoot(logicalTypeNode);
        if (logicalTypeNode.has(FIELD_NAME_NULLABLE)) {
            final boolean isNullable = logicalTypeNode.get(FIELD_NAME_NULLABLE).asBoolean();
            return dataType.copy(isNullable);
        }
        return dataType.copy(true);
    }

    private static DataType deserializeFromRoot(JsonNode dataTypeNode) {
        final DataTypeRoot typeRoot =
                DataTypeRoot.valueOf(dataTypeNode.get(FIELD_NAME_TYPE_NAME).asText());
        switch (typeRoot) {
            case BOOLEAN:
                return DataTypes.BOOLEAN();
            case TINYINT:
                return DataTypes.TINYINT();
            case SMALLINT:
                return DataTypes.SMALLINT();
            case INTEGER:
                return DataTypes.INT();
            case BIGINT:
                return DataTypes.BIGINT();
            case FLOAT:
                return DataTypes.FLOAT();
            case DOUBLE:
                return DataTypes.DOUBLE();
            case DATE:
                return DataTypes.DATE();
            case TIME_WITHOUT_TIME_ZONE:
                return DataTypes.TIME(dataTypeNode.get(FIELD_NAME_PRECISION).asInt());
            case STRING:
                return DataTypes.STRING();
            case BYTES:
                return DataTypes.BYTES();
            case BINARY:
                final int binaryLength = dataTypeNode.get(FIELD_NAME_LENGTH).asInt();
                return DataTypes.BINARY(binaryLength);
            case CHAR:
                final int charLength = dataTypeNode.get(FIELD_NAME_LENGTH).asInt();
                return DataTypes.CHAR(charLength);
            case DECIMAL:
                return DataTypes.DECIMAL(
                        dataTypeNode.get(FIELD_NAME_PRECISION).asInt(),
                        dataTypeNode.get(FILED_NAME_SCALE).asInt());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return deserializeTimestamp(typeRoot, dataTypeNode);
            case ARRAY:
                return deserializeCollection(typeRoot, dataTypeNode);
            case MAP:
                return deserializeMap(dataTypeNode);
            case ROW:
                return deserializeRow(dataTypeNode);
            default:
                throw new UnsupportedOperationException("Unsupported type root: " + typeRoot);
        }
    }

    private static DataType deserializeTimestamp(DataTypeRoot typeRoot, JsonNode dataTypeNode) {
        final int precision = dataTypeNode.get(FIELD_NAME_PRECISION).asInt();
        switch (typeRoot) {
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return new TimestampType(true, precision);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return new LocalZonedTimestampType(true, precision);
            default:
                throw new UnsupportedOperationException(
                        "Timestamp type root expected, unsupported type root: " + typeRoot);
        }
    }

    private static DataType deserializeCollection(DataTypeRoot typeRoot, JsonNode dataTypeNode) {
        final JsonNode elementNode = dataTypeNode.get(FIELD_NAME_ELEMENT_TYPE);
        final DataType elementType = DataTypeJsonSerde.INSTANCE.deserialize(elementNode);
        if (typeRoot == DataTypeRoot.ARRAY) {
            return new ArrayType(elementType);
        }
        throw new TableException("Collection type root expected.");
    }

    private static DataType deserializeMap(JsonNode dataTypeNode) {
        final JsonNode keyNode = dataTypeNode.get(FIELD_NAME_KEY_TYPE);
        final DataType keyType = DataTypeJsonSerde.INSTANCE.deserialize(keyNode);
        final JsonNode valueNode = dataTypeNode.get(FIELD_NAME_VALUE_TYPE);
        final DataType valueType = DataTypeJsonSerde.INSTANCE.deserialize(valueNode);
        return new MapType(keyType, valueType);
    }

    private static DataType deserializeRow(JsonNode dataTypeNode) {
        final ArrayNode fieldNodes = (ArrayNode) dataTypeNode.get(FIELD_NAME_FIELDS);
        final List<DataField> fields = new ArrayList<>();
        for (JsonNode fieldNode : fieldNodes) {
            final String fieldName = fieldNode.get(FIELD_NAME_FIELD_NAME).asText();
            final DataType fieldType =
                    DataTypeJsonSerde.INSTANCE.deserialize(fieldNode.get(FIELD_NAME_FIELD_TYPE));
            final String fieldDescription;
            if (fieldNode.has(FIELD_NAME_FIELD_DESCRIPTION)) {
                fieldDescription = fieldNode.get(FIELD_NAME_FIELD_DESCRIPTION).asText();
            } else {
                fieldDescription = null;
            }
            fields.add(new DataField(fieldName, fieldType, fieldDescription));
        }
        return new RowType(fields);
    }
}
