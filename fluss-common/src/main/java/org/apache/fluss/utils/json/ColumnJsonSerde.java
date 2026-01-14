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

package org.apache.fluss.utils.json;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.AggFunction;
import org.apache.fluss.metadata.AggFunctionType;
import org.apache.fluss.metadata.AggFunctions;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.types.DataType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.apache.fluss.metadata.Schema.Column.UNKNOWN_COLUMN_ID;

/** Json serializer and deserializer for {@link Schema.Column}. */
@Internal
public class ColumnJsonSerde
        implements JsonSerializer<Schema.Column>, JsonDeserializer<Schema.Column> {

    public static final ColumnJsonSerde INSTANCE = new ColumnJsonSerde();
    static final String NAME = "name";
    static final String ID = "id";
    static final String DATA_TYPE = "data_type";
    static final String COMMENT = "comment";
    static final String AGG_FUNCTION = "agg_function";
    static final String AGG_FUNCTION_TYPE = "type";
    static final String AGG_FUNCTION_PARAMS = "parameters";

    @Override
    public void serialize(Schema.Column column, JsonGenerator generator) throws IOException {
        generator.writeStartObject();

        // Common fields
        generator.writeStringField(NAME, column.getName());
        generator.writeFieldName(DATA_TYPE);
        DataTypeJsonSerde.INSTANCE.serialize(column.getDataType(), generator);
        if (column.getComment().isPresent()) {
            generator.writeStringField(COMMENT, column.getComment().get());
        }
        if (column.getAggFunction().isPresent()) {
            AggFunction aggFunc = column.getAggFunction().get();
            generator.writeObjectFieldStart(AGG_FUNCTION);
            generator.writeStringField(AGG_FUNCTION_TYPE, aggFunc.getType().toString());
            if (aggFunc.hasParameters()) {
                generator.writeObjectFieldStart(AGG_FUNCTION_PARAMS);
                for (Map.Entry<String, String> entry : aggFunc.getParameters().entrySet()) {
                    generator.writeStringField(entry.getKey(), entry.getValue());
                }
                generator.writeEndObject();
            }
            generator.writeEndObject();
        }
        generator.writeNumberField(ID, column.getColumnId());

        generator.writeEndObject();
    }

    @Override
    public Schema.Column deserialize(JsonNode node) {
        String columnName = node.required(NAME).asText();

        DataType dataType = DataTypeJsonSerde.INSTANCE.deserialize(node.get(DATA_TYPE));

        AggFunction aggFunction = null;
        if (node.hasNonNull(AGG_FUNCTION)) {
            JsonNode aggFuncNode = node.get(AGG_FUNCTION);

            // Parse new format: object with type and parameters
            String typeStr = aggFuncNode.get(AGG_FUNCTION_TYPE).asText();
            AggFunctionType type = AggFunctionType.fromString(typeStr);

            if (type != null) {
                Map<String, String> parameters = null;
                if (aggFuncNode.hasNonNull(AGG_FUNCTION_PARAMS)) {
                    parameters = new HashMap<>();
                    JsonNode paramsNode = aggFuncNode.get(AGG_FUNCTION_PARAMS);
                    Iterator<Map.Entry<String, JsonNode>> fields = paramsNode.fields();
                    while (fields.hasNext()) {
                        Map.Entry<String, JsonNode> entry = fields.next();
                        parameters.put(entry.getKey(), entry.getValue().asText());
                    }
                }
                aggFunction = AggFunctions.of(type, parameters);
            }
        }

        return new Schema.Column(
                columnName,
                dataType,
                node.hasNonNull(COMMENT) ? node.get(COMMENT).asText() : null,
                node.has(ID) ? node.get(ID).asInt() : UNKNOWN_COLUMN_ID,
                aggFunction);
    }
}
