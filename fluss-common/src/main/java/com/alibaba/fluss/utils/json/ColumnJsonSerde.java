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
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import com.alibaba.fluss.types.DataType;

import java.io.IOException;

/** Json serializer and deserializer for {@link Schema.Column}. */
@Internal
public class ColumnJsonSerde
        implements JsonSerializer<Schema.Column>, JsonDeserializer<Schema.Column> {

    public static final ColumnJsonSerde INSTANCE = new ColumnJsonSerde();
    static final String NAME = "name";
    static final String DATA_TYPE = "data_type";
    static final String COMMENT = "comment";

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

        generator.writeEndObject();
    }

    @Override
    public Schema.Column deserialize(JsonNode node) {
        String columnName = node.required(NAME).asText();

        DataType dataType = DataTypeJsonSerde.INSTANCE.deserialize(node.get(DATA_TYPE));
        Schema.Column column = new Schema.Column(columnName, dataType);
        if (node.hasNonNull(COMMENT)) {
            column = column.withComment(node.get(COMMENT).asText());
        }

        return column;
    }
}
