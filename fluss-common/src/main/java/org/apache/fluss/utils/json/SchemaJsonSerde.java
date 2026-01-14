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
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/** Json serializer and deserializer for {@link Schema}. */
@Internal
public class SchemaJsonSerde implements JsonSerializer<Schema>, JsonDeserializer<Schema> {

    public static final SchemaJsonSerde INSTANCE = new SchemaJsonSerde();

    private static final String COLUMNS_NAME = "columns";
    private static final String PRIMARY_KEY_NAME = "primary_key";
    private static final String AUTO_INCREMENT_COLUMN_NAME = "auto_increment_column";
    private static final String VERSION_KEY = "version";
    private static final String HIGHEST_FIELD_ID = "highest_field_id";
    private static final int VERSION = 1;

    @Override
    public void serialize(Schema schema, JsonGenerator generator) throws IOException {
        generator.writeStartObject();

        // serialize data version.
        generator.writeNumberField(VERSION_KEY, VERSION);

        // serialize columns name.
        generator.writeArrayFieldStart(COLUMNS_NAME);
        for (Schema.Column column : schema.getColumns()) {
            ColumnJsonSerde.INSTANCE.serialize(column, generator);
        }
        generator.writeEndArray();

        Optional<Schema.PrimaryKey> primaryKey = schema.getPrimaryKey();
        if (primaryKey.isPresent()) {
            generator.writeArrayFieldStart(PRIMARY_KEY_NAME);
            for (String columnName : primaryKey.get().getColumnNames()) {
                generator.writeString(columnName);
            }
            generator.writeEndArray();
        }
        List<String> autoIncrementColumnNames = schema.getAutoIncrementColumnNames();
        if (!autoIncrementColumnNames.isEmpty()) {
            generator.writeArrayFieldStart(AUTO_INCREMENT_COLUMN_NAME);
            for (String columnName : autoIncrementColumnNames) {
                generator.writeString(columnName);
            }
            generator.writeEndArray();
        }

        generator.writeNumberField(HIGHEST_FIELD_ID, schema.getHighestFieldId());

        generator.writeEndObject();
    }

    @Override
    public Schema deserialize(JsonNode node) {
        Iterator<JsonNode> columnJsons = node.get(COLUMNS_NAME).elements();
        List<Schema.Column> columns = new ArrayList<>();
        while (columnJsons.hasNext()) {
            columns.add(ColumnJsonSerde.INSTANCE.deserialize(columnJsons.next()));
        }
        Schema.Builder builder = Schema.newBuilder().fromColumns(columns);

        if (node.has(PRIMARY_KEY_NAME)) {
            Iterator<JsonNode> primaryKeyJsons = node.get(PRIMARY_KEY_NAME).elements();
            List<String> primaryKeys = new ArrayList<>();
            while (primaryKeyJsons.hasNext()) {
                primaryKeys.add(primaryKeyJsons.next().asText());
            }
            builder.primaryKey(primaryKeys);
        }

        if (node.has(AUTO_INCREMENT_COLUMN_NAME)) {
            Iterator<JsonNode> autoIncrementColumnJsons =
                    node.get(AUTO_INCREMENT_COLUMN_NAME).elements();
            while (autoIncrementColumnJsons.hasNext()) {
                builder.enableAutoIncrement(autoIncrementColumnJsons.next().asText());
            }
        }

        if (node.has(HIGHEST_FIELD_ID)) {
            builder.highestFieldId(node.get(HIGHEST_FIELD_ID).asInt());
        }

        return builder.build();
    }
}
