/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.cli;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import java.util.ArrayList;
import java.util.List;

/**
 * Parses a schema definition string into a {@link Schema}.
 *
 * <p>Format: {@code "col1 TYPE1, col2 TYPE2 NOT NULL, col3 TYPE3"}
 *
 * <p>Uses parenthesis-aware splitting to handle types like {@code DECIMAL(10,2)}. Type strings
 * (including nullability suffix) are parsed by {@link DataTypes#parse(String)}.
 */
@Internal
public final class SchemaParser {

    private SchemaParser() {}

    /**
     * Parse a schema definition string into a Schema.
     *
     * @param schemaStr comma-separated column definitions
     * @param primaryKeys primary key column names (empty list if no primary key)
     * @return the parsed Schema
     */
    public static Schema parseSchema(String schemaStr, List<String> primaryKeys) {
        if (schemaStr == null || schemaStr.trim().isEmpty()) {
            throw new IllegalArgumentException("Schema definition cannot be empty.");
        }

        Schema.Builder builder = Schema.newBuilder();
        List<String> columnDefs = splitRespectingParentheses(schemaStr, ',');

        for (String colDef : columnDefs) {
            String trimmed = colDef.trim();
            if (trimmed.isEmpty()) {
                continue;
            }

            int firstSpace = trimmed.indexOf(' ');
            if (firstSpace < 0) {
                throw new IllegalArgumentException(
                        "Invalid column definition: '"
                                + trimmed
                                + "'. Expected format: 'name TYPE [NOT NULL]'.");
            }

            String name = trimmed.substring(0, firstSpace).trim();
            String typeStr = trimmed.substring(firstSpace + 1).trim();

            DataType dataType = DataTypes.parse(typeStr);
            builder.column(name, dataType);
        }

        if (primaryKeys != null && !primaryKeys.isEmpty()) {
            builder.primaryKey(primaryKeys);
        }

        return builder.build();
    }

    /**
     * Split a string by delimiter, skipping delimiters inside parentheses. Handles types like
     * {@code DECIMAL(10,2)}.
     */
    static List<String> splitRespectingParentheses(String input, char delimiter) {
        List<String> result = new ArrayList<>();
        int depth = 0;
        int start = 0;
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
            } else if (c == delimiter && depth == 0) {
                result.add(input.substring(start, i));
                start = i + 1;
            }
        }
        result.add(input.substring(start));
        return result;
    }
}
