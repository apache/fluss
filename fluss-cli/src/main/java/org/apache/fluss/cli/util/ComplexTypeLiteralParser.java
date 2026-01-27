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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.cli.util;

import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericMap;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Lightweight parser for complex type literals (ARRAY, MAP, ROW) without Flink Runtime
 * dependencies.
 *
 * <p>Supports:
 *
 * <ul>
 *   <li>ARRAY[val1, val2, ...] or ARRAY(val1, val2, ...)
 *   <li>MAP[key1, val1, key2, val2, ...] or MAP(key1, val1, key2, val2, ...)
 *   <li>ROW(val1, val2, ...) or (val1, val2, ...)
 * </ul>
 */
public class ComplexTypeLiteralParser {

    private static final Pattern ARRAY_PATTERN =
            Pattern.compile("^ARRAY\\s*[\\[\\(](.*)[])]$", Pattern.CASE_INSENSITIVE);
    private static final Pattern MAP_PATTERN =
            Pattern.compile("^MAP\\s*[\\[\\(](.*)[])]$", Pattern.CASE_INSENSITIVE);
    private static final Pattern ROW_PATTERN =
            Pattern.compile("^(?:ROW\\s*)?\\((.*)\\)$", Pattern.CASE_INSENSITIVE);

    /**
     * Parse an ARRAY literal.
     *
     * @param literal SQL literal string (e.g., "ARRAY['a', 'b']")
     * @param arrayType Expected array type
     * @return Parsed GenericArray
     */
    public static Object parseArray(String literal, ArrayType arrayType) {
        Matcher matcher = ARRAY_PATTERN.matcher(literal.trim());
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid ARRAY literal: " + literal);
        }

        String content = matcher.group(1).trim();
        if (content.isEmpty()) {
            return new GenericArray(new Object[0]);
        }

        List<String> elements = splitLiteralElements(content);
        DataType elementType = arrayType.getElementType();
        Object[] values = new Object[elements.size()];

        for (int i = 0; i < elements.size(); i++) {
            values[i] = parseValue(elements.get(i), elementType);
        }

        return new GenericArray(values);
    }

    /**
     * Parse a MAP literal.
     *
     * @param literal SQL literal string (e.g., "MAP['key', 'value']")
     * @param mapType Expected map type
     * @return Parsed GenericMap
     */
    public static Object parseMap(String literal, MapType mapType) {
        Matcher matcher = MAP_PATTERN.matcher(literal.trim());
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid MAP literal: " + literal);
        }

        String content = matcher.group(1).trim();
        if (content.isEmpty()) {
            return new GenericMap(new HashMap<>());
        }

        List<String> elements = splitLiteralElements(content);
        if (elements.size() % 2 != 0) {
            throw new IllegalArgumentException(
                    "MAP literal must have even number of elements (key-value pairs): " + literal);
        }

        DataType keyType = mapType.getKeyType();
        DataType valueType = mapType.getValueType();
        Map<Object, Object> map = new HashMap<>();

        for (int i = 0; i < elements.size(); i += 2) {
            Object key = parseValue(elements.get(i), keyType);
            Object value = parseValue(elements.get(i + 1), valueType);
            map.put(key, value);
        }

        return new GenericMap(map);
    }

    /**
     * Parse a ROW literal.
     *
     * @param literal SQL literal string (e.g., "ROW('Alice', 25)" or "('Alice', 25)")
     * @param rowType Expected row type
     * @return Parsed GenericRow
     */
    public static Object parseRow(String literal, RowType rowType) {
        Matcher matcher = ROW_PATTERN.matcher(literal.trim());
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid ROW literal: " + literal);
        }

        String content = matcher.group(1).trim();
        List<String> elements = splitLiteralElements(content);
        List<DataField> fields = rowType.getFields();

        if (elements.size() != fields.size()) {
            throw new IllegalArgumentException(
                    String.format(
                            "ROW literal has %d values but type expects %d fields: %s",
                            elements.size(), fields.size(), literal));
        }

        GenericRow row = new GenericRow(fields.size());
        for (int i = 0; i < elements.size(); i++) {
            DataType fieldType = fields.get(i).getType();
            Object value = parseValue(elements.get(i), fieldType);
            row.setField(i, value);
        }

        return row;
    }

    /**
     * Parse a single value (handles primitives, strings, NULLs, and nested complex types).
     *
     * @param literal Value literal string
     * @param dataType Expected data type
     * @return Parsed value
     */
    private static Object parseValue(String literal, DataType dataType) {
        literal = literal.trim();

        // Handle NULL
        if (literal.equalsIgnoreCase("NULL")) {
            return null;
        }

        // Handle complex types recursively
        if (dataType instanceof ArrayType) {
            return parseArray(literal, (ArrayType) dataType);
        } else if (dataType instanceof MapType) {
            return parseMap(literal, (MapType) dataType);
        } else if (dataType instanceof RowType) {
            return parseRow(literal, (RowType) dataType);
        }

        // Handle primitive/basic types
        return DataTypeConverter.convertFromString(literal, dataType);
    }

    /**
     * Split comma-separated elements while respecting nested brackets/parentheses and quotes.
     *
     * <p>Examples:
     *
     * <ul>
     *   <li>"'a', 'b', 'c'" → ["'a'", "'b'", "'c'"]
     *   <li>"ARRAY[1,2], ARRAY[3,4]" → ["ARRAY[1,2]", "ARRAY[3,4]"]
     *   <li>"'key', MAP['a',1]" → ["'key'", "MAP['a',1]"]
     * </ul>
     *
     * @param content Content to split
     * @return List of element strings
     */
    private static List<String> splitLiteralElements(String content) {
        List<String> elements = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int depth = 0; // Track nesting depth of brackets/parentheses
        boolean inQuote = false;
        char quoteChar = 0;

        for (int i = 0; i < content.length(); i++) {
            char c = content.charAt(i);

            // Handle quotes
            if ((c == '\'' || c == '"') && (i == 0 || content.charAt(i - 1) != '\\')) {
                if (!inQuote) {
                    inQuote = true;
                    quoteChar = c;
                } else if (c == quoteChar) {
                    inQuote = false;
                }
                current.append(c);
                continue;
            }

            // If inside quote, just append
            if (inQuote) {
                current.append(c);
                continue;
            }

            // Track nesting depth
            if (c == '[' || c == '(') {
                depth++;
                current.append(c);
            } else if (c == ']' || c == ')') {
                depth--;
                current.append(c);
            } else if (c == ',' && depth == 0) {
                // Top-level comma separator
                String element = current.toString().trim();
                if (!element.isEmpty()) {
                    elements.add(element);
                }
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }

        // Add last element
        String element = current.toString().trim();
        if (!element.isEmpty()) {
            elements.add(element);
        }

        return elements;
    }
}
