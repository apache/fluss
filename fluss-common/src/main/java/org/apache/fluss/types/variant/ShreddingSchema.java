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

package org.apache.fluss.types.variant;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeParser;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Describes the shredding schema for a Variant column. A shredding schema defines which fields
 * within a Variant value are extracted into independent typed columns for efficient columnar
 * access.
 *
 * <p>Shredded columns are named using the convention {@code $<variantColumnName>.<fieldPath>}. For
 * example, if a Variant column "event" has a shredded field "age", the shredded column name would
 * be "$event.age".
 *
 * <p>The shredding schema is stored as a table property with key {@code
 * variant.shredding.schema.<column_name>}.
 *
 * @since 0.7
 */
@PublicEvolving
public final class ShreddingSchema implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Prefix for shredded column names. */
    public static final String SHREDDED_COLUMN_PREFIX = "$";

    /** Table property key prefix for storing shredding schemas. */
    public static final String PROPERTY_KEY_PREFIX = "variant.shredding.schema.";

    /** The name of the Variant column this shredding schema belongs to. */
    private final String variantColumnName;

    /** The list of fields that have been shredded from the Variant column. */
    private final List<ShreddedField> fields;

    public ShreddingSchema(String variantColumnName, List<ShreddedField> fields) {
        this.variantColumnName =
                checkNotNull(variantColumnName, "variantColumnName must not be null");
        this.fields = Collections.unmodifiableList(new ArrayList<>(checkNotNull(fields)));
    }

    /** Returns the name of the Variant column. */
    public String getVariantColumnName() {
        return variantColumnName;
    }

    /** Returns the list of shredded fields. */
    public List<ShreddedField> getFields() {
        return fields;
    }

    /**
     * Generates the shredded column name for a given field path.
     *
     * @param fieldPath the field path (e.g., "age", "address.city")
     * @return the shredded column name (e.g., "$event.age", "$event.address.city")
     */
    public String shreddedColumnName(String fieldPath) {
        return SHREDDED_COLUMN_PREFIX + variantColumnName + "." + fieldPath;
    }

    /**
     * Extracts the field path from a shredded column name.
     *
     * @param shreddedColumnName the shredded column name (e.g., "$event.age")
     * @return the field path (e.g., "age"), or null if not a valid shredded column name
     */
    public String extractFieldPath(String shreddedColumnName) {
        String prefix = SHREDDED_COLUMN_PREFIX + variantColumnName + ".";
        if (shreddedColumnName.startsWith(prefix)) {
            return shreddedColumnName.substring(prefix.length());
        }
        return null;
    }

    /**
     * Returns the table property key for storing this shredding schema.
     *
     * @return the property key (e.g., "variant.shredding.schema.event")
     */
    public String propertyKey() {
        return PROPERTY_KEY_PREFIX + variantColumnName;
    }

    /**
     * Checks if the given column name is a shredded column name for this Variant column.
     *
     * @param columnName the column name to check
     * @return true if it is a shredded column name for this Variant column
     */
    public boolean isShreddedColumn(String columnName) {
        return columnName.startsWith(SHREDDED_COLUMN_PREFIX + variantColumnName + ".");
    }

    /**
     * Checks if the given column name is any shredded column name (starts with "$").
     *
     * @param columnName the column name to check
     * @return true if it is a shredded column name
     */
    public static boolean isAnyShreddedColumn(String columnName) {
        return columnName.startsWith(SHREDDED_COLUMN_PREFIX);
    }

    // --------------------------------------------------------------------------------------------
    // JSON serialization / deserialization
    // --------------------------------------------------------------------------------------------

    /**
     * Serializes this shredding schema to a JSON string.
     *
     * <p>Format:
     *
     * <pre>{@code
     * {
     *   "variant_column": "event",
     *   "fields": [
     *     {"path": "age", "type": "INT", "column_id": 5},
     *     {"path": "name", "type": "STRING", "column_id": 6}
     *   ]
     * }
     * }</pre>
     */
    public String toJson() {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"variant_column\":\"");
        sb.append(escapeJson(variantColumnName));
        sb.append("\",\"fields\":[");
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) {
                sb.append(",");
            }
            ShreddedField field = fields.get(i);
            sb.append("{\"path\":\"");
            sb.append(escapeJson(field.getFieldPath()));
            sb.append("\",\"type\":\"");
            sb.append(escapeJson(field.getShreddedType().asSerializableString()));
            sb.append("\",\"column_id\":");
            sb.append(field.getColumnId());
            sb.append("}");
        }
        sb.append("]}");
        return sb.toString();
    }

    /**
     * Deserializes a shredding schema from a JSON string.
     *
     * @param json the JSON string
     * @return the deserialized ShreddingSchema
     */
    public static ShreddingSchema fromJson(String json) {
        SimpleJsonReader reader = new SimpleJsonReader(json);
        reader.expect('{');
        String variantColumn = null;
        List<ShreddedField> fields = new ArrayList<>();

        while (!reader.isEndOfObject()) {
            String key = reader.readString();
            reader.expect(':');
            if ("variant_column".equals(key)) {
                variantColumn = reader.readString();
            } else if ("fields".equals(key)) {
                reader.expect('[');
                while (!reader.isEndOfArray()) {
                    reader.expect('{');
                    String path = null;
                    String typeStr = null;
                    int columnId = -1;
                    while (!reader.isEndOfObject()) {
                        String fieldKey = reader.readString();
                        reader.expect(':');
                        if ("path".equals(fieldKey)) {
                            path = reader.readString();
                        } else if ("type".equals(fieldKey)) {
                            typeStr = reader.readString();
                        } else if ("column_id".equals(fieldKey)) {
                            columnId = reader.readInt();
                        } else {
                            reader.skipValue();
                        }
                        reader.skipComma();
                    }
                    reader.expect('}');
                    if (path == null || typeStr == null) {
                        throw new IllegalArgumentException(
                                "Missing 'path' or 'type' in field JSON: " + json);
                    }
                    DataType dataType = DataTypeParser.parse(typeStr);
                    fields.add(new ShreddedField(path, dataType, columnId));
                    reader.skipComma();
                }
                reader.expect(']');
            } else {
                reader.skipValue();
            }
            reader.skipComma();
        }
        reader.expect('}');

        if (variantColumn == null) {
            throw new IllegalArgumentException("Missing 'variant_column' in JSON: " + json);
        }
        return new ShreddingSchema(variantColumn, fields);
    }

    /**
     * Minimal JSON reader for deserializing the shredding schema format. Handles only the subset of
     * JSON produced by {@link #toJson()}: string values, integer values, objects, and arrays.
     */
    private static class SimpleJsonReader {
        private final String json;
        private int pos;

        SimpleJsonReader(String json) {
            this.json = json;
            this.pos = 0;
        }

        void skipWhitespace() {
            while (pos < json.length() && Character.isWhitespace(json.charAt(pos))) {
                pos++;
            }
        }

        void expect(char c) {
            skipWhitespace();
            if (pos >= json.length() || json.charAt(pos) != c) {
                throw new IllegalArgumentException(
                        "Expected '" + c + "' at pos " + pos + " in: " + json);
            }
            pos++;
        }

        void skipComma() {
            skipWhitespace();
            if (pos < json.length() && json.charAt(pos) == ',') {
                pos++;
            }
        }

        boolean isEndOfObject() {
            skipWhitespace();
            return pos < json.length() && json.charAt(pos) == '}';
        }

        boolean isEndOfArray() {
            skipWhitespace();
            return pos < json.length() && json.charAt(pos) == ']';
        }

        String readString() {
            skipWhitespace();
            expect('"');
            StringBuilder sb = new StringBuilder();
            while (pos < json.length()) {
                char c = json.charAt(pos++);
                if (c == '"') {
                    return sb.toString();
                }
                if (c == '\\') {
                    if (pos >= json.length()) {
                        throw new IllegalArgumentException("Unexpected end of string in: " + json);
                    }
                    char escaped = json.charAt(pos++);
                    switch (escaped) {
                        case '"':
                            sb.append('"');
                            break;
                        case '\\':
                            sb.append('\\');
                            break;
                        case '/':
                            sb.append('/');
                            break;
                        case 'n':
                            sb.append('\n');
                            break;
                        case 'r':
                            sb.append('\r');
                            break;
                        case 't':
                            sb.append('\t');
                            break;
                        case 'b':
                            sb.append('\b');
                            break;
                        case 'f':
                            sb.append('\f');
                            break;
                        case 'u':
                            if (pos + 4 > json.length()) {
                                throw new IllegalArgumentException(
                                        "Invalid unicode escape in: " + json);
                            }
                            int codeUnit = Integer.parseInt(json.substring(pos, pos + 4), 16);
                            pos += 4;
                            // Handle surrogate pairs so supplementary code points (e.g. emoji
                            // encoded as \\uD83D\\uDE00) are preserved correctly.
                            if (Character.isHighSurrogate((char) codeUnit)
                                    && pos + 6 <= json.length()
                                    && json.charAt(pos) == '\\'
                                    && json.charAt(pos + 1) == 'u') {
                                int low = Integer.parseInt(json.substring(pos + 2, pos + 6), 16);
                                if (Character.isLowSurrogate((char) low)) {
                                    sb.appendCodePoint(
                                            Character.toCodePoint((char) codeUnit, (char) low));
                                    pos += 6;
                                    break;
                                }
                            }
                            sb.append((char) codeUnit);
                            break;
                        default:
                            sb.append(escaped);
                    }
                } else {
                    sb.append(c);
                }
            }
            throw new IllegalArgumentException("Unterminated string in: " + json);
        }

        int readInt() {
            skipWhitespace();
            int start = pos;
            if (pos < json.length() && json.charAt(pos) == '-') {
                pos++;
            }
            while (pos < json.length() && Character.isDigit(json.charAt(pos))) {
                pos++;
            }
            return Integer.parseInt(json.substring(start, pos));
        }

        void skipValue() {
            skipWhitespace();
            if (pos >= json.length()) {
                return;
            }
            char c = json.charAt(pos);
            if (c == '"') {
                readString();
            } else if (c == '{') {
                expect('{');
                while (!isEndOfObject()) {
                    readString();
                    expect(':');
                    skipValue();
                    skipComma();
                }
                expect('}');
            } else if (c == '[') {
                expect('[');
                while (!isEndOfArray()) {
                    skipValue();
                    skipComma();
                }
                expect(']');
            } else {
                // number or boolean or null - skip until delimiter
                while (pos < json.length()) {
                    char ch = json.charAt(pos);
                    if (ch == ',' || ch == '}' || ch == ']' || Character.isWhitespace(ch)) {
                        break;
                    }
                    pos++;
                }
            }
        }
    }

    // --------------------------------------------------------------------------------------------
    // Helper methods
    // --------------------------------------------------------------------------------------------

    private static String escapeJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ShreddingSchema)) {
            return false;
        }
        ShreddingSchema that = (ShreddingSchema) o;
        return Objects.equals(variantColumnName, that.variantColumnName)
                && Objects.equals(fields, that.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(variantColumnName, fields);
    }

    @Override
    public String toString() {
        return String.format(
                "ShreddingSchema{variantColumn='%s', fields=%s}", variantColumnName, fields);
    }
}
