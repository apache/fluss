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

import org.apache.fluss.annotation.Internal;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builder for constructing {@link Variant} values from JSON or programmatically.
 *
 * <p>The builder collects field names for the metadata dictionary and encodes values into the
 * Parquet Variant Binary Encoding format.
 */
@Internal
public class VariantBuilder {

    private final Map<String, Integer> fieldNameToId = new HashMap<>();
    private final List<String> fieldNames = new ArrayList<>();

    /** Registers a field name and returns its dictionary ID. */
    public int addFieldName(String name) {
        Integer id = fieldNameToId.get(name);
        if (id != null) {
            return id;
        }
        int newId = fieldNames.size();
        fieldNameToId.put(name, newId);
        fieldNames.add(name);
        return newId;
    }

    /** Builds the metadata bytes from the collected field names. */
    public byte[] buildMetadata() {
        if (fieldNames.isEmpty()) {
            return VariantUtil.EMPTY_METADATA;
        }

        // Sort field names and build sorted_strings dictionary
        List<String> sortedNames = new ArrayList<>(fieldNames);
        Collections.sort(sortedNames);

        // Encode metadata: header(1) + dict_size(4) + offsets(4*(dict_size+1)) + string_bytes
        // Header: version in lower nibble (bits 0-3), sorted_strings flag in bit 4
        byte[] headerByte =
                new byte[] {
                    (byte) (VariantUtil.METADATA_VERSION | VariantUtil.METADATA_SORTED_STRINGS_BIT)
                };
        // METADATA_VERSION=1 | METADATA_SORTED_STRINGS_BIT=0x10 -> 0x11

        int dictSize = sortedNames.size();
        ByteArrayOutputStream stringBuf = new ByteArrayOutputStream();
        int[] offsets = new int[dictSize + 1];
        offsets[0] = 0;
        for (int i = 0; i < dictSize; i++) {
            byte[] nameBytes = sortedNames.get(i).getBytes(StandardCharsets.UTF_8);
            stringBuf.write(nameBytes, 0, nameBytes.length);
            offsets[i + 1] = stringBuf.size();
        }

        byte[] stringBytes = stringBuf.toByteArray();

        // header(1) + dict_size(4) + offsets(4*(dictSize+1)) + stringBytes
        int totalSize = 1 + 4 + 4 * (dictSize + 1) + stringBytes.length;
        byte[] metadata = new byte[totalSize];
        ByteBuffer buf = ByteBuffer.wrap(metadata).order(ByteOrder.LITTLE_ENDIAN);
        buf.put(headerByte[0]);
        buf.putInt(dictSize);
        for (int offset : offsets) {
            buf.putInt(offset);
        }
        buf.put(stringBytes);

        // Rebuild fieldNameToId to match sorted order
        fieldNameToId.clear();
        fieldNames.clear();
        for (int i = 0; i < sortedNames.size(); i++) {
            fieldNameToId.put(sortedNames.get(i), i);
            fieldNames.add(sortedNames.get(i));
        }

        return metadata;
    }

    // --------------------------------------------------------------------------------------------
    // JSON parsing
    // --------------------------------------------------------------------------------------------

    /**
     * Parses a JSON string into a Variant.
     *
     * @param json the JSON string to parse
     * @return a new Variant
     */
    public static Variant parseJson(String json) {
        VariantBuilder builder = new VariantBuilder();
        JsonParser parser = new JsonParser(json, builder);
        // Pass 1: parse JSON to collect all field names encountered at every nesting level.
        // The resulting 'value' bytes are discarded; only 'builder.fieldNames' is used.
        parser.parseValue();

        // Pass 2: pre-register field names in sorted order so that during re-encoding every
        // addFieldName() call returns the stable, sorted field-ID that will match the metadata
        // dictionary.  Without this step, field IDs in the value bytes would reflect insertion
        // order, which may differ from the sorted metadata dictionary produced by buildMetadata().
        VariantBuilder finalBuilder = new VariantBuilder();
        // Pre-register all names in sorted order
        List<String> sortedNames = new ArrayList<>(builder.fieldNames);
        Collections.sort(sortedNames);
        for (String name : sortedNames) {
            finalBuilder.addFieldName(name);
        }
        byte[] finalMetadata = finalBuilder.buildMetadata();

        JsonParser finalParser = new JsonParser(json, finalBuilder);
        byte[] finalValue = finalParser.parseValue();

        return new Variant(finalMetadata, finalValue);
    }

    // --------------------------------------------------------------------------------------------
    // Simple recursive-descent JSON parser
    // --------------------------------------------------------------------------------------------

    /** A minimal recursive-descent JSON parser that produces Variant-encoded binary values. */
    private static class JsonParser {
        private final String json;
        private final VariantBuilder builder;
        private int pos;

        JsonParser(String json, VariantBuilder builder) {
            this.json = json;
            this.builder = builder;
            this.pos = 0;
        }

        byte[] parseValue() {
            skipWhitespace();
            if (pos >= json.length()) {
                return VariantUtil.encodeNull();
            }
            char c = json.charAt(pos);
            switch (c) {
                case '"':
                    return parseString();
                case '{':
                    return parseObject();
                case '[':
                    return parseArray();
                case 't':
                case 'f':
                    return parseBoolean();
                case 'n':
                    return parseNull();
                default:
                    if (c == '-' || (c >= '0' && c <= '9')) {
                        return parseNumber();
                    }
                    throw new IllegalArgumentException(
                            "Unexpected character '" + c + "' at position " + pos);
            }
        }

        private byte[] parseString() {
            String str = parseStringValue();
            byte[] strBytes = str.getBytes(StandardCharsets.UTF_8);
            return VariantUtil.encodeString(strBytes);
        }

        private String parseStringValue() {
            expect('"');
            StringBuilder sb = new StringBuilder();
            while (pos < json.length()) {
                char c = json.charAt(pos++);
                if (c == '"') {
                    return sb.toString();
                }
                if (c == '\\') {
                    if (pos >= json.length()) {
                        throw new IllegalArgumentException("Unexpected end of string");
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
                        case 'b':
                            sb.append('\b');
                            break;
                        case 'f':
                            sb.append('\f');
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
                        case 'u':
                            if (pos + 4 > json.length()) {
                                throw new IllegalArgumentException("Invalid unicode escape");
                            }
                            int codeUnit = Integer.parseInt(json.substring(pos, pos + 4), 16);
                            pos += 4;
                            // Handle surrogate pairs: a high surrogate must be followed by
                            // a low surrogate escape to form a valid supplementary code point
                            // (e.g. emoji encoded as \\uD83D\\uDE00 -> U+1F600).
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
                            throw new IllegalArgumentException("Invalid escape: \\" + escaped);
                    }
                } else {
                    sb.append(c);
                }
            }
            throw new IllegalArgumentException("Unterminated string");
        }

        private byte[] parseObject() {
            expect('{');
            skipWhitespace();

            List<Integer> fieldIds = new ArrayList<>();
            List<byte[]> fieldValues = new ArrayList<>();

            if (pos < json.length() && json.charAt(pos) != '}') {
                parseObjectField(fieldIds, fieldValues);
                skipWhitespace();
                while (pos < json.length() && json.charAt(pos) == ',') {
                    pos++; // skip ','
                    parseObjectField(fieldIds, fieldValues);
                    skipWhitespace();
                }
            }
            expect('}');

            return VariantUtil.encodeObject(fieldIds, fieldValues);
        }

        private void parseObjectField(List<Integer> fieldIds, List<byte[]> fieldValues) {
            skipWhitespace();
            String fieldName = parseStringValue();
            skipWhitespace();
            expect(':');
            byte[] value = parseValue();
            int fieldId = builder.addFieldName(fieldName);
            fieldIds.add(fieldId);
            fieldValues.add(value);
        }

        private byte[] parseArray() {
            expect('[');
            skipWhitespace();

            List<byte[]> elements = new ArrayList<>();
            if (pos < json.length() && json.charAt(pos) != ']') {
                elements.add(parseValue());
                skipWhitespace();
                while (pos < json.length() && json.charAt(pos) == ',') {
                    pos++; // skip ','
                    elements.add(parseValue());
                    skipWhitespace();
                }
            }
            expect(']');

            return encodeArray(elements);
        }

        private byte[] parseBoolean() {
            if (json.startsWith("true", pos)) {
                pos += 4;
                return VariantUtil.encodeBoolean(true);
            } else if (json.startsWith("false", pos)) {
                pos += 5;
                return VariantUtil.encodeBoolean(false);
            }
            throw new IllegalArgumentException("Expected boolean at position " + pos);
        }

        private byte[] parseNull() {
            if (json.startsWith("null", pos)) {
                pos += 4;
                return VariantUtil.encodeNull();
            }
            throw new IllegalArgumentException("Expected null at position " + pos);
        }

        private byte[] parseNumber() {
            int start = pos;
            boolean hasDecimalPoint = false;
            boolean hasExponent = false;

            if (pos < json.length() && json.charAt(pos) == '-') {
                pos++;
            }
            while (pos < json.length() && json.charAt(pos) >= '0' && json.charAt(pos) <= '9') {
                pos++;
            }
            if (pos < json.length() && json.charAt(pos) == '.') {
                hasDecimalPoint = true;
                pos++;
                while (pos < json.length() && json.charAt(pos) >= '0' && json.charAt(pos) <= '9') {
                    pos++;
                }
            }
            if (pos < json.length() && (json.charAt(pos) == 'e' || json.charAt(pos) == 'E')) {
                hasExponent = true;
                pos++;
                if (pos < json.length() && (json.charAt(pos) == '+' || json.charAt(pos) == '-')) {
                    pos++;
                }
                while (pos < json.length() && json.charAt(pos) >= '0' && json.charAt(pos) <= '9') {
                    pos++;
                }
            }

            String numStr = json.substring(start, pos);
            if (hasDecimalPoint && !hasExponent) {
                // Decimal point without exponent -> encode as Decimal (matching Paimon behavior)
                BigDecimal decimal = new BigDecimal(numStr);
                return VariantUtil.encodeDecimal(decimal);
            } else if (hasExponent) {
                // Has exponent -> encode as Double
                double d = Double.parseDouble(numStr);
                return VariantUtil.encodeDouble(d);
            } else {
                try {
                    long val = Long.parseLong(numStr);
                    if (val >= Byte.MIN_VALUE && val <= Byte.MAX_VALUE) {
                        return VariantUtil.encodeByte((byte) val);
                    } else if (val >= Short.MIN_VALUE && val <= Short.MAX_VALUE) {
                        return VariantUtil.encodeShort((short) val);
                    } else if (val >= Integer.MIN_VALUE && val <= Integer.MAX_VALUE) {
                        return VariantUtil.encodeInt((int) val);
                    } else {
                        return VariantUtil.encodeLong(val);
                    }
                } catch (NumberFormatException e) {
                    // Fallback to double for very large numbers
                    return VariantUtil.encodeDouble(Double.parseDouble(numStr));
                }
            }
        }

        private void skipWhitespace() {
            while (pos < json.length() && Character.isWhitespace(json.charAt(pos))) {
                pos++;
            }
        }

        private void expect(char expected) {
            skipWhitespace();
            if (pos >= json.length() || json.charAt(pos) != expected) {
                throw new IllegalArgumentException(
                        "Expected '"
                                + expected
                                + "' at position "
                                + pos
                                + " but got '"
                                + (pos < json.length() ? json.charAt(pos) : "EOF")
                                + "'");
            }
            pos++;
            skipWhitespace();
        }

        private byte[] encodeArray(List<byte[]> elements) {
            int n = elements.size();

            // Calculate total data size
            int totalDataSize = 0;
            for (byte[] e : elements) {
                totalDataSize += e.length;
            }

            // Fixed 4-byte format: header(1) + numElements(4) + offsets(4*(n+1)) + data
            int totalSize = 1 + 4 + 4 * (n + 1) + totalDataSize;
            byte[] result = new byte[totalSize];

            // Header: ARRAY with offset_size=4 (3<<2), is_large=true (1<<4)
            result[0] = (byte) (VariantUtil.BASIC_TYPE_ARRAY | (3 << 2) | (1 << 4));
            VariantUtil.writeIntLE(result, 1, n);

            int writePos = 5;

            // Offsets (n+1 entries, last = totalDataSize)
            int dataOffset = 0;
            for (int i = 0; i < n; i++) {
                VariantUtil.writeIntLE(result, writePos, dataOffset);
                writePos += 4;
                dataOffset += elements.get(i).length;
            }
            VariantUtil.writeIntLE(result, writePos, totalDataSize);
            writePos += 4;

            // Element data
            for (int i = 0; i < n; i++) {
                byte[] e = elements.get(i);
                System.arraycopy(e, 0, result, writePos, e.length);
                writePos += e.length;
            }

            return result;
        }
    }
}
