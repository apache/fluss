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
import org.apache.fluss.shaded.arrow.org.apache.parquet.variant.VariantArrayBuilder;
import org.apache.fluss.shaded.arrow.org.apache.parquet.variant.VariantBuilder;
import org.apache.fluss.shaded.arrow.org.apache.parquet.variant.VariantObjectBuilder;

import java.math.BigDecimal;

/**
 * Lightweight JSON parser that feeds parquet-variant's {@link VariantBuilder} to construct Variant
 * binary encoding. This replaces the custom two-pass JSON parser previously in VariantBuilder.
 *
 * <p>The parser supports: null, boolean, numbers (integer/decimal/double), strings, objects, and
 * arrays. Number handling: integers are stored as the smallest fitting type (byte/short/int/long);
 * decimals without exponent are stored as DECIMAL; numbers with exponent are stored as DOUBLE.
 */
@Internal
final class JsonToVariant {

    private JsonToVariant() {}

    /** Parses the JSON string and appends the result to the given builder. */
    static void parse(String json, VariantBuilder builder) {
        int[] pos = {0};
        parseValue(json, pos, builder);
    }

    private static void parseValue(String json, int[] pos, VariantBuilder builder) {
        skipWhitespace(json, pos);
        if (pos[0] >= json.length()) {
            builder.appendNull();
            return;
        }
        char c = json.charAt(pos[0]);
        switch (c) {
            case '"':
                builder.appendString(parseStringValue(json, pos));
                break;
            case '{':
                parseObject(json, pos, builder);
                break;
            case '[':
                parseArray(json, pos, builder);
                break;
            case 't':
            case 'f':
                builder.appendBoolean(parseBooleanValue(json, pos));
                break;
            case 'n':
                parseNullValue(json, pos);
                builder.appendNull();
                break;
            default:
                if (c == '-' || (c >= '0' && c <= '9')) {
                    parseNumber(json, pos, builder);
                } else {
                    throw new IllegalArgumentException(
                            "Unexpected character '" + c + "' at position " + pos[0]);
                }
        }
    }

    private static void parseObject(String json, int[] pos, VariantBuilder builder) {
        expect(json, pos, '{');
        VariantObjectBuilder objBuilder = builder.startObject();
        skipWhitespace(json, pos);
        if (pos[0] < json.length() && json.charAt(pos[0]) != '}') {
            parseObjectField(json, pos, objBuilder);
            skipWhitespace(json, pos);
            while (pos[0] < json.length() && json.charAt(pos[0]) == ',') {
                pos[0]++;
                parseObjectField(json, pos, objBuilder);
                skipWhitespace(json, pos);
            }
        }
        expect(json, pos, '}');
        builder.endObject();
    }

    private static void parseObjectField(String json, int[] pos, VariantObjectBuilder objBuilder) {
        skipWhitespace(json, pos);
        String fieldName = parseStringValue(json, pos);
        skipWhitespace(json, pos);
        expect(json, pos, ':');
        objBuilder.appendKey(fieldName);
        parseValue(json, pos, objBuilder);
    }

    private static void parseArray(String json, int[] pos, VariantBuilder builder) {
        expect(json, pos, '[');
        VariantArrayBuilder arrBuilder = builder.startArray();
        skipWhitespace(json, pos);
        if (pos[0] < json.length() && json.charAt(pos[0]) != ']') {
            parseValue(json, pos, arrBuilder);
            skipWhitespace(json, pos);
            while (pos[0] < json.length() && json.charAt(pos[0]) == ',') {
                pos[0]++;
                parseValue(json, pos, arrBuilder);
                skipWhitespace(json, pos);
            }
        }
        expect(json, pos, ']');
        builder.endArray();
    }

    private static void parseNumber(String json, int[] pos, VariantBuilder builder) {
        int start = pos[0];
        boolean hasDecimalPoint = false;
        boolean hasExponent = false;

        if (pos[0] < json.length() && json.charAt(pos[0]) == '-') {
            pos[0]++;
        }
        while (pos[0] < json.length() && json.charAt(pos[0]) >= '0' && json.charAt(pos[0]) <= '9') {
            pos[0]++;
        }
        if (pos[0] < json.length() && json.charAt(pos[0]) == '.') {
            hasDecimalPoint = true;
            pos[0]++;
            while (pos[0] < json.length()
                    && json.charAt(pos[0]) >= '0'
                    && json.charAt(pos[0]) <= '9') {
                pos[0]++;
            }
        }
        if (pos[0] < json.length() && (json.charAt(pos[0]) == 'e' || json.charAt(pos[0]) == 'E')) {
            hasExponent = true;
            pos[0]++;
            if (pos[0] < json.length()
                    && (json.charAt(pos[0]) == '+' || json.charAt(pos[0]) == '-')) {
                pos[0]++;
            }
            while (pos[0] < json.length()
                    && json.charAt(pos[0]) >= '0'
                    && json.charAt(pos[0]) <= '9') {
                pos[0]++;
            }
        }

        String numStr = json.substring(start, pos[0]);
        if (hasDecimalPoint && !hasExponent) {
            builder.appendDecimal(new BigDecimal(numStr));
        } else if (hasExponent) {
            builder.appendDouble(Double.parseDouble(numStr));
        } else {
            try {
                long val = Long.parseLong(numStr);
                if (val >= Byte.MIN_VALUE && val <= Byte.MAX_VALUE) {
                    builder.appendByte((byte) val);
                } else if (val >= Short.MIN_VALUE && val <= Short.MAX_VALUE) {
                    builder.appendShort((short) val);
                } else if (val >= Integer.MIN_VALUE && val <= Integer.MAX_VALUE) {
                    builder.appendInt((int) val);
                } else {
                    builder.appendLong(val);
                }
            } catch (NumberFormatException e) {
                builder.appendDouble(Double.parseDouble(numStr));
            }
        }
    }

    private static boolean parseBooleanValue(String json, int[] pos) {
        if (json.startsWith("true", pos[0])) {
            pos[0] += 4;
            return true;
        } else if (json.startsWith("false", pos[0])) {
            pos[0] += 5;
            return false;
        }
        throw new IllegalArgumentException("Expected boolean at position " + pos[0]);
    }

    private static void parseNullValue(String json, int[] pos) {
        if (json.startsWith("null", pos[0])) {
            pos[0] += 4;
            return;
        }
        throw new IllegalArgumentException("Expected null at position " + pos[0]);
    }

    private static String parseStringValue(String json, int[] pos) {
        expect(json, pos, '"');
        StringBuilder sb = new StringBuilder();
        while (pos[0] < json.length()) {
            char c = json.charAt(pos[0]++);
            if (c == '"') {
                return sb.toString();
            }
            if (c == '\\') {
                if (pos[0] >= json.length()) {
                    throw new IllegalArgumentException("Unexpected end of string");
                }
                char escaped = json.charAt(pos[0]++);
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
                        if (pos[0] + 4 > json.length()) {
                            throw new IllegalArgumentException("Invalid unicode escape");
                        }
                        int codeUnit = Integer.parseInt(json.substring(pos[0], pos[0] + 4), 16);
                        pos[0] += 4;
                        if (Character.isHighSurrogate((char) codeUnit)
                                && pos[0] + 6 <= json.length()
                                && json.charAt(pos[0]) == '\\'
                                && json.charAt(pos[0] + 1) == 'u') {
                            int low = Integer.parseInt(json.substring(pos[0] + 2, pos[0] + 6), 16);
                            if (Character.isLowSurrogate((char) low)) {
                                sb.appendCodePoint(
                                        Character.toCodePoint((char) codeUnit, (char) low));
                                pos[0] += 6;
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

    private static void skipWhitespace(String json, int[] pos) {
        while (pos[0] < json.length() && Character.isWhitespace(json.charAt(pos[0]))) {
            pos[0]++;
        }
    }

    private static void expect(String json, int[] pos, char expected) {
        skipWhitespace(json, pos);
        if (pos[0] >= json.length() || json.charAt(pos[0]) != expected) {
            throw new IllegalArgumentException(
                    "Expected '"
                            + expected
                            + "' at position "
                            + pos[0]
                            + " but got '"
                            + (pos[0] < json.length() ? json.charAt(pos[0]) : "EOF")
                            + "'");
        }
        pos[0]++;
    }
}
