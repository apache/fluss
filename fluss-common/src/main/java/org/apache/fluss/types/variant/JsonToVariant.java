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
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Parses strict JSON into the Parquet Variant binary representation. */
@Internal
final class JsonToVariant {

    private static final JsonFactory JSON_FACTORY =
            new JsonFactory().enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);

    private JsonToVariant() {}

    /**
     * Parses one JSON value, pre-registering names so nested values share stable dictionary IDs.
     */
    static Variant parse(String json) {
        Set<String> names = new TreeSet<>(VariantUtil::compareFieldNames);
        try {
            try (JsonParser parser = JSON_FACTORY.createParser(json)) {
                int depth = 0;
                int nameBytes = 0;
                boolean hasRoot = false;
                JsonToken token;
                while ((token = parser.nextToken()) != null) {
                    if (depth == 0 && !token.isStructEnd()) {
                        checkArgument(!hasRoot, "Expected exactly one JSON value.");
                        hasRoot = true;
                    }
                    if (token.isStructStart()) {
                        depth++;
                    } else if (token.isStructEnd()) {
                        depth--;
                    } else if (token == JsonToken.FIELD_NAME) {
                        String name = parser.currentName();
                        byte[] encodedName = VariantUtil.encodeUtf8(name);
                        if (names.add(name)) {
                            nameBytes =
                                    VariantUtil.checkComponentSize(
                                            (long) nameBytes + encodedName.length);
                            VariantUtil.checkComponentSize(3L + names.size() + nameBytes);
                        }
                    }
                }
                checkArgument(hasRoot, "Expected a JSON value.");
            }
            List<String> dictionary = new ArrayList<>(names);
            byte[] metadata = VariantUtil.buildMetadata(dictionary);
            Map<String, Integer> ids = new HashMap<>();
            for (int i = 0; i < dictionary.size(); i++) {
                ids.put(dictionary.get(i), i);
            }
            try (JsonParser parser = JSON_FACTORY.createParser(json)) {
                parser.nextToken();
                return new Variant(metadata, parseValue(parser, metadata, ids));
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid JSON value", e);
        }
    }

    private static byte[] parseValue(JsonParser parser, byte[] metadata, Map<String, Integer> ids)
            throws IOException {
        switch (parser.currentToken()) {
            case START_OBJECT:
                List<Integer> fieldIds = new ArrayList<>();
                List<byte[]> fields = new ArrayList<>();
                int fieldBytes = 0;
                while (parser.nextToken() != JsonToken.END_OBJECT) {
                    fieldIds.add(ids.get(parser.currentName()));
                    parser.nextToken();
                    fields.add(parseValue(parser, metadata, ids));
                    fieldBytes =
                            VariantUtil.checkComponentSize(
                                    (long) fieldBytes + fields.get(fields.size() - 1).length);
                    VariantUtil.checkComponentSize(3L + 2L * fields.size() + fieldBytes);
                }
                return VariantUtil.encodeObject(metadata, fieldIds, fields);
            case START_ARRAY:
                List<byte[]> elements = new ArrayList<>();
                int elementBytes = 0;
                while (parser.nextToken() != JsonToken.END_ARRAY) {
                    elements.add(parseValue(parser, metadata, ids));
                    elementBytes =
                            VariantUtil.checkComponentSize(
                                    (long) elementBytes + elements.get(elements.size() - 1).length);
                    VariantUtil.checkComponentSize(3L + elements.size() + elementBytes);
                }
                return VariantUtil.encodeArray(elements);
            case VALUE_STRING:
                return VariantUtil.encodeString(VariantUtil.encodeUtf8(parser.getText()));
            case VALUE_NULL:
                return VariantUtil.encodeNull();
            case VALUE_TRUE:
            case VALUE_FALSE:
                return VariantUtil.encodeBoolean(parser.getBooleanValue());
            case VALUE_NUMBER_INT:
            case VALUE_NUMBER_FLOAT:
                return encodeNumber(parser.getText());
            default:
                throw new IllegalArgumentException(
                        "Unexpected JSON token: " + parser.currentToken());
        }
    }

    private static byte[] encodeNumber(String text) {
        if (text.indexOf('e') < 0 && text.indexOf('E') < 0) {
            if (text.indexOf('.') < 0) {
                try {
                    long value = Long.parseLong(text);
                    if (value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE) {
                        return VariantUtil.encodeByte((byte) value);
                    } else if (value >= Short.MIN_VALUE && value <= Short.MAX_VALUE) {
                        return VariantUtil.encodeShort((short) value);
                    } else if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
                        return VariantUtil.encodeInt((int) value);
                    }
                    return VariantUtil.encodeLong(value);
                } catch (NumberFormatException ignored) {
                    // Larger JSON integers can still be represented exactly as Variant decimals.
                }
            }
            BigDecimal decimal = new BigDecimal(text);
            if (decimal.precision() <= 38 && decimal.scale() <= 38) {
                return VariantUtil.encodeDecimal(decimal);
            }
        }
        double value = Double.parseDouble(text);
        checkArgument(Double.isFinite(value), "JSON number is outside the Variant numeric range.");
        return VariantUtil.encodeDouble(value);
    }
}
