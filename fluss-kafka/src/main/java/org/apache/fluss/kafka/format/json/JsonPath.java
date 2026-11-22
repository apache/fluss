/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.kafka.format.json;

/** Builds bounded, escaped JSON paths for Kafka record errors. */
final class JsonPath {

    static final String ROOT = "$";

    private static final int MAX_SEGMENT_LENGTH = 128;
    private static final int MAX_PATH_LENGTH = 512;
    private static final String TRUNCATED = "...";

    private JsonPath() {}

    static String field(String path, String fieldName) {
        return append(path, "[\"" + escapeAndTruncate(fieldName) + "\"]");
    }

    static String index(String path, int index) {
        return append(path, "[" + index + "]");
    }

    private static String append(String path, String segment) {
        if (path.length() + segment.length() <= MAX_PATH_LENGTH) {
            return path + segment;
        }
        int prefixLength = MAX_PATH_LENGTH - TRUNCATED.length() - segment.length();
        if (prefixLength <= ROOT.length()) {
            return (ROOT + TRUNCATED + segment).substring(0, MAX_PATH_LENGTH);
        }
        return path.substring(0, Math.min(path.length(), prefixLength)) + TRUNCATED + segment;
    }

    private static String escapeAndTruncate(String value) {
        StringBuilder escaped = new StringBuilder(MAX_SEGMENT_LENGTH);
        int contentLimit = MAX_SEGMENT_LENGTH - TRUNCATED.length();
        int index = 0;
        while (index < value.length()) {
            String encodedCharacter;
            char character = value.charAt(index);
            switch (character) {
                case '\\':
                    encodedCharacter = "\\\\";
                    break;
                case '"':
                    encodedCharacter = "\\\"";
                    break;
                case '\b':
                    encodedCharacter = "\\b";
                    break;
                case '\f':
                    encodedCharacter = "\\f";
                    break;
                case '\n':
                    encodedCharacter = "\\n";
                    break;
                case '\r':
                    encodedCharacter = "\\r";
                    break;
                case '\t':
                    encodedCharacter = "\\t";
                    break;
                default:
                    if (character < 0x20) {
                        encodedCharacter = String.format("\\u%04x", (int) character);
                    } else {
                        encodedCharacter = String.valueOf(character);
                    }
            }
            if (escaped.length() + encodedCharacter.length() > contentLimit) {
                break;
            }
            escaped.append(encodedCharacter);
            index++;
        }
        if (index < value.length()) {
            escaped.append(TRUNCATED);
        }
        return escaped.toString();
    }
}
