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

package org.apache.fluss.cli.util;

import org.apache.fluss.cli.exception.SqlParseException;
import org.apache.fluss.metadata.TablePath;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Utility class for parsing SQL statements and extracting tokens. */
public class SqlParserUtil {

    public static String stripTrailingSemicolon(String sql) {
        String trimmed = sql.trim();
        if (trimmed.endsWith(";")) {
            return trimmed.substring(0, trimmed.length() - 1).trim();
        }
        return trimmed;
    }

    public static String stripQuotes(String value) {
        if (value == null || value.isEmpty()) {
            return value;
        }
        String trimmed = value.trim();
        if (trimmed.length() >= 2) {
            char first = trimmed.charAt(0);
            char last = trimmed.charAt(trimmed.length() - 1);
            if ((first == '\'' && last == '\'')
                    || (first == '"' && last == '"')
                    || (first == '`' && last == '`')) {
                return trimmed.substring(1, trimmed.length() - 1);
            }
        }
        return trimmed;
    }

    public static String[] splitFirstToken(String input) {
        if (input == null) {
            return new String[] {"", ""};
        }
        String trimmed = input.trim();
        if (trimmed.isEmpty()) {
            return new String[] {"", ""};
        }
        int spaceIdx = trimmed.indexOf(' ');
        if (spaceIdx < 0) {
            return new String[] {trimmed, ""};
        }
        return new String[] {trimmed.substring(0, spaceIdx), trimmed.substring(spaceIdx + 1)};
    }

    public static String extractParenthesizedContent(String text) {
        int start = text.indexOf('(');
        int end = text.lastIndexOf(')');
        if (start < 0 || end < 0 || start >= end) {
            return "";
        }
        return text.substring(start + 1, end).trim();
    }

    public static String stripLeadingKeyword(String text, String keyword) {
        if (text == null || keyword == null) {
            return text;
        }
        String trimmed = text.trim();
        String upper = trimmed.toUpperCase();
        String keywordUpper = keyword.toUpperCase();
        if (upper.startsWith(keywordUpper)) {
            return trimmed.substring(keyword.length()).trim();
        }
        return text;
    }

    public static List<String> splitCommaSeparated(String input) {
        List<String> result = new ArrayList<>();
        if (input == null || input.isEmpty()) {
            return result;
        }

        StringBuilder current = new StringBuilder();
        int parenDepth = 0;
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;

        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);

            if (c == '\'' && !inDoubleQuote) {
                inSingleQuote = !inSingleQuote;
            } else if (c == '"' && !inSingleQuote) {
                inDoubleQuote = !inDoubleQuote;
            } else if (c == '(' && !inSingleQuote && !inDoubleQuote) {
                parenDepth++;
            } else if (c == ')' && !inSingleQuote && !inDoubleQuote) {
                parenDepth--;
            } else if (c == ',' && parenDepth == 0 && !inSingleQuote && !inDoubleQuote) {
                result.add(current.toString().trim());
                current.setLength(0);
                continue;
            }
            current.append(c);
        }

        result.add(current.toString().trim());
        return result;
    }

    public static Map<String, String> parseKeyValueMap(String content) {
        Map<String, String> map = new HashMap<>();
        if (content == null || content.trim().isEmpty()) {
            return map;
        }

        List<String> entries = splitCommaSeparated(content);
        for (String entry : entries) {
            int eqIdx = entry.indexOf('=');
            if (eqIdx > 0) {
                String key = entry.substring(0, eqIdx).trim();
                String value = entry.substring(eqIdx + 1).trim();
                map.put(stripQuotes(key), stripQuotes(value));
            }
        }
        return map;
    }

    public static List<String> parseKeyList(String content) {
        if (content == null || content.trim().isEmpty()) {
            return new ArrayList<>();
        }

        String cleaned = content.trim();
        if (cleaned.startsWith("(") && cleaned.endsWith(")")) {
            cleaned = extractParenthesizedContent(cleaned);
        }

        if (cleaned.isEmpty()) {
            return new ArrayList<>();
        }

        List<String> keys = new ArrayList<>();
        for (String entry : splitCommaSeparated(cleaned)) {
            String trimmed = entry.trim();
            if (!trimmed.isEmpty()) {
                keys.add(stripQuotes(trimmed));
            }
        }
        return keys;
    }

    public static TablePath parseTablePath(String tableName) throws SqlParseException {
        if (tableName == null || tableName.isEmpty()) {
            throw new SqlParseException("Table name cannot be empty");
        }

        String cleaned = tableName.trim();

        if (cleaned.startsWith("`")) {
            int secondBacktick = cleaned.indexOf('`', 1);
            if (secondBacktick > 0
                    && cleaned.length() > secondBacktick + 1
                    && cleaned.charAt(secondBacktick + 1) == '.') {
                String db = stripQuotes(cleaned.substring(0, secondBacktick + 1));
                String table = stripQuotes(cleaned.substring(secondBacktick + 2));
                return TablePath.of(db, table);
            }
        }

        String[] parts = cleaned.split("\\.", 2);
        if (parts.length == 2) {
            return TablePath.of(stripQuotes(parts[0]), stripQuotes(parts[1]));
        } else if (parts.length == 1) {
            throw new SqlParseException(
                    "Table name must be in format 'database.table', got: " + tableName);
        } else {
            throw new SqlParseException("Invalid table name format: " + tableName);
        }
    }

    public static TablePath parseTablePathWithDefault(String tableName, String defaultDatabase)
            throws SqlParseException {
        if (tableName == null || tableName.isEmpty()) {
            throw new SqlParseException("Table name cannot be empty");
        }

        String cleaned = stripQuotes(tableName);
        String[] parts = cleaned.split("\\.", 2);

        if (parts.length == 2) {
            return TablePath.of(stripQuotes(parts[0]), stripQuotes(parts[1]));
        } else if (parts.length == 1) {
            if (defaultDatabase == null || defaultDatabase.isEmpty()) {
                throw new SqlParseException(
                        "Table name must be in format 'database.table' or use USING database first");
            }
            return TablePath.of(defaultDatabase, stripQuotes(parts[0]));
        } else {
            throw new SqlParseException("Invalid table name format: " + tableName);
        }
    }

    public static long parseTimestampLiteral(String literal) throws SqlParseException {
        String trimmed = literal.trim();
        try {
            return java.time.Instant.parse(trimmed).toEpochMilli();
        } catch (java.time.format.DateTimeParseException e) {
            try {
                java.time.format.DateTimeFormatter formatter =
                        java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                java.time.LocalDateTime localDateTime =
                        java.time.LocalDateTime.parse(trimmed, formatter);
                return localDateTime.toInstant(java.time.ZoneOffset.UTC).toEpochMilli();
            } catch (Exception ex) {
                throw new SqlParseException("Invalid timestamp literal: " + literal, ex);
            }
        }
    }

    private SqlParserUtil() {}
}
