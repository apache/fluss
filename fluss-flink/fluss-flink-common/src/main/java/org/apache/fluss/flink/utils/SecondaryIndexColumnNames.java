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

package org.apache.fluss.flink.utils;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Encoding for secondary-index column names stored in Flink table options. */
@Internal
public final class SecondaryIndexColumnNames {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private SecondaryIndexColumnNames() {}

    /** Encodes column names as a JSON array so every valid name is preserved exactly. */
    public static String encode(List<String> columnNames) {
        validate(columnNames);
        try {
            return OBJECT_MAPPER.writeValueAsString(columnNames);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to encode secondary index column names", e);
        }
    }

    /** Decodes either a JSON string array or a comma-separated list of column names. */
    public static List<String> decode(String value) {
        if (value.trim().startsWith("[")) {
            try {
                String[] columnNames = OBJECT_MAPPER.readValue(value, String[].class);
                List<String> result = Arrays.asList(columnNames);
                validate(result);
                return result;
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException(
                        "Invalid JSON secondary index column list: " + value, e);
            }
        }

        String[] items = value.split(",", -1);
        List<String> result = new ArrayList<>(items.length);
        for (String item : items) {
            String columnName = item.trim();
            if (columnName.isEmpty()) {
                throw new IllegalArgumentException(
                        "Secondary index column list contains an empty column name");
            }
            result.add(columnName);
        }
        validate(result);
        return result;
    }

    private static void validate(List<String> columnNames) {
        if (columnNames == null || columnNames.isEmpty()) {
            throw new IllegalArgumentException(
                    "Secondary index column list must contain at least one column name");
        }
        for (String columnName : columnNames) {
            if (columnName == null || columnName.isEmpty()) {
                throw new IllegalArgumentException(
                        "Secondary index column list contains an empty column name");
            }
        }
    }
}
