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

package org.apache.fluss.microbench.config;

import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses YAML type strings (e.g. "INT", "DECIMAL(10,2)", "CHAR(255)") into Fluss {@link DataType}
 * objects.
 *
 * <p>This is a thin adapter that normalises the shorthand forms used in perf YAML configs into the
 * canonical SQL-style strings that {@link DataTypes#parse(String)} understands, then delegates to
 * the core parser.
 */
public class DataTypeParser {

    private static final Pattern PARAMETERIZED = Pattern.compile("(\\w+)\\((.+)\\)");

    /**
     * Parses a type string from a perf YAML config into a {@link DataType}.
     *
     * @param typeStr the type string, e.g. "INT", "DECIMAL(10,2)", "TIMESTAMP_LTZ(3)"
     * @return the corresponding {@link DataType}
     * @throws IllegalArgumentException if the type string is not recognised
     */
    public static DataType parse(String typeStr) {
        if (typeStr == null || typeStr.trim().isEmpty()) {
            throw new IllegalArgumentException("Type string must not be null or empty");
        }
        String normalized = typeStr.trim().toUpperCase();

        // Handle TIMESTAMP_LTZ specially: the core parser expects
        // "TIMESTAMP WITH LOCAL TIME ZONE" or "TIMESTAMP_LTZ".
        Matcher m = PARAMETERIZED.matcher(normalized);
        if (m.matches()) {
            String base = m.group(1);
            String params = m.group(2).trim();
            if ("TIMESTAMP_LTZ".equals(base)) {
                int precision = Integer.parseInt(params.trim());
                return DataTypes.TIMESTAMP_LTZ(precision);
            }
            // For other parameterized types, delegate to the core parser directly.
            return parseCore(normalized);
        }

        // Non-parameterized shortcuts.
        if ("TIMESTAMP_LTZ".equals(normalized)) {
            return DataTypes.TIMESTAMP_LTZ();
        }

        return parseCore(normalized);
    }

    private static DataType parseCore(String normalized) {
        try {
            return DataTypes.parse(normalized);
        } catch (Exception e) {
            throw new IllegalArgumentException("Unknown or unsupported type: " + normalized, e);
        }
    }
}
