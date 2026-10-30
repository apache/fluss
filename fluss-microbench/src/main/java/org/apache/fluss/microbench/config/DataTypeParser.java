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

import java.util.Locale;

/** Parses YAML column types using Fluss types. */
public final class DataTypeParser {

    private DataTypeParser() {}

    public static DataType parse(String value) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Column type is required");
        }
        String type = value.trim().toUpperCase(Locale.ROOT);
        if (type.equals("TIMESTAMP_LTZ")) {
            return DataTypes.TIMESTAMP_LTZ();
        }
        if (type.startsWith("TIMESTAMP_LTZ(") && type.endsWith(")")) {
            return DataTypes.TIMESTAMP_LTZ(
                    Integer.parseInt(
                            type.substring("TIMESTAMP_LTZ(".length(), type.length() - 1).trim()));
        }
        try {
            return DataTypes.parse(type);
        } catch (Exception e) {
            throw new IllegalArgumentException("Unknown or unsupported type: " + type, e);
        }
    }
}
