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

package org.apache.fluss.config;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.exception.InvalidConfigException;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.RowType;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Validator for statistics column configurations. This class validates that configured statistics
 * columns exist in the table schema and are of supported data types.
 */
@Internal
public class StatisticsColumnValidator {

    /**
     * Validates the statistics column configuration against the given row type schema.
     *
     * @param columnsConfig the statistics columns configuration string
     * @param rowType the table schema as RowType
     * @throws InvalidConfigException if the configuration is invalid
     */
    public static void validateStatisticsConfig(String columnsConfig, RowType rowType) {
        // Handle wildcard configuration
        if ("*".equals(columnsConfig.trim())) {
            // Wildcard configuration is always valid
            return;
        }

        // Parse and validate specific column names
        List<String> columnNames = parseColumnNames(columnsConfig);
        if (columnNames.isEmpty()) {
            throw new InvalidConfigException(
                    "Statistics columns configuration cannot be empty. Use '*' to collect statistics for all non-binary columns.");
        }

        validateColumns(rowType, columnNames);
    }

    /**
     * Parses comma-separated column names from the configuration string.
     *
     * @param columnsConfig the configuration string
     * @return list of parsed column names
     */
    private static List<String> parseColumnNames(String columnsConfig) {
        return Arrays.stream(columnsConfig.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }

    /**
     * Validates that the specified columns exist in the schema and are of supported types.
     *
     * @param rowType the table schema
     * @param statisticsColumns the list of column names to validate
     * @throws InvalidConfigException if validation fails
     */
    public static void validateColumns(RowType rowType, List<String> statisticsColumns) {
        Map<String, DataType> columnTypeMap = buildColumnTypeMap(rowType);

        for (String columnName : statisticsColumns) {
            // Check if column exists
            if (!columnTypeMap.containsKey(columnName)) {
                throw new InvalidConfigException(
                        String.format(
                                "Column '%s' specified in statistics collection does not exist in table schema",
                                columnName));
            }

            // Check if column type is supported
            DataType dataType = columnTypeMap.get(columnName);
            if (isBinaryType(dataType)) {
                throw new InvalidConfigException(
                        String.format(
                                "Binary column '%s' cannot be included in statistics collection. "
                                        + "Binary and bytes columns are not supported for statistics collection.",
                                columnName));
            }
        }
    }

    /**
     * Builds a map from column name to data type for quick lookup.
     *
     * @param rowType the table schema
     * @return map of column name to data type
     */
    private static Map<String, DataType> buildColumnTypeMap(RowType rowType) {
        return rowType.getFields().stream()
                .collect(Collectors.toMap(DataField::getName, DataField::getType));
    }

    /**
     * Checks if the given data type is a binary type (BINARY or BYTES).
     *
     * @param dataType the data type to check
     * @return true if the data type is binary, false otherwise
     */
    private static boolean isBinaryType(DataType dataType) {
        return dataType.getTypeRoot() == DataTypeRoot.BINARY
                || dataType.getTypeRoot() == DataTypeRoot.BYTES;
    }

    /**
     * Gets the list of non-binary column names from the given row type.
     *
     * @param rowType the table schema
     * @return list of non-binary column names
     */
    public static List<String> getNonBinaryColumns(RowType rowType) {
        return rowType.getFields().stream()
                .filter(field -> !isBinaryType(field.getType()))
                .map(DataField::getName)
                .collect(Collectors.toList());
    }

    /**
     * Determines the effective statistics columns based on configuration and schema.
     *
     * @param columnsConfig the statistics columns configuration string
     * @param rowType the table schema
     * @return list of column names that should have statistics collected
     */
    public static List<String> determineStatisticsColumns(String columnsConfig, RowType rowType) {
        // Validate configuration first
        validateStatisticsConfig(columnsConfig, rowType);

        if ("*".equals(columnsConfig.trim())) {
            // Return all non-binary columns
            return getNonBinaryColumns(rowType);
        } else {
            // Return the parsed column names (already validated)
            return parseColumnNames(columnsConfig);
        }
    }
}
