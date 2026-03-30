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
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeChecks;
import org.apache.fluss.types.RowType;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utility class for validating table statistics configuration.
 *
 * <p>This provides simple validation methods that can be called during CREATE TABLE operations to
 * ensure statistics configuration is valid and compatible with the table schema.
 */
@Internal
public class StatisticsConfigUtils {

    private StatisticsConfigUtils() {}

    /**
     * Validates statistics configuration for a table descriptor.
     *
     * @param tableDescriptor the table descriptor to validate
     * @throws InvalidConfigException if the statistics configuration is invalid
     */
    public static void validateStatisticsConfig(TableDescriptor tableDescriptor) {
        Map<String, String> properties = tableDescriptor.getProperties();
        String statisticsColumns = properties.get(ConfigOptions.TABLE_STATISTICS_COLUMNS.key());

        // Not set means statistics disabled - no validation needed
        if (statisticsColumns == null) {
            return;
        }

        RowType rowType = tableDescriptor.getSchema().getRowType();

        // Wildcard means all supported columns - no validation needed
        if ("*".equals(statisticsColumns.trim())) {
            return;
        }

        // Parse using TableConfig's logic via StatisticsColumnsConfig
        Configuration config = new Configuration();
        config.setString(ConfigOptions.TABLE_STATISTICS_COLUMNS.key(), statisticsColumns);
        StatisticsColumnsConfig columnsConfig = new TableConfig(config).getStatisticsColumns();

        if (columnsConfig.getMode() == StatisticsColumnsConfig.Mode.SPECIFIED
                && columnsConfig.getColumns().isEmpty()) {
            throw new InvalidConfigException(
                    "Statistics columns configuration cannot be empty. "
                            + "Use '*' to collect statistics for all supported columns, "
                            + "or remove the property to disable statistics collection.");
        }

        if (columnsConfig.getMode() == StatisticsColumnsConfig.Mode.SPECIFIED) {
            validateColumns(rowType, columnsConfig);
        }
    }

    /**
     * Validates that the specified columns exist in the schema and are of supported types.
     *
     * @param rowType the table schema
     * @param columnsConfig the statistics columns configuration
     * @throws InvalidConfigException if validation fails
     */
    private static void validateColumns(RowType rowType, StatisticsColumnsConfig columnsConfig) {
        Map<String, DataType> columnTypeMap = buildColumnTypeMap(rowType);

        for (String columnName : columnsConfig.getColumns()) {
            // Check if column exists
            if (!columnTypeMap.containsKey(columnName)) {
                throw new InvalidConfigException(
                        String.format(
                                "Column '%s' specified in statistics collection does not exist in table schema",
                                columnName));
            }

            // Check if column type is supported (whitelist approach)
            DataType dataType = columnTypeMap.get(columnName);
            if (!DataTypeChecks.isSupportedStatisticsType(dataType)) {
                throw new InvalidConfigException(
                        String.format(
                                "Column '%s' of type '%s' is not supported for statistics collection. "
                                        + "Supported types are: BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, "
                                        + "FLOAT, DOUBLE, STRING, CHAR, DECIMAL, DATE, TIME, TIMESTAMP, "
                                        + "and TIMESTAMP_LTZ.",
                                columnName, dataType.asSummaryString()));
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
}
