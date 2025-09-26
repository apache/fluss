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

package org.apache.fluss.server.coordinator.validate;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidConfigException;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeChecks;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Configuration validator for table statistics. This validator ensures that statistics
 * configuration in table properties is valid and compatible with the table schema.
 */
@Internal
public class TableStatisticsConfigValidator implements ConfigValidator {

    @Override
    public String getName() {
        return "TableStatisticsConfigValidator";
    }

    @Override
    public ValidationResult validateCreateTable(TableDescriptor tableDescriptor) {
        Map<String, String> tableProperties = tableDescriptor.getProperties();
        Schema schema = tableDescriptor.getSchema();

        return validateStatisticsConfig(tableProperties, schema, "CREATE TABLE");
    }

    @Override
    public ValidationResult validateAlterTable(
            TableDescriptor currentTableDescriptor,
            Configuration alterProperties,
            @Nullable Schema newSchema) {

        // Merge current properties with alter properties to get the final configuration
        Map<String, String> mergedProperties =
                new HashMap<>(currentTableDescriptor.getProperties());

        // Add alter properties (Configuration to Map conversion)
        Map<String, String> alterPropertiesMap = alterProperties.toMap();
        mergedProperties.putAll(alterPropertiesMap);

        // Use new schema if provided, otherwise use current schema
        Schema targetSchema = newSchema != null ? newSchema : currentTableDescriptor.getSchema();

        return validateStatisticsConfig(mergedProperties, targetSchema, "ALTER TABLE");
    }

    @Override
    public ValidationResult validateProperty(String key, String value, Schema schema) {
        List<String> errors = new ArrayList<>();
        List<String> warnings = new ArrayList<>();

        if (handles(key)) {
            // Validate columns configuration using safe method
            RowType rowType = schema.getRowType();
            ValidationResult columnValidationResult = validateStatisticsConfigSafe(value, rowType);

            errors.addAll(columnValidationResult.getErrors());
            warnings.addAll(columnValidationResult.getWarnings());
        }

        if (!errors.isEmpty()) {
            return ValidationResult.failure(errors, warnings);
        } else {
            return ValidationResult.success(warnings);
        }
    }

    @Override
    public boolean handles(String key) {
        return ConfigOptions.TABLE_STATISTICS_COLUMNS.key().equals(key);
    }

    @Override
    public int getPriority() {
        return 100; // Medium priority
    }

    /**
     * Validates statistics configuration against a schema.
     *
     * @param tableProperties the table properties containing statistics configuration
     * @param schema the table schema
     * @param operation the DDL operation name (for error messages)
     * @return validation result
     */
    private ValidationResult validateStatisticsConfig(
            Map<String, String> tableProperties, Schema schema, String operation) {

        List<String> errors = new ArrayList<>();
        List<String> warnings = new ArrayList<>();

        // Get statistics columns configuration
        String statisticsColumns =
                tableProperties.getOrDefault(ConfigOptions.TABLE_STATISTICS_COLUMNS.key(), "*");

        // Check if statistics are disabled (empty string)
        if (statisticsColumns.isEmpty()) {
            // If statistics are disabled, no further validation needed
            warnings.add("Statistics collection is disabled for this table");
            return ValidationResult.success(warnings);
        }

        // Use the safe validation method
        RowType rowType = schema.getRowType();
        ValidationResult columnValidationResult =
                validateStatisticsConfigSafe(statisticsColumns, rowType);

        // Merge validation results
        errors.addAll(columnValidationResult.getErrors());
        warnings.addAll(columnValidationResult.getWarnings());

        // Don't add duplicate column details - they're already included in
        // validateStatisticsConfigSafe

        // Additional validations
        validateStatisticsCompatibilityWithTableType(tableProperties, schema, errors, warnings);

        if (!errors.isEmpty()) {
            return ValidationResult.failure(errors, warnings);
        } else {
            return ValidationResult.success(warnings);
        }
    }

    /**
     * Validates statistics configuration compatibility with table type and features.
     *
     * @param tableProperties the table properties
     * @param schema the table schema
     * @param errors error list to append to
     * @param warnings warning list to append to
     */
    private void validateStatisticsCompatibilityWithTableType(
            Map<String, String> tableProperties,
            Schema schema,
            List<String> errors,
            List<String> warnings) {

        // Check if table has primary key
        boolean hasPrimaryKey = schema.getPrimaryKey().isPresent();

        // For primary key tables, statistics might have different behavior
        if (hasPrimaryKey) {
            warnings.add(
                    "Table has primary key - statistics collection will work with latest values");
        }

        // Add more compatibility checks as needed
        // For example, check compatibility with:
        // - Partitioned tables
        // - Different storage formats
        // - Replication settings
        // - Data lake integration
    }

    /**
     * Validates statistics configuration without throwing exceptions. Returns a ValidationResult
     * containing any errors or warnings.
     *
     * @param columnsConfig the statistics columns configuration string
     * @param rowType the table schema
     * @return validation result with errors and warnings
     */
    private ValidationResult validateStatisticsConfigSafe(String columnsConfig, RowType rowType) {
        List<String> errors = new ArrayList<>();
        List<String> warnings = new ArrayList<>();

        try {
            // Handle wildcard configuration
            if ("*".equals(columnsConfig.trim())) {
                List<String> nonBinaryColumns = getNonBinaryColumns(rowType);
                if (nonBinaryColumns.isEmpty()) {
                    warnings.add("No non-binary columns available for statistics collection");
                } else {
                    warnings.add(
                            String.format(
                                    "Wildcard configuration will collect statistics for %d non-binary columns",
                                    nonBinaryColumns.size()));
                }
                return ValidationResult.success(warnings);
            }

            // Parse and validate specific column names
            List<String> columnNames = parseColumnNames(columnsConfig);
            if (columnNames.isEmpty()) {
                errors.add(
                        "Statistics columns configuration cannot be empty. Use '*' to collect statistics for all non-binary columns.");
                return ValidationResult.failure(errors);
            }

            // Validate columns
            try {
                validateColumns(rowType, columnNames);
                warnings.add(
                        String.format(
                                "Statistics will be collected for %d columns: %s",
                                columnNames.size(), String.join(", ", columnNames)));
            } catch (InvalidConfigException e) {
                errors.add(e.getMessage());
            }

        } catch (Exception e) {
            errors.add("Failed to validate statistics configuration: " + e.getMessage());
        }

        if (!errors.isEmpty()) {
            return ValidationResult.failure(errors, warnings);
        } else {
            return ValidationResult.success(warnings);
        }
    }

    /**
     * Determines the effective statistics columns based on configuration and schema.
     *
     * @param columnsConfig the statistics columns configuration string
     * @param rowType the table schema
     * @return list of column names that should have statistics collected
     */
    private List<String> determineStatisticsColumns(String columnsConfig, RowType rowType) {
        // Validate configuration first
        ValidationResult validationResult = validateStatisticsConfigSafe(columnsConfig, rowType);
        if (!validationResult.isValid()) {
            throw new InvalidConfigException(
                    "Invalid statistics configuration: "
                            + String.join("; ", validationResult.getErrors()));
        }

        if ("*".equals(columnsConfig.trim())) {
            // Return all non-binary columns
            return getNonBinaryColumns(rowType);
        } else {
            // Return the parsed column names (already validated)
            return parseColumnNames(columnsConfig);
        }
    }

    /**
     * Parses comma-separated column names from the configuration string.
     *
     * @param columnsConfig the configuration string
     * @return list of parsed column names
     */
    private List<String> parseColumnNames(String columnsConfig) {
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
    private void validateColumns(RowType rowType, List<String> statisticsColumns) {
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
            if (DataTypeChecks.isBinaryType(dataType)) {
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
    private Map<String, DataType> buildColumnTypeMap(RowType rowType) {
        return rowType.getFields().stream()
                .collect(Collectors.toMap(DataField::getName, DataField::getType));
    }

    /**
     * Gets the list of non-binary column names from the given row type.
     *
     * @param rowType the table schema
     * @return list of non-binary column names
     */
    private List<String> getNonBinaryColumns(RowType rowType) {
        return rowType.getFields().stream()
                .filter(field -> !DataTypeChecks.isBinaryType(field.getType()))
                .map(DataField::getName)
                .collect(Collectors.toList());
    }
}
