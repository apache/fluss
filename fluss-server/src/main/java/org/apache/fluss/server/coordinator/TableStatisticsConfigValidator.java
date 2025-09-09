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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.StatisticsColumnValidator;
import org.apache.fluss.exception.InvalidConfigException;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Validator for table statistics configuration during DDL operations. This validator ensures that
 * statistics configuration in table properties is valid and compatible with the table schema.
 */
@Internal
public class TableStatisticsConfigValidator {

    /** Validation result for table statistics configuration. */
    public static class ValidationResult {
        private final boolean valid;
        private final List<String> errors;
        private final List<String> warnings;

        public ValidationResult(boolean valid, List<String> errors, List<String> warnings) {
            this.valid = valid;
            this.errors = errors != null ? errors : new ArrayList<>();
            this.warnings = warnings != null ? warnings : new ArrayList<>();
        }

        public boolean isValid() {
            return valid;
        }

        public List<String> getErrors() {
            return errors;
        }

        public List<String> getWarnings() {
            return warnings;
        }

        public boolean hasErrors() {
            return !errors.isEmpty();
        }

        public boolean hasWarnings() {
            return !warnings.isEmpty();
        }

        public static ValidationResult success() {
            return new ValidationResult(true, null, null);
        }

        public static ValidationResult success(List<String> warnings) {
            return new ValidationResult(true, null, warnings);
        }

        public static ValidationResult failure(List<String> errors) {
            return new ValidationResult(false, errors, null);
        }

        public static ValidationResult failure(List<String> errors, List<String> warnings) {
            return new ValidationResult(false, errors, warnings);
        }

        public static ValidationResult failure(String error) {
            List<String> errors = new ArrayList<>();
            errors.add(error);
            return new ValidationResult(false, errors, null);
        }
    }

    /**
     * Validates the statistics configuration in a table descriptor during CREATE TABLE.
     *
     * @param tableDescriptor the table descriptor to validate
     * @return validation result
     */
    public static ValidationResult validateCreateTable(TableDescriptor tableDescriptor) {
        Map<String, String> tableProperties = tableDescriptor.getProperties();
        Schema schema = tableDescriptor.getSchema();

        return validateStatisticsConfig(tableProperties, schema, "CREATE TABLE");
    }

    /**
     * Validates the statistics configuration during ALTER TABLE operations.
     *
     * @param currentTableDescriptor the current table descriptor
     * @param alterProperties the properties to be altered (can be partial)
     * @param newSchema the new schema after ALTER (if schema change)
     * @return validation result
     */
    public static ValidationResult validateAlterTable(
            TableDescriptor currentTableDescriptor,
            Configuration alterProperties,
            @Nullable Schema newSchema) {

        // Merge current properties with alter properties to get the final configuration
        Map<String, String> mergedProperties =
                new HashMap<>(currentTableDescriptor.getProperties());

        // Add alter properties (Configuration to Map conversion)
        // Convert Configuration to Map using toMap() method
        Map<String, String> alterPropertiesMap = alterProperties.toMap();
        mergedProperties.putAll(alterPropertiesMap);

        // Use new schema if provided, otherwise use current schema
        Schema targetSchema = newSchema != null ? newSchema : currentTableDescriptor.getSchema();

        return validateStatisticsConfig(mergedProperties, targetSchema, "ALTER TABLE");
    }

    /**
     * Validates statistics configuration against a schema.
     *
     * @param tableProperties the table properties containing statistics configuration
     * @param schema the table schema
     * @param operation the DDL operation name (for error messages)
     * @return validation result
     */
    private static ValidationResult validateStatisticsConfig(
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

        try {
            RowType rowType = schema.getRowType();
            List<String> validatedColumns =
                    StatisticsColumnValidator.determineStatisticsColumns(
                            statisticsColumns, rowType);

            // Check if any columns are selected for statistics
            if (validatedColumns.isEmpty()) {
                warnings.add("No columns selected for statistics collection");
            } else {
                // Provide info about selected columns
                warnings.add(
                        String.format(
                                "Statistics will be collected for %d columns: %s",
                                validatedColumns.size(), String.join(", ", validatedColumns)));
            }

            // Validate that all specified columns exist and are compatible
            List<String> availableColumns = schema.getColumnNames();
            for (String columnName : validatedColumns) {
                if (!availableColumns.contains(columnName)) {
                    errors.add(
                            String.format(
                                    "Statistics column '%s' does not exist in table schema",
                                    columnName));
                }
            }

        } catch (IllegalArgumentException e) {
            errors.add(
                    String.format("Invalid statistics columns configuration: %s", e.getMessage()));
        } catch (Exception e) {
            errors.add(
                    String.format(
                            "Failed to validate statistics columns configuration: %s",
                            e.getMessage()));
        }

        // Additional validations can be added here
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
    private static void validateStatisticsCompatibilityWithTableType(
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
     * Validates a specific statistics configuration property.
     *
     * @param key the configuration key
     * @param value the configuration value
     * @param schema the table schema
     * @return validation result for this specific property
     */
    public static ValidationResult validateStatisticsProperty(
            String key, String value, Schema schema) {
        List<String> errors = new ArrayList<>();
        List<String> warnings = new ArrayList<>();

        if (ConfigOptions.TABLE_STATISTICS_COLUMNS.key().equals(key)) {
            // Validate columns configuration
            try {
                RowType rowType = schema.getRowType();
                List<String> validatedColumns =
                        StatisticsColumnValidator.determineStatisticsColumns(value, rowType);

                if (validatedColumns.isEmpty()) {
                    warnings.add("No columns will be selected for statistics collection");
                }
            } catch (Exception e) {
                errors.add(
                        String.format(
                                "Invalid statistics columns configuration: %s", e.getMessage()));
            }
        }

        if (!errors.isEmpty()) {
            return ValidationResult.failure(errors, warnings);
        } else {
            return ValidationResult.success(warnings);
        }
    }

    /**
     * Checks if a configuration key is related to statistics.
     *
     * @param key the configuration key
     * @return true if the key is statistics-related
     */
    public static boolean isStatisticsConfigKey(String key) {
        return ConfigOptions.TABLE_STATISTICS_COLUMNS.key().equals(key);
    }

    /**
     * Gets default statistics configuration for a table schema.
     *
     * @param schema the table schema
     * @return default statistics configuration
     */
    public static Map<String, String> getDefaultStatisticsConfig(Schema schema) {
        Map<String, String> config = new HashMap<>();

        // Collect statistics for all non-binary columns by default
        config.put(ConfigOptions.TABLE_STATISTICS_COLUMNS.key(), "*");

        return config;
    }

    /**
     * Sanitizes and normalizes statistics configuration. This method can be used to clean up user
     * input and apply defaults.
     *
     * @param tableProperties the table properties to sanitize
     * @param schema the table schema
     * @return sanitized configuration
     */
    public static Map<String, String> sanitizeStatisticsConfig(
            Map<String, String> tableProperties, Schema schema) {
        Map<String, String> sanitized = new HashMap<>(tableProperties);

        // Ensure statistics columns configuration is present
        if (!sanitized.containsKey(ConfigOptions.TABLE_STATISTICS_COLUMNS.key())) {
            sanitized.put(ConfigOptions.TABLE_STATISTICS_COLUMNS.key(), "*");
        }

        return sanitized;
    }

    /**
     * Validates and throws exception if configuration is invalid. This is a convenience method for
     * cases where you want to fail fast.
     *
     * @param tableDescriptor the table descriptor to validate
     * @throws InvalidConfigException if validation fails
     */
    public static void validateOrThrow(TableDescriptor tableDescriptor)
            throws InvalidConfigException {
        ValidationResult result = validateCreateTable(tableDescriptor);

        if (!result.isValid()) {
            String errorMessage = String.join("; ", result.getErrors());
            throw new InvalidConfigException("Invalid statistics configuration: " + errorMessage);
        }
    }

    /**
     * Validates ALTER TABLE operation and throws exception if invalid.
     *
     * @param currentTableDescriptor the current table descriptor
     * @param alterProperties the properties to be altered
     * @param newSchema the new schema (if schema change)
     * @throws InvalidConfigException if validation fails
     */
    public static void validateAlterOrThrow(
            TableDescriptor currentTableDescriptor,
            Map<String, String> alterProperties,
            @Nullable Schema newSchema)
            throws InvalidConfigException {

        // Convert Configuration to Map if needed
        Configuration configAlterProperties = new Configuration();
        for (Map.Entry<String, String> entry : alterProperties.entrySet()) {
            configAlterProperties.setString(entry.getKey(), entry.getValue());
        }

        ValidationResult result =
                validateAlterTable(currentTableDescriptor, configAlterProperties, newSchema);

        if (!result.isValid()) {
            String errorMessage = String.join("; ", result.getErrors());
            throw new InvalidConfigException(
                    "Invalid statistics configuration for ALTER TABLE: " + errorMessage);
        }
    }
}
