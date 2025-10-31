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
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidConfigException;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

/**
 * Main entry point for table configuration validation. This class coordinates all registered
 * validators and provides unified validation methods for DDL operations.
 */
@Internal
public class TableConfigValidator {

    private static final Logger LOG = LoggerFactory.getLogger(TableConfigValidator.class);

    private final ValidatorRegistry validatorRegistry;

    /**
     * Creates a new table configuration validator with the given validator registry.
     *
     * @param validatorRegistry the validator registry to use
     */
    public TableConfigValidator(ValidatorRegistry validatorRegistry) {
        this.validatorRegistry = validatorRegistry;
    }

    /** Creates a new table configuration validator with default validators. */
    public TableConfigValidator() {
        this.validatorRegistry = new ValidatorRegistry();
        registerDefaultValidators();
    }

    /**
     * Validates table configuration during CREATE TABLE operations.
     *
     * @param tableDescriptor the table descriptor to validate
     * @return validation result containing all errors and warnings from all validators
     */
    public ValidationResult validateCreateTable(TableDescriptor tableDescriptor) {
        LOG.debug("Validating CREATE TABLE configuration");

        ValidationResult combinedResult = ValidationResult.success();
        List<ConfigValidator> validators = validatorRegistry.getAllValidators();

        for (ConfigValidator validator : validators) {
            try {
                ValidationResult result = validator.validateCreateTable(tableDescriptor);
                combinedResult = combinedResult.merge(result);

                if (result.hasErrors()) {
                    LOG.debug(
                            "Validator {} found errors: {}",
                            validator.getName(),
                            result.getErrors());
                } else if (result.hasWarnings()) {
                    LOG.debug(
                            "Validator {} found warnings: {}",
                            validator.getName(),
                            result.getWarnings());
                }
            } catch (Exception e) {
                LOG.error(
                        "Validator {} threw exception during CREATE TABLE validation",
                        validator.getName(),
                        e);
                ValidationResult errorResult =
                        ValidationResult.failure(
                                String.format(
                                        "Validation failed in %s: %s",
                                        validator.getName(), e.getMessage()));
                combinedResult = combinedResult.merge(errorResult);
            }
        }

        return combinedResult;
    }

    /**
     * Validates table configuration during ALTER TABLE operations.
     *
     * @param currentTableDescriptor the current table descriptor
     * @param alterProperties the properties being altered
     * @param newSchema the new schema after ALTER (if schema changes)
     * @return validation result containing all errors and warnings from all validators
     */
    public ValidationResult validateAlterTable(
            TableDescriptor currentTableDescriptor,
            Configuration alterProperties,
            @Nullable Schema newSchema) {

        LOG.debug("Validating ALTER TABLE configuration");

        ValidationResult combinedResult = ValidationResult.success();
        List<ConfigValidator> validators = validatorRegistry.getAllValidators();

        for (ConfigValidator validator : validators) {
            try {
                ValidationResult result =
                        validator.validateAlterTable(
                                currentTableDescriptor, alterProperties, newSchema);
                combinedResult = combinedResult.merge(result);

                if (result.hasErrors()) {
                    LOG.debug(
                            "Validator {} found errors: {}",
                            validator.getName(),
                            result.getErrors());
                } else if (result.hasWarnings()) {
                    LOG.debug(
                            "Validator {} found warnings: {}",
                            validator.getName(),
                            result.getWarnings());
                }
            } catch (Exception e) {
                LOG.error(
                        "Validator {} threw exception during ALTER TABLE validation",
                        validator.getName(),
                        e);
                ValidationResult errorResult =
                        ValidationResult.failure(
                                String.format(
                                        "Validation failed in %s: %s",
                                        validator.getName(), e.getMessage()));
                combinedResult = combinedResult.merge(errorResult);
            }
        }

        return combinedResult;
    }

    /**
     * Validates a specific configuration property against all relevant validators.
     *
     * @param key the configuration key
     * @param value the configuration value
     * @param schema the table schema
     * @return validation result containing all errors and warnings from relevant validators
     */
    public ValidationResult validateProperty(String key, String value, Schema schema) {
        LOG.debug("Validating property: {} = {}", key, value);

        ValidationResult combinedResult = ValidationResult.success();
        List<ConfigValidator> validators = validatorRegistry.getValidatorsForKey(key);

        for (ConfigValidator validator : validators) {
            try {
                ValidationResult result = validator.validateProperty(key, value, schema);
                combinedResult = combinedResult.merge(result);

                if (result.hasErrors()) {
                    LOG.debug(
                            "Validator {} found errors for property {}: {}",
                            validator.getName(),
                            key,
                            result.getErrors());
                } else if (result.hasWarnings()) {
                    LOG.debug(
                            "Validator {} found warnings for property {}: {}",
                            validator.getName(),
                            key,
                            result.getWarnings());
                }
            } catch (Exception e) {
                LOG.error(
                        "Validator {} threw exception during property validation",
                        validator.getName(),
                        e);
                ValidationResult errorResult =
                        ValidationResult.failure(
                                String.format(
                                        "Validation failed in %s: %s",
                                        validator.getName(), e.getMessage()));
                combinedResult = combinedResult.merge(errorResult);
            }
        }

        return combinedResult;
    }

    /**
     * Validates configuration and throws exception if invalid. This is a convenience method for
     * cases where you want to fail fast.
     *
     * @param tableDescriptor the table descriptor to validate
     * @throws InvalidConfigException if validation fails
     */
    public void validateCreateTableOrThrow(TableDescriptor tableDescriptor)
            throws InvalidConfigException {
        ValidationResult result = validateCreateTable(tableDescriptor);

        if (!result.isValid()) {
            String errorMessage = String.join("; ", result.getErrors());
            throw new InvalidConfigException("Invalid table configuration: " + errorMessage);
        }
    }

    /**
     * Validates ALTER TABLE configuration and throws exception if invalid.
     *
     * @param currentTableDescriptor the current table descriptor
     * @param alterProperties the properties being altered
     * @param newSchema the new schema after ALTER (if schema changes)
     * @throws InvalidConfigException if validation fails
     */
    public void validateAlterTableOrThrow(
            TableDescriptor currentTableDescriptor,
            Configuration alterProperties,
            @Nullable Schema newSchema)
            throws InvalidConfigException {

        ValidationResult result =
                validateAlterTable(currentTableDescriptor, alterProperties, newSchema);

        if (!result.isValid()) {
            String errorMessage = String.join("; ", result.getErrors());
            throw new InvalidConfigException("Invalid table configuration: " + errorMessage);
        }
    }

    /**
     * Gets the validator registry for accessing and managing validators.
     *
     * @return the validator registry
     */
    public ValidatorRegistry getValidatorRegistry() {
        return validatorRegistry;
    }

    /**
     * Gets default configuration for a table schema by consulting all validators.
     *
     * @param schema the table schema
     * @return default configuration from all validators
     */
    public Map<String, String> getDefaultConfig(Schema schema) {
        // This could be implemented later if needed
        // For now, we don't have a general way to get default configs from validators
        throw new UnsupportedOperationException("Default config generation not yet implemented");
    }

    /** Registers default validators that should always be available. */
    private void registerDefaultValidators() {
        // Register the statistics configuration validator
        validatorRegistry.register(new TableStatisticsConfigValidator());

        // Future validators can be added here:
        // validatorRegistry.register(new PartitionConfigValidator());
        // validatorRegistry.register(new StorageConfigValidator());
        // etc.
    }
}
