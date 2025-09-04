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
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;

import javax.annotation.Nullable;

/**
 * Interface for configuration validators. Validators are responsible for checking table
 * configuration properties during DDL operations and ensuring they are valid and compatible with
 * the table schema.
 */
@Internal
public interface ConfigValidator {

    /**
     * Gets the name of this validator for identification and debugging purposes.
     *
     * @return the validator name
     */
    String getName();

    /**
     * Validates table configuration during CREATE TABLE operations.
     *
     * @param tableDescriptor the table descriptor containing schema and properties
     * @return validation result containing errors and warnings
     */
    ValidationResult validateCreateTable(TableDescriptor tableDescriptor);

    /**
     * Validates table configuration during ALTER TABLE operations.
     *
     * @param currentTableDescriptor the current table descriptor
     * @param alterProperties the properties being altered (can be partial)
     * @param newSchema the new schema after ALTER (if schema changes)
     * @return validation result containing errors and warnings
     */
    ValidationResult validateAlterTable(
            TableDescriptor currentTableDescriptor,
            Configuration alterProperties,
            @Nullable Schema newSchema);

    /**
     * Validates a specific configuration property.
     *
     * @param key the configuration key
     * @param value the configuration value
     * @param schema the table schema
     * @return validation result for this specific property
     */
    ValidationResult validateProperty(String key, String value, Schema schema);

    /**
     * Checks if this validator is responsible for validating the given configuration key.
     *
     * @param key the configuration key
     * @return true if this validator should handle this key
     */
    boolean handles(String key);

    /**
     * Gets the priority of this validator. Validators with lower priority values are executed
     * first. This allows controlling the order of validation when multiple validators may affect
     * each other.
     *
     * @return the priority value (lower means higher priority)
     */
    default int getPriority() {
        return Integer.MAX_VALUE;
    }
}
