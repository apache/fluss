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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidConfigException;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TableStatisticsConfigValidator}. */
class TableStatisticsConfigValidatorTest {

    private static final Schema TEST_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("name", DataTypes.STRING())
                    .column("age", DataTypes.BIGINT())
                    .column("data", DataTypes.BYTES()) // Binary column
                    .column("score", DataTypes.DOUBLE())
                    .build();

    @Test
    void testValidateCreateTableWithDefaults() {
        // Test with default statistics configuration
        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(TEST_SCHEMA).distributedBy(3).build();

        TableStatisticsConfigValidator.ValidationResult result =
                TableStatisticsConfigValidator.validateCreateTable(tableDescriptor);

        assertThat(result.isValid()).isTrue();
        assertThat(result.getWarnings()).hasSize(1);
        assertThat(result.getWarnings().get(0))
                .contains("Statistics will be collected for 4 columns");
    }

    @Test
    void testValidateCreateTableWithStatisticsEnabled() {
        // Test with statistics enabled and specific columns
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(TEST_SCHEMA)
                        .distributedBy(3)
                        .property(ConfigOptions.TABLE_STATISTICS_COLUMNS.key(), "id,name,score")
                        .build();

        TableStatisticsConfigValidator.ValidationResult result =
                TableStatisticsConfigValidator.validateCreateTable(tableDescriptor);

        assertThat(result.isValid()).isTrue();
        assertThat(result.getWarnings()).hasSize(1);
        assertThat(result.getWarnings().get(0))
                .contains("Statistics will be collected for 3 columns");
    }

    @Test
    void testValidateCreateTableWithStatisticsDisabled() {
        // Test with statistics disabled
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(TEST_SCHEMA)
                        .distributedBy(3)
                        .property(ConfigOptions.TABLE_STATISTICS_COLUMNS.key(), "")
                        .build();

        TableStatisticsConfigValidator.ValidationResult result =
                TableStatisticsConfigValidator.validateCreateTable(tableDescriptor);

        assertThat(result.isValid()).isTrue();
        assertThat(result.getWarnings()).hasSize(1);
        assertThat(result.getWarnings().get(0)).contains("Statistics collection is disabled");
    }

    @Test
    void testValidateCreateTableWithInvalidColumn() {
        // Test with invalid column name
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(TEST_SCHEMA)
                        .distributedBy(3)
                        .property(ConfigOptions.TABLE_STATISTICS_COLUMNS.key(), "id,invalid_column")
                        .build();

        TableStatisticsConfigValidator.ValidationResult result =
                TableStatisticsConfigValidator.validateCreateTable(tableDescriptor);

        assertThat(result.isValid()).isFalse();
        assertThat(result.getErrors()).hasSize(1);
        assertThat(result.getErrors().get(0))
                .contains(
                        "Column 'invalid_column' specified in statistics collection does not exist in table schema");
    }

    @Test
    void testValidateCreateTableWithBinaryColumn() {
        // Test that binary columns are automatically excluded
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(TEST_SCHEMA)
                        .distributedBy(3)
                        .property(ConfigOptions.TABLE_STATISTICS_COLUMNS.key(), "*")
                        .build();

        TableStatisticsConfigValidator.ValidationResult result =
                TableStatisticsConfigValidator.validateCreateTable(tableDescriptor);

        assertThat(result.isValid()).isTrue();
        // Should exclude the 'data' binary column, so 4 columns instead of 5
        assertThat(result.getWarnings().get(0))
                .contains("Statistics will be collected for 4 columns");
    }

    @Test
    void testValidateAlterTable() {
        // Test ALTER TABLE validation
        TableDescriptor currentTableDescriptor =
                TableDescriptor.builder()
                        .schema(TEST_SCHEMA)
                        .distributedBy(3)
                        .property(ConfigOptions.TABLE_STATISTICS_COLUMNS.key(), "id,name")
                        .build();

        Configuration alterProperties = new Configuration();
        alterProperties.setString(ConfigOptions.TABLE_STATISTICS_COLUMNS.key(), "id,name,score");

        TableStatisticsConfigValidator.ValidationResult result =
                TableStatisticsConfigValidator.validateAlterTable(
                        currentTableDescriptor, alterProperties, null);

        assertThat(result.isValid()).isTrue();
        assertThat(result.getWarnings().get(0))
                .contains("Statistics will be collected for 3 columns");
    }

    @Test
    void testValidateStatisticsProperty() {
        // Test individual property validation
        TableStatisticsConfigValidator.ValidationResult result3 =
                TableStatisticsConfigValidator.validateStatisticsProperty(
                        ConfigOptions.TABLE_STATISTICS_COLUMNS.key(), "id,name", TEST_SCHEMA);
        assertThat(result3.isValid()).isTrue();

        TableStatisticsConfigValidator.ValidationResult result4 =
                TableStatisticsConfigValidator.validateStatisticsProperty(
                        ConfigOptions.TABLE_STATISTICS_COLUMNS.key(),
                        "invalid_column",
                        TEST_SCHEMA);
        assertThat(result4.isValid()).isFalse();
        assertThat(result4.getErrors().get(0)).contains("Invalid statistics columns configuration");
    }

    @Test
    void testIsStatisticsConfigKey() {
        // Test config key detection
        assertThat(
                        TableStatisticsConfigValidator.isStatisticsConfigKey(
                                ConfigOptions.TABLE_STATISTICS_COLUMNS.key()))
                .isTrue();
        assertThat(TableStatisticsConfigValidator.isStatisticsConfigKey("other.config")).isFalse();
    }

    @Test
    void testGetDefaultStatisticsConfig() {
        // Test default configuration generation
        Map<String, String> defaultConfig =
                TableStatisticsConfigValidator.getDefaultStatisticsConfig(TEST_SCHEMA);

        assertThat(defaultConfig).containsEntry(ConfigOptions.TABLE_STATISTICS_COLUMNS.key(), "*");
    }

    @Test
    void testSanitizeStatisticsConfig() {
        // Test configuration sanitization
        Map<String, String> inputConfig = new HashMap<>();
        inputConfig.put("other.config", "value");

        Map<String, String> sanitizedConfig =
                TableStatisticsConfigValidator.sanitizeStatisticsConfig(inputConfig, TEST_SCHEMA);

        assertThat(sanitizedConfig).containsEntry("other.config", "value");
        assertThat(sanitizedConfig)
                .containsEntry(ConfigOptions.TABLE_STATISTICS_COLUMNS.key(), "*");
    }

    @Test
    void testValidateOrThrow() {
        // Test successful validation
        TableDescriptor validTableDescriptor =
                TableDescriptor.builder()
                        .schema(TEST_SCHEMA)
                        .distributedBy(3)
                        .property(ConfigOptions.TABLE_STATISTICS_COLUMNS.key(), "id,name")
                        .build();

        // Should not throw
        TableStatisticsConfigValidator.validateOrThrow(validTableDescriptor);

        // Test invalid validation
        TableDescriptor invalidTableDescriptor =
                TableDescriptor.builder()
                        .schema(TEST_SCHEMA)
                        .distributedBy(3)
                        .property(ConfigOptions.TABLE_STATISTICS_COLUMNS.key(), "invalid_column")
                        .build();

        // Should throw InvalidConfigException
        assertThatThrownBy(
                        () ->
                                TableStatisticsConfigValidator.validateOrThrow(
                                        invalidTableDescriptor))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("Invalid statistics configuration");
    }

    @Test
    void testValidateAlterOrThrow() {
        // Test ALTER TABLE validation with exception
        TableDescriptor currentTableDescriptor =
                TableDescriptor.builder().schema(TEST_SCHEMA).distributedBy(3).build();

        Map<String, String> invalidAlterProperties = new HashMap<>();
        invalidAlterProperties.put(ConfigOptions.TABLE_STATISTICS_COLUMNS.key(), "invalid_column");

        // Should throw InvalidConfigException
        assertThatThrownBy(
                        () ->
                                TableStatisticsConfigValidator.validateAlterOrThrow(
                                        currentTableDescriptor, invalidAlterProperties, null))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("Invalid statistics configuration for ALTER TABLE");
    }

    @Test
    void testValidateWithPrimaryKeyTable() {
        // Test validation with primary key table
        Schema pkSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("value", DataTypes.BIGINT())
                        .primaryKey("id")
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(pkSchema)
                        .distributedBy(3)
                        .property(ConfigOptions.TABLE_STATISTICS_COLUMNS.key(), "*")
                        .build();

        TableStatisticsConfigValidator.ValidationResult result =
                TableStatisticsConfigValidator.validateCreateTable(tableDescriptor);

        assertThat(result.isValid()).isTrue();
        assertThat(result.getWarnings())
                .anyMatch(warning -> warning.contains("Table has primary key"));
    }

    @Test
    void testValidateWithEmptyColumns() {
        // Test with empty column selection
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(TEST_SCHEMA)
                        .distributedBy(3)
                        .property(ConfigOptions.TABLE_STATISTICS_COLUMNS.key(), "")
                        .build();

        TableStatisticsConfigValidator.ValidationResult result =
                TableStatisticsConfigValidator.validateCreateTable(tableDescriptor);

        assertThat(result.isValid()).isTrue();
        assertThat(result.getWarnings())
                .anyMatch(
                        warning ->
                                warning.contains(
                                        "Statistics collection is disabled for this table"));
    }
}
