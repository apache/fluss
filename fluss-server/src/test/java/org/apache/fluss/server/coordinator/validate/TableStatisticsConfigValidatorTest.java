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

/** Tests for table statistics configuration validation. */
class TableStatisticsConfigValidatorTest {

    private static final Schema TEST_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("name", DataTypes.STRING())
                    .column("age", DataTypes.BIGINT())
                    .column("data", DataTypes.BYTES()) // Binary column
                    .column("score", DataTypes.DOUBLE())
                    .build();

    private final TableConfigValidator validator = new TableConfigValidator();

    @Test
    void testValidateCreateTableWithDefaults() {
        // Test with default statistics configuration
        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(TEST_SCHEMA).distributedBy(3).build();

        ValidationResult result = validator.validateCreateTable(tableDescriptor);

        assertThat(result.isValid()).isTrue();
        assertThat(result.getWarnings()).hasSize(1);
        assertThat(result.getWarnings().get(0))
                .contains(
                        "Wildcard configuration will collect statistics for 4 non-binary columns");
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

        ValidationResult result = validator.validateCreateTable(tableDescriptor);

        assertThat(result.isValid()).isTrue();
        assertThat(result.getWarnings()).hasSize(1);
        assertThat(result.getWarnings().get(0))
                .contains("Statistics will be collected for 3 columns: id, name, score");
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

        ValidationResult result = validator.validateCreateTable(tableDescriptor);

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

        ValidationResult result = validator.validateCreateTable(tableDescriptor);

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

        ValidationResult result = validator.validateCreateTable(tableDescriptor);

        assertThat(result.isValid()).isTrue();
        // Should exclude the 'data' binary column, so 4 columns instead of 5
        assertThat(result.getWarnings().get(0))
                .contains(
                        "Wildcard configuration will collect statistics for 4 non-binary columns");
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

        ValidationResult result =
                validator.validateAlterTable(currentTableDescriptor, alterProperties, null);

        assertThat(result.isValid()).isTrue();
        assertThat(result.getWarnings().get(0))
                .contains("Statistics will be collected for 3 columns: id, name, score");
    }

    @Test
    void testValidateStatisticsProperty() {
        // Test individual property validation
        ValidationResult result3 =
                validator.validateProperty(
                        ConfigOptions.TABLE_STATISTICS_COLUMNS.key(), "id,name", TEST_SCHEMA);
        assertThat(result3.isValid()).isTrue();

        ValidationResult result4 =
                validator.validateProperty(
                        ConfigOptions.TABLE_STATISTICS_COLUMNS.key(),
                        "invalid_column",
                        TEST_SCHEMA);
        assertThat(result4.isValid()).isFalse();
        assertThat(result4.getErrors().get(0)).contains("does not exist in table schema");
    }

    @Test
    void testIsStatisticsConfigKey() {
        // Test config key detection through validator registry.
        boolean handlesStatisticsKey =
                validator
                        .getValidatorRegistry()
                        .getValidatorsForKey(ConfigOptions.TABLE_STATISTICS_COLUMNS.key())
                        .stream()
                        .anyMatch(v -> v.handles(ConfigOptions.TABLE_STATISTICS_COLUMNS.key()));
        assertThat(handlesStatisticsKey).isTrue();

        boolean handlesOtherKey =
                validator.getValidatorRegistry().getValidatorsForKey("other.config").stream()
                        .anyMatch(v -> v.handles("other.config"));
        assertThat(handlesOtherKey).isFalse();
    }

    @Test
    void testGetDefaultStatisticsConfig() {
        // Test default configuration generation - this is no longer supported in the new
        // architecture
        // We could potentially implement this in the future if needed
        // For now, we test that the default behavior works with "*"
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(TEST_SCHEMA)
                        .distributedBy(3)
                        .property(ConfigOptions.TABLE_STATISTICS_COLUMNS.key(), "*")
                        .build();

        ValidationResult result = validator.validateCreateTable(tableDescriptor);
        assertThat(result.isValid()).isTrue();
    }

    @Test
    void testSanitizeStatisticsConfig() {
        // Test configuration sanitization - this is no longer a specific method
        // We test that the validator handles missing statistics configuration properly
        Map<String, String> properties = new HashMap<>();
        properties.put("other.config", "value");
        // Don't set statistics columns - should use default behavior

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(TEST_SCHEMA)
                        .distributedBy(3)
                        .properties(properties)
                        .build();

        ValidationResult result = validator.validateCreateTable(tableDescriptor);
        assertThat(result.isValid()).isTrue();
        // Should use default "*" behavior
        assertThat(result.getWarnings().get(0))
                .contains(
                        "Wildcard configuration will collect statistics for 4 non-binary columns");
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
        validator.validateCreateTableOrThrow(validTableDescriptor);

        // Test invalid validation
        TableDescriptor invalidTableDescriptor =
                TableDescriptor.builder()
                        .schema(TEST_SCHEMA)
                        .distributedBy(3)
                        .property(ConfigOptions.TABLE_STATISTICS_COLUMNS.key(), "invalid_column")
                        .build();

        // Should throw InvalidConfigException
        assertThatThrownBy(() -> validator.validateCreateTableOrThrow(invalidTableDescriptor))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("Invalid table configuration");
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

        ValidationResult result = validator.validateCreateTable(tableDescriptor);

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

        ValidationResult result = validator.validateCreateTable(tableDescriptor);

        assertThat(result.isValid()).isTrue();
        assertThat(result.getWarnings())
                .anyMatch(
                        warning ->
                                warning.contains(
                                        "Statistics collection is disabled for this table"));
    }
}
