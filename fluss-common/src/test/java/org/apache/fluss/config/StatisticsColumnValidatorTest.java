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

import org.apache.fluss.exception.InvalidConfigException;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link StatisticsColumnValidator}. */
class StatisticsColumnValidatorTest {

    private static final RowType TEST_ROW_TYPE =
            new RowType(
                    Arrays.asList(
                            new DataField("id", DataTypes.INT()),
                            new DataField("name", DataTypes.STRING()),
                            new DataField("age", DataTypes.BIGINT()),
                            new DataField("data", DataTypes.BYTES()),
                            new DataField("score", DataTypes.DOUBLE()),
                            new DataField("binary_data", DataTypes.BINARY(20))));

    @Test
    void testValidateStatisticsConfigWithWildcard() {
        // Wildcard configuration should always be valid
        StatisticsColumnValidator.validateStatisticsConfig("*", TEST_ROW_TYPE);
        StatisticsColumnValidator.validateStatisticsConfig(" * ", TEST_ROW_TYPE);
    }

    @Test
    void testValidateStatisticsConfigWithValidColumns() {
        // Valid non-binary columns should pass validation
        StatisticsColumnValidator.validateStatisticsConfig("id,name,age", TEST_ROW_TYPE);
        StatisticsColumnValidator.validateStatisticsConfig("id, name, score", TEST_ROW_TYPE);
        StatisticsColumnValidator.validateStatisticsConfig("name", TEST_ROW_TYPE);
    }

    @Test
    void testValidateStatisticsConfigWithInvalidColumn() {
        // Non-existent column should throw exception
        assertThatThrownBy(
                        () ->
                                StatisticsColumnValidator.validateStatisticsConfig(
                                        "invalid_column", TEST_ROW_TYPE))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining(
                        "Column 'invalid_column' specified in statistics collection does not exist");

        assertThatThrownBy(
                        () ->
                                StatisticsColumnValidator.validateStatisticsConfig(
                                        "id,invalid,name", TEST_ROW_TYPE))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining(
                        "Column 'invalid' specified in statistics collection does not exist");
    }

    @Test
    void testValidateStatisticsConfigWithBinaryColumn() {
        // Binary columns should throw exception
        assertThatThrownBy(
                        () ->
                                StatisticsColumnValidator.validateStatisticsConfig(
                                        "data", TEST_ROW_TYPE))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining(
                        "Binary column 'data' cannot be included in statistics collection");

        assertThatThrownBy(
                        () ->
                                StatisticsColumnValidator.validateStatisticsConfig(
                                        "binary_data", TEST_ROW_TYPE))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining(
                        "Binary column 'binary_data' cannot be included in statistics collection");

        assertThatThrownBy(
                        () ->
                                StatisticsColumnValidator.validateStatisticsConfig(
                                        "id,data,name", TEST_ROW_TYPE))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining(
                        "Binary column 'data' cannot be included in statistics collection");
    }

    @Test
    void testValidateStatisticsConfigWithEmptyConfig() {
        // Empty configuration should throw exception
        assertThatThrownBy(
                        () -> StatisticsColumnValidator.validateStatisticsConfig("", TEST_ROW_TYPE))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("Statistics columns configuration cannot be empty");

        assertThatThrownBy(
                        () ->
                                StatisticsColumnValidator.validateStatisticsConfig(
                                        "  ", TEST_ROW_TYPE))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("Statistics columns configuration cannot be empty");

        assertThatThrownBy(
                        () ->
                                StatisticsColumnValidator.validateStatisticsConfig(
                                        ",,,", TEST_ROW_TYPE))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("Statistics columns configuration cannot be empty");
    }

    @Test
    void testGetNonBinaryColumns() {
        List<String> nonBinaryColumns =
                StatisticsColumnValidator.getNonBinaryColumns(TEST_ROW_TYPE);

        assertThat(nonBinaryColumns).hasSize(4);
        assertThat(nonBinaryColumns).containsExactlyInAnyOrder("id", "name", "age", "score");
        assertThat(nonBinaryColumns).doesNotContain("data", "binary_data");
    }

    @Test
    void testDetermineStatisticsColumnsWithWildcard() {
        List<String> columns =
                StatisticsColumnValidator.determineStatisticsColumns("*", TEST_ROW_TYPE);

        assertThat(columns).hasSize(4);
        assertThat(columns).containsExactlyInAnyOrder("id", "name", "age", "score");
        assertThat(columns).doesNotContain("data", "binary_data");
    }

    @Test
    void testDetermineStatisticsColumnsWithSpecificColumns() {
        List<String> columns =
                StatisticsColumnValidator.determineStatisticsColumns("id,name", TEST_ROW_TYPE);

        assertThat(columns).hasSize(2);
        assertThat(columns).containsExactlyInAnyOrder("id", "name");
    }

    @Test
    void testDetermineStatisticsColumnsWithWhitespace() {
        List<String> columns =
                StatisticsColumnValidator.determineStatisticsColumns(
                        " id , name , score ", TEST_ROW_TYPE);

        assertThat(columns).hasSize(3);
        assertThat(columns).containsExactlyInAnyOrder("id", "name", "score");
    }

    @Test
    void testValidateColumnsDirectly() {
        // Valid columns should not throw
        StatisticsColumnValidator.validateColumns(
                TEST_ROW_TYPE, Arrays.asList("id", "name", "score"));

        // Invalid column should throw
        assertThatThrownBy(
                        () ->
                                StatisticsColumnValidator.validateColumns(
                                        TEST_ROW_TYPE, Arrays.asList("invalid")))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining(
                        "Column 'invalid' specified in statistics collection does not exist");

        // Binary column should throw
        assertThatThrownBy(
                        () ->
                                StatisticsColumnValidator.validateColumns(
                                        TEST_ROW_TYPE, Arrays.asList("data")))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining(
                        "Binary column 'data' cannot be included in statistics collection");
    }

    @Test
    void testWithOnlyBinaryColumns() {
        // Schema with only binary columns
        RowType binaryOnlyRowType =
                new RowType(
                        Arrays.asList(
                                new DataField("data1", DataTypes.BYTES()),
                                new DataField("data2", DataTypes.BINARY(10))));

        // Wildcard should return empty list for binary-only schema
        List<String> columns = StatisticsColumnValidator.getNonBinaryColumns(binaryOnlyRowType);
        assertThat(columns).isEmpty();

        List<String> determineColumns =
                StatisticsColumnValidator.determineStatisticsColumns("*", binaryOnlyRowType);
        assertThat(determineColumns).isEmpty();
    }

    @Test
    void testWithMixedDataTypes() {
        // Schema with various data types
        RowType mixedRowType =
                new RowType(
                        Arrays.asList(
                                new DataField("tiny_col", DataTypes.TINYINT()),
                                new DataField("small_col", DataTypes.SMALLINT()),
                                new DataField("int_col", DataTypes.INT()),
                                new DataField("bigint_col", DataTypes.BIGINT()),
                                new DataField("float_col", DataTypes.FLOAT()),
                                new DataField("double_col", DataTypes.DOUBLE()),
                                new DataField("bool_col", DataTypes.BOOLEAN()),
                                new DataField("string_col", DataTypes.STRING()),
                                new DataField("bytes_col", DataTypes.BYTES()),
                                new DataField("timestamp_col", DataTypes.TIMESTAMP())));

        List<String> nonBinaryColumns = StatisticsColumnValidator.getNonBinaryColumns(mixedRowType);

        assertThat(nonBinaryColumns).hasSize(9); // All except bytes_col
        assertThat(nonBinaryColumns)
                .containsExactlyInAnyOrder(
                        "tiny_col",
                        "small_col",
                        "int_col",
                        "bigint_col",
                        "float_col",
                        "double_col",
                        "bool_col",
                        "string_col",
                        "timestamp_col");
        assertThat(nonBinaryColumns).doesNotContain("bytes_col");
    }

    @Test
    void testDuplicateColumnNames() {
        // Test with duplicate column names in configuration
        List<String> columns =
                StatisticsColumnValidator.determineStatisticsColumns(
                        "id,name,id,score", TEST_ROW_TYPE);

        // Should contain duplicates as parsed (validation will handle duplicates separately if
        // needed)
        assertThat(columns).hasSize(4);
        assertThat(columns).containsExactly("id", "name", "id", "score");
    }

    @Test
    void testCaseSensitivity() {
        // Column names should be case-sensitive
        assertThatThrownBy(
                        () ->
                                StatisticsColumnValidator.validateStatisticsConfig(
                                        "ID,NAME", TEST_ROW_TYPE))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining(
                        "Column 'ID' specified in statistics collection does not exist");
    }

    @Test
    void testEmptyRowType() {
        RowType emptyRowType = new RowType(Arrays.asList());

        // Empty schema should have no non-binary columns
        List<String> nonBinaryColumns = StatisticsColumnValidator.getNonBinaryColumns(emptyRowType);
        assertThat(nonBinaryColumns).isEmpty();

        // Wildcard should return empty list
        List<String> columns =
                StatisticsColumnValidator.determineStatisticsColumns("*", emptyRowType);
        assertThat(columns).isEmpty();

        // Any specific column should fail
        assertThatThrownBy(
                        () ->
                                StatisticsColumnValidator.validateStatisticsConfig(
                                        "any_column", emptyRowType))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining(
                        "Column 'any_column' specified in statistics collection does not exist");
    }
}
