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
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link StatisticsConfigUtils}. */
class StatisticsConfigUtilsTest {

    private static final Schema TEST_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("name", DataTypes.STRING())
                    .column("data", DataTypes.BYTES())
                    .column("tags", DataTypes.ARRAY(DataTypes.STRING()))
                    .column("metadata", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
                    .column("nested", DataTypes.ROW(DataTypes.FIELD("f1", DataTypes.INT())))
                    .build();

    @Test
    void testValidateWithWildcard() {
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(TEST_SCHEMA)
                        .distributedBy(3)
                        .property(ConfigOptions.TABLE_STATISTICS_COLUMNS.key(), "*")
                        .build();

        assertThatNoException()
                .isThrownBy(() -> StatisticsConfigUtils.validateStatisticsConfig(descriptor));
    }

    @Test
    void testValidateWithNotSet() {
        // When the property is not set, statistics is disabled - no validation needed
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(TEST_SCHEMA).distributedBy(3).build();

        assertThatNoException()
                .isThrownBy(() -> StatisticsConfigUtils.validateStatisticsConfig(descriptor));
    }

    @Test
    void testValidateWithSpecificColumns() {
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(TEST_SCHEMA)
                        .distributedBy(3)
                        .property(ConfigOptions.TABLE_STATISTICS_COLUMNS.key(), "id,name")
                        .build();

        assertThatNoException()
                .isThrownBy(() -> StatisticsConfigUtils.validateStatisticsConfig(descriptor));
    }

    @Test
    void testValidateWithNonExistentColumn() {
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(TEST_SCHEMA)
                        .distributedBy(3)
                        .property(ConfigOptions.TABLE_STATISTICS_COLUMNS.key(), "nonexistent")
                        .build();

        assertThatThrownBy(() -> StatisticsConfigUtils.validateStatisticsConfig(descriptor))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("does not exist in table schema");
    }

    @Test
    void testValidateWithBinaryColumn() {
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(TEST_SCHEMA)
                        .distributedBy(3)
                        .property(ConfigOptions.TABLE_STATISTICS_COLUMNS.key(), "data")
                        .build();

        assertThatThrownBy(() -> StatisticsConfigUtils.validateStatisticsConfig(descriptor))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("is not supported for statistics collection");
    }

    @Test
    void testValidateWithArrayColumn() {
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(TEST_SCHEMA)
                        .distributedBy(3)
                        .property(ConfigOptions.TABLE_STATISTICS_COLUMNS.key(), "tags")
                        .build();

        assertThatThrownBy(() -> StatisticsConfigUtils.validateStatisticsConfig(descriptor))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("is not supported for statistics collection");
    }

    @Test
    void testValidateWithMapColumn() {
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(TEST_SCHEMA)
                        .distributedBy(3)
                        .property(ConfigOptions.TABLE_STATISTICS_COLUMNS.key(), "metadata")
                        .build();

        assertThatThrownBy(() -> StatisticsConfigUtils.validateStatisticsConfig(descriptor))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("is not supported for statistics collection");
    }

    @Test
    void testValidateWithRowColumn() {
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(TEST_SCHEMA)
                        .distributedBy(3)
                        .property(ConfigOptions.TABLE_STATISTICS_COLUMNS.key(), "nested")
                        .build();

        assertThatThrownBy(() -> StatisticsConfigUtils.validateStatisticsConfig(descriptor))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("is not supported for statistics collection");
    }

    @Test
    void testTableConfigStatisticsColumnsDisabled() {
        // Not set -> DISABLED
        Configuration config = new Configuration();
        TableConfig tableConfig = new TableConfig(config);
        StatisticsColumnsConfig columnsConfig = tableConfig.getStatisticsColumns();

        assertThat(columnsConfig.getMode()).isEqualTo(StatisticsColumnsConfig.Mode.DISABLED);
        assertThat(columnsConfig.isEnabled()).isFalse();
    }

    @Test
    void testTableConfigStatisticsColumnsAll() {
        Configuration config = new Configuration();
        config.setString(ConfigOptions.TABLE_STATISTICS_COLUMNS.key(), "*");
        TableConfig tableConfig = new TableConfig(config);
        StatisticsColumnsConfig columnsConfig = tableConfig.getStatisticsColumns();

        assertThat(columnsConfig.getMode()).isEqualTo(StatisticsColumnsConfig.Mode.ALL);
        assertThat(columnsConfig.isEnabled()).isTrue();
    }

    @Test
    void testTableConfigStatisticsColumnsSpecified() {
        Configuration config = new Configuration();
        config.setString(ConfigOptions.TABLE_STATISTICS_COLUMNS.key(), "id,name");
        TableConfig tableConfig = new TableConfig(config);
        StatisticsColumnsConfig columnsConfig = tableConfig.getStatisticsColumns();

        assertThat(columnsConfig.getMode()).isEqualTo(StatisticsColumnsConfig.Mode.SPECIFIED);
        assertThat(columnsConfig.isEnabled()).isTrue();
        assertThat(columnsConfig.getColumns()).containsExactly("id", "name");
    }
}
