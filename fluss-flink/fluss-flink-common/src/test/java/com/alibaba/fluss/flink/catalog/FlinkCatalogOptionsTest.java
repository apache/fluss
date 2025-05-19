/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.catalog;

import org.apache.flink.configuration.Configuration;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link FlinkCatalogOptions}.
 *
 * <p>These tests verify the configuration options defined in the FlinkCatalogOptions class work as
 * expected, with correct default values and proper integration with Flink's configuration system.
 */
public class FlinkCatalogOptionsTest {

    @Test
    public void testDefaultDatabaseOption() {

        Assertions.assertThat(FlinkCatalogOptions.DEFAULT_DATABASE.key())
                .isEqualTo("default-database");

        Assertions.assertThat(FlinkCatalogOptions.DEFAULT_DATABASE.defaultValue())
                .isEqualTo("fluss");

        Assertions.assertThat(FlinkCatalogOptions.DEFAULT_DATABASE.description()).isNotNull();

        // Test that the option works with Configuration
        Configuration config = new Configuration();
        Assertions.assertThat(config.get(FlinkCatalogOptions.DEFAULT_DATABASE)).isEqualTo("fluss");

        // Test setting a custom value
        String customValue = "custom_database";
        config.set(FlinkCatalogOptions.DEFAULT_DATABASE, customValue);
        Assertions.assertThat(config.get(FlinkCatalogOptions.DEFAULT_DATABASE))
                .isEqualTo(customValue);
    }
}
