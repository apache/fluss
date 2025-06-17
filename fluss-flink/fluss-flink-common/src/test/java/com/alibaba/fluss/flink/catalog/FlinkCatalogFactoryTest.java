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

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.flink.FlinkConnectorOptions;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.factories.FactoryUtil;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link FlinkCatalogFactory}. */
abstract class FlinkCatalogFactoryTest {

    static final String CATALOG_NAME = "my_catalog";
    static final String BOOTSTRAP_SERVERS_NAME = "localhost:9092";
    static final String DB_NAME = "my_db";

    @Test
    public void testCreateCatalog() {
        Map<String, String> options = new HashMap<>();
        options.put(FlinkConnectorOptions.BOOTSTRAP_SERVERS.key(), BOOTSTRAP_SERVERS_NAME);
        options.put(FlinkCatalogOptions.DEFAULT_DATABASE.key(), DB_NAME);
        options.put(CommonCatalogOptions.CATALOG_TYPE.key(), FlinkCatalogFactory.IDENTIFIER);

        // test create catalog
        FlinkCatalog actualCatalog =
                (FlinkCatalog)
                        FactoryUtil.createCatalog(
                                CATALOG_NAME,
                                options,
                                new Configuration(),
                                Thread.currentThread().getContextClassLoader());

        FlinkCatalog flinkCatalog =
                new FlinkCatalog(
                        CATALOG_NAME,
                        DB_NAME,
                        BOOTSTRAP_SERVERS_NAME,
                        Thread.currentThread().getContextClassLoader(),
                        Collections.emptyMap());

        checkEquals(flinkCatalog, actualCatalog);

        // test security configs
        Map<String, String> securityMap = new HashMap<>();
        securityMap.put(ConfigOptions.CLIENT_SECURITY_PROTOCOL.key(), "sasl");
        securityMap.put(ConfigOptions.CLIENT_SASL_MECHANISM.key(), "plain");
        securityMap.put("client.security.sasl.username", "root");
        securityMap.put("client.security.sasl.password", "password");

        options.putAll(securityMap);
        FlinkCatalog actualCatalog2 =
                (FlinkCatalog)
                        FactoryUtil.createCatalog(
                                CATALOG_NAME,
                                options,
                                new Configuration(),
                                Thread.currentThread().getContextClassLoader());

        assertThat(actualCatalog2.getSecurityConfigs()).isEqualTo(securityMap);
    }

    @Test
    public void testOptions() {
        Map<String, String> options = new HashMap<>();
        options.put(CommonCatalogOptions.CATALOG_TYPE.key(), FlinkCatalogFactory.IDENTIFIER);

        // test required options
        assertThatThrownBy(
                        () ->
                                FactoryUtil.createCatalog(
                                        CATALOG_NAME,
                                        options,
                                        new Configuration(),
                                        Thread.currentThread().getContextClassLoader()))
                .rootCause()
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Missing required options are:\n" + "\n" + "bootstrap.servers");

        // test op options
        options.put(FlinkConnectorOptions.BOOTSTRAP_SERVERS.key(), BOOTSTRAP_SERVERS_NAME);
        FlinkCatalog actualCatalog =
                (FlinkCatalog)
                        FactoryUtil.createCatalog(
                                CATALOG_NAME,
                                options,
                                new Configuration(),
                                Thread.currentThread().getContextClassLoader());

        assertThat(actualCatalog.getDefaultDatabase())
                .isEqualTo(FlinkCatalogOptions.DEFAULT_DATABASE.defaultValue());
    }

    private static void checkEquals(FlinkCatalog c1, FlinkCatalog c2) {
        assertThat(c2.getName()).isEqualTo(c1.getName());
        assertThat(c2.getDefaultDatabase()).isEqualTo(c1.getDefaultDatabase());
    }

    @Test
    public void testOptionalOptionsConfiguration() {
        FlinkCatalogFactory factory = new FlinkCatalogFactory();

        // Test that optionalOptions() correctly declares DEFAULT_DATABASE
        Set<ConfigOption<?>> optionalOptions = factory.optionalOptions();
        assertThat(optionalOptions)
                .hasSize(1)
                .containsExactly(FlinkCatalogOptions.DEFAULT_DATABASE);

        ConfigOption<?> defaultDbOption = FlinkCatalogOptions.DEFAULT_DATABASE;
        assertThat(defaultDbOption.key()).isEqualTo("default-database");
        assertThat(defaultDbOption.hasDefaultValue()).isTrue();
        assertThat(defaultDbOption.defaultValue()).isEqualTo("fluss");
    }
}
