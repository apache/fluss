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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.factories.FactoryUtil;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FlinkCatalogFactory}. */
public class FlinkCatalogFactoryTest {

    @Test
    public void testCreateCatalog() {
        String catalogName = "my_catalog";
        String bootstrapServers = "localhost:9092";
        String dbName = "my_db";

        Map<String, String> options = new HashMap<>();
        options.put(FlinkConnectorOptions.BOOTSTRAP_SERVERS.key(), bootstrapServers);
        options.put(FlinkCatalogOptions.DEFAULT_DATABASE.key(), dbName);
        options.put(CommonCatalogOptions.CATALOG_TYPE.key(), FlinkCatalogFactory.IDENTIFIER);

        // test create catalog
        FlinkCatalog actualCatalog =
                (FlinkCatalog)
                        FactoryUtil.createCatalog(
                                catalogName,
                                options,
                                new Configuration(),
                                Thread.currentThread().getContextClassLoader());

        FlinkCatalog flinkCatalog =
                new FlinkCatalog(
                        catalogName,
                        dbName,
                        bootstrapServers,
                        Thread.currentThread().getContextClassLoader(),
                        Collections.emptyMap());

        checkEquals(flinkCatalog, actualCatalog);

        // test security configs
        Map<String, String> securityMap = new HashMap<>();
        securityMap.put(ConfigOptions.CLIENT_SECURITY_PROTOCOL.key(), "username_password");
        securityMap.put("client.security.username_password.username", "root");
        securityMap.put("client.security.username_password.password", "password");

        options.putAll(securityMap);
        FlinkCatalog actualCatalog2 =
                (FlinkCatalog)
                        FactoryUtil.createCatalog(
                                catalogName,
                                options,
                                new Configuration(),
                                Thread.currentThread().getContextClassLoader());

        assertThat(actualCatalog2.getSecurityConfigs()).isEqualTo(securityMap);
    }

    private static void checkEquals(FlinkCatalog c1, FlinkCatalog c2) {
        assertThat(c2.getName()).isEqualTo(c1.getName());
        assertThat(c2.getDefaultDatabase()).isEqualTo(c1.getDefaultDatabase());
    }
}
