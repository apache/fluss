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

package org.apache.fluss.server;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.dynamic.AlterConfigOp;
import org.apache.fluss.exception.ConfigException;
import org.apache.fluss.server.coordinator.LakeCatalogDynamicLoader;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.ZooKeeperUtils;
import org.apache.fluss.server.zk.data.ZkData.ConfigEntityZNode;
import org.apache.fluss.testutils.common.AllCallbackWrapper;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.config.ConfigOptions.DATALAKE_FORMAT;
import static org.apache.fluss.metadata.DataLakeFormat.PAIMON;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link DynamicConfigManager}. */
public class DynamicConfigChangeTest {

    @RegisterExtension
    public static AllCallbackWrapper<ZooKeeperExtension> zooKeeperExtensionWrapper =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    protected static ZooKeeperClient zookeeperClient;

    @BeforeAll
    static void beforeAll() {
        final Configuration configuration = new Configuration();
        configuration.setString(
                ConfigOptions.ZOOKEEPER_ADDRESS,
                zooKeeperExtensionWrapper.getCustomExtension().getConnectString());
        zookeeperClient =
                ZooKeeperUtils.startZookeeperClient(configuration, NOPErrorHandler.INSTANCE);
    }

    @AfterAll
    static void afterAll() {
        if (zookeeperClient != null) {
            zookeeperClient.close();
        }
    }

    @AfterEach
    void after() throws Exception {
        zookeeperClient.deletePath(ConfigEntityZNode.path());
    }

    @Test
    void testAlterLakehouseConfigs() throws Exception {
        DynamicServerConfig dynamicServerConfig = new DynamicServerConfig(new Configuration());
        DynamicConfigManager dynamicConfigManager =
                new DynamicConfigManager(zookeeperClient, dynamicServerConfig, true);
        dynamicConfigManager.startup();
        try (LakeCatalogDynamicLoader lakeCatalogDynamicLoader =
                new LakeCatalogDynamicLoader(dynamicServerConfig, null, true)) {
            assertThatThrownBy(
                            () ->
                                    dynamicConfigManager.alterConfigs(
                                            Collections.singletonList(
                                                    new AlterConfigOp(
                                                            "un_support_key",
                                                            "value",
                                                            AlterConfigOp.OpType.SET))))
                    .isExactlyInstanceOf(ConfigException.class)
                    .hasMessageContaining(
                            "The config key un_support_key is not allowed to be changed dynamically.");

            dynamicConfigManager.alterConfigs(
                    Arrays.asList(
                            new AlterConfigOp(
                                    DATALAKE_FORMAT.key(), "paimon", AlterConfigOp.OpType.SET),
                            new AlterConfigOp(
                                    "datalake.paimon.metastore",
                                    "filesystem",
                                    AlterConfigOp.OpType.SET)));
            assertThat(lakeCatalogDynamicLoader.getDataLakeFormat()).isEqualTo(PAIMON);
            assertThat(lakeCatalogDynamicLoader.getDefaultLakeProperties())
                    .isEqualTo(
                            Collections.singletonMap(
                                    "table.datalake.paimon.metastore", "filesystem"));
            dynamicConfigManager.alterConfigs(
                    Collections.singletonList(
                            new AlterConfigOp(
                                    DATALAKE_FORMAT.key(), null, AlterConfigOp.OpType.DELETE)));
            assertThat(lakeCatalogDynamicLoader.getDataLakeFormat()).isEqualTo(null);
            assertThat(lakeCatalogDynamicLoader.getDefaultLakeProperties()).isNull();
        }
    }

    @Test
    void testOverrideConfigs() throws Exception {
        Configuration configuration = new Configuration();
        configuration.setString(DATALAKE_FORMAT.key(), "paimon");
        DynamicServerConfig dynamicServerConfig = new DynamicServerConfig(configuration);
        DynamicConfigManager dynamicConfigManager =
                new DynamicConfigManager(zookeeperClient, dynamicServerConfig, true);
        dynamicConfigManager.startup();
        try (LakeCatalogDynamicLoader lakeCatalogDynamicLoader =
                new LakeCatalogDynamicLoader(dynamicServerConfig, null, true)) {
            assertThat(lakeCatalogDynamicLoader.getDataLakeFormat()).isEqualTo(PAIMON);
            dynamicConfigManager.alterConfigs(
                    Collections.singletonList(
                            new AlterConfigOp(
                                    DATALAKE_FORMAT.key(), null, AlterConfigOp.OpType.SET)));
            assertThat(lakeCatalogDynamicLoader.getDataLakeFormat()).isEqualTo(null);
            dynamicConfigManager.alterConfigs(
                    Collections.singletonList(
                            new AlterConfigOp(
                                    DATALAKE_FORMAT.key(), null, AlterConfigOp.OpType.DELETE)));
            assertThat(lakeCatalogDynamicLoader.getDataLakeFormat()).isEqualTo(PAIMON);
        }
    }

    @Test
    void testUnknownLakeHouse() throws Exception {
        Configuration configuration = new Configuration();
        DynamicServerConfig dynamicServerConfig = new DynamicServerConfig(configuration);
        DynamicConfigManager dynamicConfigManager =
                new DynamicConfigManager(zookeeperClient, dynamicServerConfig, true);
        dynamicConfigManager.startup();
        try (LakeCatalogDynamicLoader lakeCatalogDynamicLoader =
                new LakeCatalogDynamicLoader(dynamicServerConfig, null, true)) {
            assertThatThrownBy(
                            () ->
                                    dynamicConfigManager.alterConfigs(
                                            Collections.singletonList(
                                                    new AlterConfigOp(
                                                            DATALAKE_FORMAT.key(),
                                                            "unknown",
                                                            AlterConfigOp.OpType.SET))))
                    .hasMessageContaining(
                            "Could not parse value 'unknown' for key 'datalake.format'");

            assertThat(lakeCatalogDynamicLoader.getDataLakeFormat()).isNull();
        }
    }

    @Test
    void testWrongLakeFormatPrefix() throws Exception {
        Configuration configuration = new Configuration();
        DynamicServerConfig dynamicServerConfig = new DynamicServerConfig(configuration);
        DynamicConfigManager dynamicConfigManager =
                new DynamicConfigManager(zookeeperClient, dynamicServerConfig, true);
        dynamicConfigManager.startup();
        try (LakeCatalogDynamicLoader lakeCatalogDynamicLoader =
                new LakeCatalogDynamicLoader(dynamicServerConfig, null, true)) {
            assertThatThrownBy(
                            () ->
                                    dynamicConfigManager.alterConfigs(
                                            Arrays.asList(
                                                    new AlterConfigOp(
                                                            DATALAKE_FORMAT.key(),
                                                            "paimon",
                                                            AlterConfigOp.OpType.SET),
                                                    new AlterConfigOp(
                                                            "datalake.iceberg.metastore",
                                                            "filesystem",
                                                            AlterConfigOp.OpType.SET))))
                    .hasMessage(
                            "Invalid configuration for datalake format paimon: datalake.iceberg.metastore");

            assertThat(lakeCatalogDynamicLoader.getDataLakeFormat()).isNull();
        }
    }

    @Test
    void testListenUnMatchedDynamicConfigChanges() throws Exception {
        DynamicServerConfig dynamicServerConfig = new DynamicServerConfig(new Configuration());
        DynamicConfigManager dynamicConfigManager =
                new DynamicConfigManager(zookeeperClient, dynamicServerConfig, false);
        dynamicConfigManager.startup();
        try (LakeCatalogDynamicLoader lakeCatalogDynamicLoader =
                new LakeCatalogDynamicLoader(dynamicServerConfig, null, true)) {
            assertThat(lakeCatalogDynamicLoader.getDataLakeFormat()).isEqualTo(null);
            Map<String, String> config = new HashMap<>();
            config.put(DATALAKE_FORMAT.key(), "paimon");
            config.put("un_support_key", "value");
            zookeeperClient.upsertServerEntityConfig(config);
            retry(
                    Duration.ofMinutes(1),
                    () ->
                            assertThat(lakeCatalogDynamicLoader.getDataLakeFormat())
                                    .isEqualTo(PAIMON));
        }
    }

    @Test
    void testReStartupContainsNoMatchedDynamicConfig() throws Exception {
        DynamicServerConfig dynamicServerConfig = new DynamicServerConfig(new Configuration());
        Map<String, String> config = new HashMap<>();
        config.put(DATALAKE_FORMAT.key(), "paimon");
        config.put("un_support_key", "value");

        // This often happens when upgrading with different allowed configs.
        zookeeperClient.upsertServerEntityConfig(config);
        DynamicConfigManager dynamicConfigManager =
                new DynamicConfigManager(zookeeperClient, dynamicServerConfig, true);
        // Startup dynamic manager even is not matched now.
        dynamicConfigManager.startup();

        try (LakeCatalogDynamicLoader lakeCatalogDynamicLoader =
                new LakeCatalogDynamicLoader(dynamicServerConfig, null, true)) {
            assertThat(lakeCatalogDynamicLoader.getDataLakeFormat()).isEqualTo(PAIMON);
        }
    }
}
