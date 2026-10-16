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
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.ZkData.DatabasesZNode;
import org.apache.fluss.testutils.common.AllCallbackWrapper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.apache.fluss.server.coordinator.CoordinatorServer.DEFAULT_DATABASE;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests startup database creation for native and Kafka default database configurations. */
class KafkaDefaultDatabaseTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZK =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private MetadataManager manager;

    @BeforeEach
    void setup() throws Exception {
        ZooKeeperClient zk = ZK.getCustomExtension().getZooKeeperClient(NOPErrorHandler.INSTANCE);
        if (zk.getStat(DatabasesZNode.path()).isPresent()) {
            zk.getCuratorClient()
                    .delete()
                    .deletingChildrenIfNeeded()
                    .forPath(DatabasesZNode.path());
        }
        manager =
                new MetadataManager(
                        zk,
                        new Configuration(),
                        new LakeCatalogDynamicLoader(new Configuration(), null, true));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testNativeDefaultFollowsClusterInitialization(boolean hasDatabase) {
        if (hasDatabase) {
            manager.createDatabase("existing", DatabaseDescriptor.EMPTY, false);
        }
        Configuration config = kafkaConfiguration(DEFAULT_DATABASE);
        CoordinatorServer.createDefaultDatabases(manager, config);
        CoordinatorServer.createDefaultDatabases(manager, config);
        assertThat(manager.listDatabases())
                .containsExactly(hasDatabase ? "existing" : DEFAULT_DATABASE);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testCreateMissingCustomDefaultAndPreserveExisting(boolean exists) {
        String database = "kafka_default";
        manager.createDatabase("existing", DatabaseDescriptor.EMPTY, false);
        DatabaseDescriptor descriptor =
                DatabaseDescriptor.builder().comment("existing default").build();
        if (exists) {
            manager.createDatabase(database, descriptor, false);
        }
        Configuration config = kafkaConfiguration(database);
        CoordinatorServer.createDefaultDatabases(manager, config);
        CoordinatorServer.createDefaultDatabases(manager, config);
        assertThat(manager.listDatabases()).containsExactlyInAnyOrder("existing", database);
        assertThat(manager.getDatabase(database).getDatabaseDescriptor().toJsonBytes())
                .isEqualTo((exists ? descriptor : DatabaseDescriptor.EMPTY).toJsonBytes());
    }

    @Test
    void testDisabledKafkaDoesNotCreateConfiguredDatabase() {
        Configuration config = kafkaConfiguration("kafka_default");
        config.set(ConfigOptions.KAFKA_ENABLED, false);
        CoordinatorServer.createDefaultDatabases(manager, config);
        assertThat(manager.listDatabases()).containsExactly(DEFAULT_DATABASE);
    }

    private static Configuration kafkaConfiguration(String database) {
        Configuration config = new Configuration();
        config.set(ConfigOptions.KAFKA_ENABLED, true);
        config.set(ConfigOptions.KAFKA_DEFAULT_DATABASE, database);
        return config;
    }
}
