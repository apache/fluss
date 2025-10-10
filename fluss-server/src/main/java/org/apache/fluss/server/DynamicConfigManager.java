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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.cluster.AlterConfig;
import org.apache.fluss.config.cluster.ConfigEntry;
import org.apache.fluss.config.cluster.ServerReconfigurable;
import org.apache.fluss.exception.ConfigException;
import org.apache.fluss.server.authorizer.ZkNodeChangeNotificationWatcher;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.ZkData.ConfigEntityChangeNotificationSequenceZNode;
import org.apache.fluss.server.zk.data.ZkData.ConfigEntityChangeNotificationZNode;
import org.apache.fluss.utils.clock.SystemClock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Manager for dynamic configurations. */
public class DynamicConfigManager {
    private static final Logger LOG = LoggerFactory.getLogger(DynamicConfigManager.class);
    private static final long CHANGE_NOTIFICATION_EXPIRATION_MS = 15 * 60 * 1000L;

    private final DynamicServerConfig dynamicServerConfig;
    private final ZooKeeperClient zooKeeperClient;
    private final ZkNodeChangeNotificationWatcher configChangeListener;
    private final boolean isCoordinator;

    public DynamicConfigManager(
            ZooKeeperClient zooKeeperClient, Configuration configuration, boolean isCoordinator) {
        this.dynamicServerConfig = new DynamicServerConfig(configuration);
        this.zooKeeperClient = zooKeeperClient;
        this.isCoordinator = isCoordinator;
        this.configChangeListener =
                new ZkNodeChangeNotificationWatcher(
                        zooKeeperClient,
                        ConfigEntityChangeNotificationZNode.path(),
                        ConfigEntityChangeNotificationSequenceZNode.prefix(),
                        CHANGE_NOTIFICATION_EXPIRATION_MS,
                        new ConfigChangedNotificationHandler(),
                        SystemClock.getInstance());
    }

    public void startup() throws Exception {
        try {
            configChangeListener.start();
            Map<String, String> entityConfigs = zooKeeperClient.fetchEntityConfig();
            dynamicServerConfig.updateDynamicConfig(entityConfigs, true);
        } catch (Exception e) {
            LOG.error("Failed to update dynamic configs from zookeeper", e);
        }
    }

    /** Register a ServerReconfigurable which listens to configuration changes. */
    public void register(ServerReconfigurable serverReconfigurable) {
        dynamicServerConfig.register(serverReconfigurable);
    }

    public void close() {
        configChangeListener.stop();
    }

    public List<ConfigEntry> describeConfigs() {
        Map<String, String> dynamicDefaultConfigs = dynamicServerConfig.getDynamicConfigs();
        Map<String, String> staticServerConfigs = dynamicServerConfig.getInitialServerConfigs();

        List<ConfigEntry> configEntries = new ArrayList<>();
        staticServerConfigs.forEach(
                (key, value) -> {
                    if (!dynamicDefaultConfigs.containsKey(key)) {
                        ConfigEntry configEntry =
                                new ConfigEntry(
                                        key, value, ConfigEntry.ConfigSource.INITIAL_SERVER_CONFIG);
                        configEntries.add(configEntry);
                    }
                });
        dynamicDefaultConfigs.forEach(
                (key, value) -> {
                    ConfigEntry configEntry =
                            new ConfigEntry(
                                    key, value, ConfigEntry.ConfigSource.DYNAMIC_SERVER_CONFIG);
                    configEntries.add(configEntry);
                });

        return configEntries;
    }

    public void alterConfigs(List<AlterConfig> clusterConfigChanges) throws Exception {
        Map<String, String> persistentProps = zooKeeperClient.fetchEntityConfig();
        prepareIncrementalConfigs(clusterConfigChanges, persistentProps);
        alterServerConfigs(persistentProps);
    }

    private void prepareIncrementalConfigs(
            List<AlterConfig> alterConfigs, Map<String, String> configsProps) {
        alterConfigs.forEach(
                alterConfigOp -> {
                    String configPropName = alterConfigOp.key();
                    if (!dynamicServerConfig.isAllowedConfig(configPropName)) {
                        throw new ConfigException(
                                String.format(
                                        "The config key %s is not allowed to be changed dynamically.",
                                        configPropName));
                    }

                    String configPropValue = alterConfigOp.value();
                    switch (alterConfigOp.opType()) {
                        case SET:
                            configsProps.put(configPropName, configPropValue);
                            break;
                        case DELETE:
                            configsProps.remove(configPropName);
                            break;
                        default:
                            throw new ConfigException(
                                    "Unsupported config operation type " + alterConfigOp.opType());
                    }
                });
    }

    @VisibleForTesting
    protected void alterServerConfigs(Map<String, String> configsProps) throws Exception {
        dynamicServerConfig.updateDynamicConfig(configsProps, false);

        // Apply to zookeeper only after verification.
        zooKeeperClient.upsertServerEntityConfig(configsProps);
    }

    private class ConfigChangedNotificationHandler
            implements ZkNodeChangeNotificationWatcher.NotificationHandler {

        @Override
        public void processNotification(byte[] notification) throws Exception {
            if (isCoordinator) {
                return;
            }

            if (notification.length != 0) {
                throw new ConfigException(
                        "Config change notification of this version is only empty");
            }

            Map<String, String> entityConfig = zooKeeperClient.fetchEntityConfig();
            dynamicServerConfig.updateDynamicConfig(entityConfig, true);
        }
    }
}
