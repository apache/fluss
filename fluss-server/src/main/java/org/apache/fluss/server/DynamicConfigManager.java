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
import org.apache.fluss.config.ConfigOption;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.cluster.AlterConfig;
import org.apache.fluss.config.cluster.ConfigEntry;
import org.apache.fluss.config.cluster.ConfigValidator;
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
import java.util.Objects;

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

    /**
     * Register a ConfigValidator for stateless validation.
     *
     * <p>Typically used by CoordinatorServer to validate configs for components it doesn't run
     * (e.g., KvManager). Validators are stateless and only perform validation without requiring
     * component instances.
     *
     * @param validator the config validator to register
     */
    public void registerValidator(ConfigValidator validator) {
        dynamicServerConfig.registerValidator(validator);
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
                                        key,
                                        dynamicServerConfig.redactConfigValue(key, value),
                                        ConfigEntry.ConfigSource.INITIAL_SERVER_CONFIG);
                        configEntries.add(configEntry);
                    }
                });
        dynamicDefaultConfigs.forEach(
                (key, value) -> {
                    ConfigEntry configEntry =
                            new ConfigEntry(
                                    key,
                                    dynamicServerConfig.redactConfigValue(key, value),
                                    ConfigEntry.ConfigSource.DYNAMIC_SERVER_CONFIG);
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
                    String configKey = alterConfigOp.key();
                    if (!dynamicServerConfig.isAllowedConfig(configKey)) {
                        throw new ConfigException(
                                String.format(
                                        "The config key %s is not allowed to be changed dynamically.",
                                        configKey));
                    }

                    String configValue = alterConfigOp.value();
                    switch (alterConfigOp.opType()) {
                        case SET:
                            configsProps.put(configKey, configValue);
                            break;
                        case DELETE:
                            configsProps.remove(configKey);
                            break;
                        case APPEND:
                            validateListOrMapType(configKey);
                            appendCollectionConfig(configsProps, configKey, configValue);
                            break;
                        case SUBTRACT:
                            validateListOrMapType(configKey);
                            subtractCollectionConfig(configsProps, configKey, configValue);
                            break;
                        default:
                            throw new ConfigException(
                                    "Unsupported config operation type " + alterConfigOp.opType());
                    }
                });
    }

    private void appendCollectionConfig(
            Map<String, String> dynamicConfigs, String configKey, String configValue) {
        if (isMapType(configKey)) {
            appendMapConfig(dynamicConfigs, configKey, configValue);
            return;
        }

        appendListConfig(dynamicConfigs, configKey, configValue);
    }

    private void appendListConfig(
            Map<String, String> dynamicConfigs, String configKey, String configValue) {
        String existingValue = getExistingConfigValue(dynamicConfigs, configKey);
        if (existingValue == null || existingValue.isEmpty()) {
            dynamicConfigs.put(configKey, configValue);
        } else {
            dynamicConfigs.put(configKey, existingValue + "," + configValue);
        }
    }

    private void subtractCollectionConfig(
            Map<String, String> dynamicConfigs, String configKey, String configValue) {
        if (isMapType(configKey)) {
            subtractMapConfig(dynamicConfigs, configKey, configValue);
            return;
        }

        subtractListConfig(dynamicConfigs, configKey, configValue);
    }

    private void subtractListConfig(
            Map<String, String> dynamicConfigs, String configKey, String configValue) {
        String existingValue = getExistingConfigValue(dynamicConfigs, configKey);
        if (existingValue == null || existingValue.isEmpty()) {
            return;
        }

        List<String> items = new ArrayList<>();
        for (String item : existingValue.split(",")) {
            String trimmed = item.trim();
            if (!trimmed.isEmpty()) {
                items.add(trimmed);
            }
        }
        items.removeIf(v -> v.equals(configValue));
        if (items.isEmpty()) {
            dynamicConfigs.put(configKey, null);
        } else {
            dynamicConfigs.put(configKey, String.join(",", items));
        }
    }

    private void appendMapConfig(
            Map<String, String> dynamicConfigs, String configKey, String configValue) {
        validateMapEntry(configKey, configValue);
        String mapEntryKey = getMapEntryKey(configValue);
        String existingValue = getExistingConfigValue(dynamicConfigs, configKey);
        if (existingValue == null || existingValue.isEmpty()) {
            dynamicConfigs.put(configKey, configValue);
            return;
        }

        String existingEntry = findMapEntry(existingValue, mapEntryKey);
        if (existingEntry != null) {
            throw new ConfigException(
                    configKey
                            + " must not contain duplicate map entry keys: '"
                            + mapEntryKey
                            + "'.");
        }
        dynamicConfigs.put(configKey, existingValue + "," + configValue);
    }

    private void subtractMapConfig(
            Map<String, String> dynamicConfigs, String configKey, String configValue) {
        validateMapEntry(configKey, configValue);
        String targetMapEntryKey = getMapEntryKey(configValue);
        String existingValue = getExistingConfigValue(dynamicConfigs, configKey);
        if (existingValue == null || existingValue.isEmpty()) {
            return;
        }

        List<String> entries = new ArrayList<>();
        boolean removed = false;
        for (String entry : existingValue.split(",")) {
            String trimmed = entry.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            if (Objects.equals(getMapEntryKey(trimmed), targetMapEntryKey)) {
                removed = true;
                continue;
            }
            entries.add(trimmed);
        }
        if (!removed) {
            return;
        }
        if (entries.isEmpty()) {
            dynamicConfigs.put(configKey, null);
        } else {
            dynamicConfigs.put(configKey, String.join(",", entries));
        }
    }

    private static String getMapEntryKey(String entry) {
        int separatorIndex = entry.indexOf(':');
        if (separatorIndex <= 0) {
            throw new ConfigException(
                    String.format("Map item is not a key-value pair: '%s'.", entry));
        }
        return entry.substring(0, separatorIndex);
    }

    private static String findMapEntry(String value, String targetKey) {
        for (String entry : value.split(",")) {
            String trimmed = entry.trim();
            if (!trimmed.isEmpty() && Objects.equals(getMapEntryKey(trimmed), targetKey)) {
                return trimmed;
            }
        }
        return null;
    }

    private static void validateMapEntry(String configKey, String configValue) {
        Configuration configuration = new Configuration();
        configuration.setString(configKey, configValue);
        try {
            configuration.get(ConfigOptions.getConfigOption(configKey));
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw new ConfigException(
                    String.format("Invalid map entry for config '%s': %s", configKey, configValue),
                    e);
        }
    }

    private String getExistingConfigValue(Map<String, String> dynamicConfigs, String configKey) {
        if (dynamicConfigs.containsKey(configKey)) {
            return dynamicConfigs.get(configKey);
        }
        return dynamicServerConfig.getInitialServerConfigs().get(configKey);
    }

    @VisibleForTesting
    protected void alterServerConfigs(Map<String, String> configsProps) throws Exception {
        dynamicServerConfig.updateDynamicConfig(configsProps, false);

        // Apply to zookeeper only after verification.
        zooKeeperClient.upsertServerEntityConfig(configsProps);
    }

    private static void validateListOrMapType(String configKey) {
        ConfigOption<?> configOption = ConfigOptions.getConfigOption(configKey);
        if (configOption == null
                || (!configOption.isList() && configOption.getClazz() != Map.class)) {
            throw new ConfigException(
                    String.format(
                            "APPEND/SUBTRACT operations are only supported for list-typed or map-typed config keys, "
                                    + "but '%s' is not a list or map type.",
                            configKey));
        }
    }

    private static boolean isMapType(String configKey) {
        ConfigOption<?> configOption = ConfigOptions.getConfigOption(configKey);
        return configOption != null && configOption.getClazz() == Map.class;
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
