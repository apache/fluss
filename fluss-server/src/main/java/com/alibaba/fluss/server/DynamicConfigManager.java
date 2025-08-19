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

package com.alibaba.fluss.server;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.config.dynamic.AlterConfigOp;
import com.alibaba.fluss.config.dynamic.ConfigEntry;
import com.alibaba.fluss.exception.ConfigException;
import com.alibaba.fluss.server.authorizer.ZkNodeChangeNotificationWatcher;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.data.ZkData;
import com.alibaba.fluss.utils.clock.SystemClock;

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
            ZooKeeperClient zooKeeperClient,
            DynamicServerConfig dynamicServerConfig,
            boolean isCoordinator) {
        this.dynamicServerConfig = dynamicServerConfig;
        this.zooKeeperClient = zooKeeperClient;
        this.isCoordinator = isCoordinator;
        this.configChangeListener =
                new ZkNodeChangeNotificationWatcher(
                        zooKeeperClient,
                        ZkData.ConfigEntityChangeNotificationZNode.path(),
                        ZkData.ConfigEntityChangeNotificationSequenceZNode.prefix(),
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

    public void close() {
        configChangeListener.stop();
    }

    public List<ConfigEntry> describeConfigs() {
        Map<String, String> dynamicDefaultConfigs = dynamicServerConfig.getDynamicConfigs();
        Map<String, String> staticServerConfigs = dynamicServerConfig.getInitialServerConfigs();

        List<ConfigEntry> configEntries = new ArrayList<>();
        staticServerConfigs.forEach(
                (key, value) -> {
                    ConfigEntry configEntry =
                            new ConfigEntry(
                                    key, value, ConfigEntry.ConfigSource.INITIAL_SERVER_CONFIG);
                    configEntries.add(configEntry);
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

    public void alterConfigs(List<AlterConfigOp> serverConfigChanges) throws Exception {
        Map<String, String> persistentProps = zooKeeperClient.fetchEntityConfig();
        prepareIncrementalConfigs(serverConfigChanges, persistentProps);
        alterServerConfigs(persistentProps);
    }

    private void prepareIncrementalConfigs(
            List<AlterConfigOp> alterConfigOps, Map<String, String> configsProps) {
        alterConfigOps.forEach(
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
                            //                        case APPEND:
                            //                            {
                            //                                //                    if
                            // (!configKeys.containsKey(configPropName) ||
                            //                                //
                            // !configKeys.get(configPropName).isList()) {
                            //                                //                        throw new
                            // InvalidRequestException("Config
                            //                                // value append is not allowed for
                            // config key " + configPropName);
                            //                                //                    }
                            //                                List<String> oldValueList =
                            //
                            // getOldListValue(configPropName, configsProps, configKeys);
                            //                                List<String> appendValueList =
                            //
                            // Arrays.asList(configPropValue.split(","));
                            //                                ArrayList<String> newValueList = new
                            // ArrayList<>(oldValueList);
                            //                                newValueList.addAll(appendValueList);
                            //                                configsProps.put(configPropName,
                            // String.join(",", newValueList));
                            //                                break;
                            //                            }
                            //                        case SUBTRACT:
                            //                            {
                            //                                //                    if
                            // (!configKeys.containsKey(configPropName) ||
                            //                                //
                            // !configKeys.get(configPropName).isList()) {
                            //                                //                        throw new
                            // InvalidRequestException("Config
                            //                                // value subtract is not allowed for
                            // config key " + configPropName);
                            //                                //                    }
                            //                                List<String> oldValueList =
                            //
                            // getOldListValue(configPropName, configsProps, configKeys);
                            //                                List<String> substractValueList =
                            //
                            // Arrays.asList(configPropValue.split(","));
                            //                                ArrayList<String> newValueList = new
                            // ArrayList<>(oldValueList);
                            //
                            // newValueList.removeAll(substractValueList);
                            //                                configsProps.put(configPropName,
                            // String.join(",", newValueList));
                            //                                break;
                            //                            }
                        default:
                            throw new ConfigException(
                                    "Unknown config operation type " + alterConfigOp.opType());
                    }
                });
    }

    //    private List<String> getOldListValue(
    //            String configPropName,
    //            Map<String, String> configsProps,
    //            Map<String, ConfigOption<?>> configKeys) {
    //        List<String> oldValueList;
    //        if (configsProps.containsKey(configPropName)) {
    //            oldValueList = Arrays.asList(configsProps.get(configPropName).split(","));
    //        } else if (configKeys.get(configPropName).hasDefaultValue()) {
    //            List<?> list = (List<?>) configKeys.get(configPropName).defaultValue();
    //            oldValueList = list.stream().map(String::valueOf).collect(Collectors.toList());
    //        } else {
    //            oldValueList = Collections.emptyList();
    //        }
    //        return oldValueList;
    //    }

    @VisibleForTesting
    protected void alterServerConfigs(Map<String, String> configsProps) throws Exception {
        dynamicServerConfig.updateDynamicConfig(configsProps, false);

        // only after verify can add and apply.
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
            // todo: log日志，对一些敏感的key加密
            dynamicServerConfig.updateDynamicConfig(entityConfig, true);
        }
    }
}
