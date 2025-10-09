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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.dynamic.ServerReconfigurable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.fluss.config.ConfigOptions.DATALAKE_FORMAT;
import static org.apache.fluss.utils.concurrent.LockUtils.inReadLock;
import static org.apache.fluss.utils.concurrent.LockUtils.inWriteLock;

/**
 * The dynamic configuration for server. If a {@link ServerReconfigurable} implementation class
 * wants to listen for configuration changes, it can register through a method. Subsequently, when
 * {@link DynamicConfigManager} detects changes, it will update the configuration items and push
 * them to these {@link ServerReconfigurable} instances.
 */
@Internal
public class DynamicServerConfig {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicServerConfig.class);
    private static final Set<String> ALLOWED_CONFIG_KEYS =
            Collections.singleton(DATALAKE_FORMAT.key());
    private static final Set<String> ALLOWED_CONFIG_PREFIXES = Collections.singleton("datalake.");

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Set<ServerReconfigurable> serverReconfigurableSet = ConcurrentHashMap.newKeySet();

    /** The initial configuration items when the server starts from server.yaml. */
    private final Map<String, String> initialConfigMap;

    /** The dynamic configuration items that are added during running(stored in zk). */
    private final Map<String, String> dynamicConfigs = new HashMap<>();

    /**
     * The current configuration map, which is a combination of initial configuration and dynamic.
     */
    private final Map<String, String> currentConfigMap;

    /**
     * The current configuration, which is a combination of initial configuration and dynamic
     * configuration.
     */
    private volatile Configuration currentConfig;

    public DynamicServerConfig(Configuration flussConfig) {
        this.currentConfig = flussConfig;
        this.initialConfigMap = flussConfig.toMap();
        this.currentConfigMap = flussConfig.toMap();
    }

    /** Register a ServerReconfigurable which listens to configuration changes. */
    public void register(ServerReconfigurable serverReconfigurable) {
        serverReconfigurableSet.add(serverReconfigurable);
    }

    /** Update the dynamic configuration and apply to registered ServerReconfigurables. */
    public void updateDynamicConfig(Map<String, String> newDynamicConfigs, boolean skipErrorConfig)
            throws Exception {
        inWriteLock(lock, () -> updateCurrentConfig(newDynamicConfigs, skipErrorConfig));
    }

    public Configuration getCurrentConfig() {
        return inReadLock(lock, () -> currentConfig);
    }

    public Map<String, String> getDynamicConfigs() {
        return inReadLock(lock, () -> new HashMap<>(dynamicConfigs));
    }

    public Map<String, String> getInitialServerConfigs() {
        return inReadLock(lock, () -> new HashMap<>(initialConfigMap));
    }

    public boolean isAllowedConfig(String key) {
        if (ALLOWED_CONFIG_KEYS.contains(key)) {
            return true;
        }

        for (String prefix : ALLOWED_CONFIG_PREFIXES) {
            if (key.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    private void updateCurrentConfig(Map<String, String> newDynamicConfigs, boolean skipErrorConfig)
            throws Exception {
        Map<String, String> newProps = new HashMap<>(initialConfigMap);
        overrideProps(newProps, newDynamicConfigs);
        Configuration newConfig = Configuration.fromMap(newProps);
        Configuration oldConfig = currentConfig;
        Set<ServerReconfigurable> appliedServerReconfigurableSet = new HashSet<>();
        if (!newProps.equals(currentConfigMap)) {
            serverReconfigurableSet.forEach(
                    serverReconfigurable -> {
                        try {
                            serverReconfigurable.validate(newConfig);
                        } catch (Exception e) {
                            LOG.error(
                                    "Validate new dynamic config error and will roll back all the applied config.",
                                    e);
                            if (!skipErrorConfig) {
                                throw e;
                            }
                        }
                    });

            Exception throwable = null;
            for (ServerReconfigurable serverReconfigurable : serverReconfigurableSet) {
                try {
                    serverReconfigurable.reconfigure(newConfig);
                    appliedServerReconfigurableSet.add(serverReconfigurable);
                } catch (Exception e) {
                    LOG.error(
                            "Apply new dynamic error and will roll back all the applied config.",
                            e);
                    if (!skipErrorConfig) {
                        throwable = e;
                        break;
                    }
                }
            }

            // rollback to old config if there is an error.
            if (throwable != null) {
                appliedServerReconfigurableSet.forEach(
                        serverReconfigurable -> serverReconfigurable.reconfigure(oldConfig));
                throw throwable;
            }

            currentConfig = newConfig;
            currentConfigMap.clear();
            dynamicConfigs.clear();
            currentConfigMap.putAll(newProps);
            dynamicConfigs.putAll(newDynamicConfigs);
            LOG.info("Dynamic configs changed: {}", newDynamicConfigs);
        }
    }

    private void overrideProps(Map<String, String> props, Map<String, String> propsOverride) {
        propsOverride.forEach(
                (key, value) -> {
                    if (value == null) {
                        props.remove(key);
                    } else {
                        props.put(key, value);
                    }
                });
    }
}
