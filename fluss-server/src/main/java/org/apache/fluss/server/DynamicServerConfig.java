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
import org.apache.fluss.config.ConfigOption;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.cluster.ConfigValidator;
import org.apache.fluss.config.cluster.ServerReconfigurable;
import org.apache.fluss.exception.ConfigException;
import org.apache.fluss.utils.MapUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.fluss.config.ConfigOptions.DATALAKE_FORMAT;
import static org.apache.fluss.config.ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC;
import static org.apache.fluss.utils.concurrent.LockUtils.inReadLock;
import static org.apache.fluss.utils.concurrent.LockUtils.inWriteLock;

/**
 * The dynamic configuration for server. If a {@link ServerReconfigurable} implementation class
 * wants to listen for configuration changes, it can register through a method. Subsequently, when
 * {@link DynamicConfigManager} detects changes, it will update the configuration items and push
 * them to these {@link ServerReconfigurable} instances.
 */
@Internal
class DynamicServerConfig {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicServerConfig.class);
    private static final Set<String> ALLOWED_CONFIG_KEYS =
            new HashSet<>(
                    Arrays.asList(
                            DATALAKE_FORMAT.key(), KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key()));
    private static final Set<String> ALLOWED_CONFIG_PREFIXES = Collections.singleton("datalake.");

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Map<Class<? extends ServerReconfigurable>, ServerReconfigurable>
            serverReconfigures = MapUtils.newConcurrentHashMap();

    /** Registered stateless config validators, organized by config key for efficient lookup. */
    private final Map<String, List<ConfigValidator<?>>> configValidatorsByKey =
            MapUtils.newConcurrentHashMap();

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
    private Configuration currentConfig;

    DynamicServerConfig(Configuration flussConfig) {
        this.currentConfig = flussConfig;
        this.initialConfigMap = flussConfig.toMap();
        this.currentConfigMap = flussConfig.toMap();
    }

    void register(ServerReconfigurable serverReconfigurable) {
        serverReconfigures.put(serverReconfigurable.getClass(), serverReconfigurable);
    }

    /**
     * Register a ConfigValidator for stateless validation.
     *
     * <p>This is typically used by CoordinatorServer to validate configs for components it doesn't
     * run (e.g., KvManager). Validators are stateless and only perform validation without requiring
     * component instances.
     *
     * <p>Validators are organized by config key for efficient lookup. Multiple validators can be
     * registered for the same config key.
     *
     * @param validator the config validator to register
     */
    void registerValidator(ConfigValidator<?> validator) {
        String configKey = validator.configKey();
        configValidatorsByKey
                .computeIfAbsent(configKey, k -> new CopyOnWriteArrayList<>())
                .add(validator);
    }

    /**
     * Update the dynamic configuration and apply to registered ServerReconfigurable. If skipping
     * error config, only the error one will be ignored.
     */
    void updateDynamicConfig(Map<String, String> newDynamicConfigs, boolean skipErrorConfig)
            throws Exception {
        inWriteLock(lock, () -> updateCurrentConfig(newDynamicConfigs, skipErrorConfig));
    }

    Map<String, String> getDynamicConfigs() {
        return inReadLock(lock, () -> new HashMap<>(dynamicConfigs));
    }

    Map<String, String> getInitialServerConfigs() {
        return inReadLock(lock, () -> new HashMap<>(initialConfigMap));
    }

    boolean isAllowedConfig(String key) {
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
        // Compute effective config changes (merge with initial configs)
        Map<String, String> effectiveChanges =
                computeEffectiveChanges(newDynamicConfigs, skipErrorConfig);

        // Early return if no effective changes
        if (effectiveChanges.isEmpty()) {
            LOG.info("No effective config changes detected for: {}", newDynamicConfigs);
            return;
        }

        // Build new configuration by merging initial + dynamic configs
        Map<String, String> newConfigMap = buildConfigMap(effectiveChanges);
        Configuration newConfig = Configuration.fromMap(newConfigMap);

        // Apply changes to all registered ServerReconfigurable instances
        applyToServerReconfigurables(newConfig, skipErrorConfig);

        // Update internal state
        updateInternalState(newConfig, newConfigMap, newDynamicConfigs);
        LOG.info("Dynamic configs changed: {}", effectiveChanges);
    }

    /**
     * Computes effective config changes by validating new configs and handling deletions.
     *
     * @param newDynamicConfigs new dynamic configs from ZooKeeper
     * @param skipErrorConfig whether to skip invalid configs
     * @return map of config changes that passed validation
     * @throws ConfigException if validation fails and skipErrorConfig is false
     */
    private Map<String, String> computeEffectiveChanges(
            Map<String, String> newDynamicConfigs, boolean skipErrorConfig) throws ConfigException {
        Map<String, String> effectiveChanges = new HashMap<>();
        Set<String> skippedConfigs = new HashSet<>();

        // Process deleted configs: restore to initial value or remove
        processDeletions(newDynamicConfigs, effectiveChanges, skippedConfigs, skipErrorConfig);

        // Process added/modified configs
        processModifications(newDynamicConfigs, effectiveChanges, skippedConfigs, skipErrorConfig);

        if (!skippedConfigs.isEmpty()) {
            LOG.warn("Skipped invalid configs: {}", skippedConfigs);
        }

        return effectiveChanges;
    }

    /** Processes config deletions by restoring to initial values or removing them. */
    private void processDeletions(
            Map<String, String> newDynamicConfigs,
            Map<String, String> effectiveChanges,
            Set<String> skippedConfigs,
            boolean skipErrorConfig)
            throws ConfigException {
        for (String configKey : dynamicConfigs.keySet()) {
            if (newDynamicConfigs.containsKey(configKey)) {
                continue; // Not deleted
            }

            String currentValue = currentConfigMap.get(configKey);
            String initialValue = initialConfigMap.get(configKey);

            // Determine target value: initial value or null (removal)
            String targetValue = initialValue;

            // Skip if no change needed (already at initial value)
            if (Objects.equals(currentValue, targetValue)) {
                continue;
            }

            // Validate the change
            if (validateConfigChange(
                    configKey, currentValue, targetValue, skippedConfigs, skipErrorConfig)) {
                effectiveChanges.put(configKey, targetValue);
            }
        }
    }

    /** Processes config additions and modifications. */
    private void processModifications(
            Map<String, String> newDynamicConfigs,
            Map<String, String> effectiveChanges,
            Set<String> skippedConfigs,
            boolean skipErrorConfig)
            throws ConfigException {
        for (Map.Entry<String, String> entry : newDynamicConfigs.entrySet()) {
            String configKey = entry.getKey();
            String newValue = entry.getValue();
            String currentValue = currentConfigMap.get(configKey);

            // Skip if value unchanged
            if (Objects.equals(currentValue, newValue)) {
                continue;
            }

            // Validate and add to effective changes
            if (validateConfigChange(
                    configKey, currentValue, newValue, skippedConfigs, skipErrorConfig)) {
                effectiveChanges.put(configKey, newValue);
            }
        }
    }

    /**
     * Validates a single config change.
     *
     * @return true if validation passed, false if skipped due to error
     * @throws ConfigException if validation fails and skipErrorConfig is false
     */
    private boolean validateConfigChange(
            String configKey,
            String oldValue,
            String newValue,
            Set<String> skippedConfigs,
            boolean skipErrorConfig)
            throws ConfigException {
        try {
            validateSingleConfig(configKey, oldValue, newValue);
            return true;
        } catch (ConfigException e) {
            LOG.error(
                    "Config validation failed for '{}': {} -> {}. {}",
                    configKey,
                    oldValue,
                    newValue,
                    e.getMessage());
            if (skipErrorConfig) {
                skippedConfigs.add(configKey);
                return false;
            } else {
                throw e;
            }
        }
    }

    /** Builds final config map by merging initial configs with effective changes. */
    private Map<String, String> buildConfigMap(Map<String, String> effectiveChanges) {
        Map<String, String> configMap = new HashMap<>(initialConfigMap);
        effectiveChanges.forEach(
                (key, value) -> {
                    if (value == null) {
                        configMap.remove(key);
                    } else {
                        configMap.put(key, value);
                    }
                });
        return configMap;
    }

    /** Updates internal state after successful reconfiguration. */
    private void updateInternalState(
            Configuration newConfig,
            Map<String, String> newConfigMap,
            Map<String, String> newDynamicConfigs) {
        currentConfig = newConfig;
        currentConfigMap.clear();
        currentConfigMap.putAll(newConfigMap);
        dynamicConfigs.clear();
        dynamicConfigs.putAll(newDynamicConfigs);
    }

    /**
     * Validates a single config entry including type parsing and business validation.
     *
     * @param configKey config key
     * @param oldValueStr old value string
     * @param newValueStr new value string
     * @throws ConfigException if validation fails
     */
    private void validateSingleConfig(String configKey, String oldValueStr, String newValueStr)
            throws ConfigException {
        // Get ConfigOption for type information
        ConfigOption<?> configOption = ConfigOptions.getConfigOption(configKey);

        // For configs with allowed prefixes (like "datalake."), skip ConfigOption validation
        // and rely on ServerReconfigurable's business validation
        boolean hasPrefixConfig = false;
        for (String prefix : ALLOWED_CONFIG_PREFIXES) {
            if (configKey.startsWith(prefix)) {
                hasPrefixConfig = true;
                break;
            }
        }

        if (configOption == null && !hasPrefixConfig) {
            throw new ConfigException(
                    String.format("No ConfigOption found for config key: %s", configKey));
        }

        // Parse and validate type only if ConfigOption exists
        Object newValue = null;
        if (configOption != null && newValueStr != null) {
            Configuration tempConfig = new Configuration();
            tempConfig.setString(configKey, newValueStr);
            try {
                newValue = tempConfig.getOptional(configOption).get();
            } catch (Exception e) {
                String causeMessage =
                        e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
                throw new ConfigException(
                        String.format(
                                "Cannot parse '%s' as %s for config '%s': %s",
                                newValueStr,
                                configOption.isList()
                                        ? "List<" + configOption.getClazz().getSimpleName() + ">"
                                        : configOption.getClazz().getSimpleName(),
                                configKey,
                                causeMessage),
                        e);
            }
        }

        // Business validation with registered validators (if any)
        List<ConfigValidator<?>> validators = configValidatorsByKey.get(configKey);
        if (validators != null && !validators.isEmpty() && configOption != null) {
            Object oldValue =
                    oldValueStr != null
                            ? currentConfig.getOptional(configOption).orElse(null)
                            : null;
            for (ConfigValidator<?> validator : validators) {
                invokeValidator(validator, oldValue, newValue);
            }
        }
    }

    /**
     * Applies new configuration to all ServerReconfigurable instances with rollback support.
     *
     * @param newConfig new configuration to apply
     * @param skipErrorConfig whether to skip errors
     * @throws Exception if apply fails and skipErrorConfig is false
     */
    private void applyToServerReconfigurables(Configuration newConfig, boolean skipErrorConfig)
            throws Exception {
        Configuration oldConfig = currentConfig;
        Set<ServerReconfigurable> appliedSet = new HashSet<>();

        // Validate all first
        for (ServerReconfigurable reconfigurable : serverReconfigures.values()) {
            try {
                reconfigurable.validate(newConfig);
            } catch (ConfigException e) {
                LOG.error(
                        "Validation failed for {}: {}",
                        reconfigurable.getClass().getSimpleName(),
                        e.getMessage(),
                        e);
                if (!skipErrorConfig) {
                    throw e;
                }
            }
        }

        // Apply to all instances
        Exception throwable = null;
        for (ServerReconfigurable reconfigurable : serverReconfigures.values()) {
            try {
                reconfigurable.reconfigure(newConfig);
                appliedSet.add(reconfigurable);
            } catch (ConfigException e) {
                LOG.error(
                        "Reconfiguration failed for {}: {}",
                        reconfigurable.getClass().getSimpleName(),
                        e.getMessage(),
                        e);
                if (!skipErrorConfig) {
                    throwable = e;
                    break;
                }
            }
        }

        // Rollback if there was an error
        if (throwable != null) {
            appliedSet.forEach(r -> r.reconfigure(oldConfig));
            throw throwable;
        }
    }

    /**
     * Invokes validator with strongly-typed values.
     *
     * @param validator the config validator to invoke
     * @param oldValue the old typed value
     * @param newValue the new typed value
     * @throws ConfigException if validation fails
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void invokeValidator(ConfigValidator<?> validator, Object oldValue, Object newValue)
            throws ConfigException {
        // Invoke validator with typed values
        // We suppress unchecked warning because we trust that the validator
        // is registered for the correct type matching the ConfigOption
        ((ConfigValidator) validator).validate(oldValue, newValue);
    }
}
