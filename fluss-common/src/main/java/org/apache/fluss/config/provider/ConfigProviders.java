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

package org.apache.fluss.config.provider;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.IllegalConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;

/**
 * Resolves <code>${identifier:[path:]key}</code> markers in a {@link Configuration} through the
 * {@link ConfigProvider}s declared in {@link ConfigOptions#CONFIG_PROVIDERS}. A marker must be the
 * whole value; a literal leading <code>${</code> can be escaped as <code>$${</code>.
 */
@Internal
public final class ConfigProviders {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigProviders.class);

    private static final String PROVIDER_PREFIX = ConfigOptions.CONFIG_PROVIDERS.key() + ".";
    private static final String PARAM_INFIX = ".param.";

    private ConfigProviders() {}

    /**
     * Resolves all markers in {@code config} in place. A no-op when {@link
     * ConfigOptions#CONFIG_PROVIDERS} is absent.
     *
     * @throws IllegalConfigurationException if a marker is malformed, references an undeclared
     *     provider, or fails to resolve
     */
    public static void resolve(Configuration config) {
        List<String> declared = config.get(ConfigOptions.CONFIG_PROVIDERS);
        if (declared == null || declared.isEmpty()) {
            warnAboutUnresolvableMarkers(config);
            return;
        }

        Map<String, ConfigProvider> providers = loadProviders(config, declared);
        try {
            for (String key : config.keySet()) {
                Optional<Object> raw = config.getRawValue(key);
                if (!raw.isPresent() || !(raw.get() instanceof String)) {
                    continue;
                }
                String value = (String) raw.get();
                if (value.startsWith("$${")) {
                    // escaped: drop one '$', leave the rest literal
                    config.setString(key, value.substring(1));
                } else if (isMarker(value)) {
                    config.setString(key, resolveMarker(value, providers));
                    // hide the resolved secret even under a key name that doesn't look sensitive
                    config.markSensitive(key);
                }
            }
        } finally {
            closeAll(providers);
        }
    }

    private static String resolveMarker(String value, Map<String, ConfigProvider> providers) {
        String inner = value.substring(2, value.length() - 1);
        int firstColon = inner.indexOf(':');
        if (firstColon <= 0 || firstColon == inner.length() - 1) {
            throw new IllegalConfigurationException(
                    "Malformed config provider marker '"
                            + value
                            + "'. Expected ${identifier:[path:]key}.");
        }
        String identifier = inner.substring(0, firstColon);
        String rest = inner.substring(firstColon + 1);
        int lastColon = rest.lastIndexOf(':');
        String path = lastColon < 0 ? "" : rest.substring(0, lastColon);
        String key = lastColon < 0 ? rest : rest.substring(lastColon + 1);

        ConfigProvider provider = providers.get(identifier);
        if (provider == null) {
            throw new IllegalConfigurationException(
                    "Config provider '"
                            + identifier
                            + "' referenced by marker '"
                            + value
                            + "' is not declared in '"
                            + ConfigOptions.CONFIG_PROVIDERS.key()
                            + "'.");
        }
        try {
            return provider.resolve(path, key);
        } catch (Exception e) {
            throw new IllegalConfigurationException(
                    "Failed to resolve config provider marker '" + value + "': " + e.getMessage(),
                    e);
        }
    }

    /** Whether the value has the <code>${...}</code> marker shape. */
    public static boolean isMarker(String value) {
        return value.length() > 3 && value.startsWith("${") && value.endsWith("}");
    }

    private static void warnAboutUnresolvableMarkers(Configuration config) {
        for (String key : config.keySet()) {
            Optional<Object> raw = config.getRawValue(key);
            if (raw.isPresent() && raw.get() instanceof String && isMarker((String) raw.get())) {
                LOG.warn(
                        "Value of '{}' looks like a config provider marker, but '{}' is not set; "
                                + "it is kept as a literal.",
                        key,
                        ConfigOptions.CONFIG_PROVIDERS.key());
            }
        }
    }

    private static Map<String, ConfigProvider> loadProviders(
            Configuration config, List<String> declared) {
        Set<String> requested = new HashSet<>();
        for (String name : declared) {
            String trimmed = name.trim();
            if (!trimmed.isEmpty()) {
                requested.add(trimmed);
            }
        }

        Map<String, ConfigProvider> providers = new HashMap<>();
        try {
            ServiceLoader<ConfigProvider> loader =
                    ServiceLoader.load(ConfigProvider.class, ConfigProvider.class.getClassLoader());
            for (Iterator<ConfigProvider> it = loader.iterator(); it.hasNext(); ) {
                ConfigProvider provider = it.next();
                if (!requested.contains(provider.identifier())) {
                    continue;
                }
                ConfigProvider existing = providers.get(provider.identifier());
                if (existing != null) {
                    throw new IllegalConfigurationException(
                            "Multiple config providers with identifier '"
                                    + provider.identifier()
                                    + "' found on the classpath: "
                                    + existing.getClass().getName()
                                    + ", "
                                    + provider.getClass().getName()
                                    + ".");
                }
                try {
                    provider.configure(paramsFor(config, provider.identifier()));
                } catch (Exception e) {
                    throw new IllegalConfigurationException(
                            "Invalid configuration for config provider '"
                                    + provider.identifier()
                                    + "': "
                                    + e.getMessage(),
                            e);
                }
                providers.put(provider.identifier(), provider);
            }

            for (String name : requested) {
                if (!providers.containsKey(name)) {
                    throw new IllegalConfigurationException(
                            "No config provider with identifier '"
                                    + name
                                    + "' found on the classpath, but it is declared in '"
                                    + ConfigOptions.CONFIG_PROVIDERS.key()
                                    + "'.");
                }
            }
            return providers;
        } catch (Exception e) {
            closeAll(providers);
            throw e;
        }
    }

    private static void closeAll(Map<String, ConfigProvider> providers) {
        for (ConfigProvider provider : providers.values()) {
            try {
                provider.close();
            } catch (Exception e) {
                LOG.warn("Failed to close config provider '{}'.", provider.identifier(), e);
            }
        }
    }

    private static Map<String, String> paramsFor(Configuration config, String identifier) {
        String prefix = PROVIDER_PREFIX + identifier + PARAM_INFIX;
        Map<String, String> params = new HashMap<>();
        for (String key : config.keySet()) {
            if (key.startsWith(prefix)) {
                config.getRawValue(key)
                        .ifPresent(v -> params.put(key.substring(prefix.length()), v.toString()));
            }
        }
        return params;
    }
}
