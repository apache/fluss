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

package com.alibaba.fluss.security.auth.sasl.jaas;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * Configuration and execution context required for JAAS authentication.
 *
 * <p>This class holds the JAAS configuration entries, context name, and related settings used
 * during the authentication process.
 */
public class JaasContext {
    private static final Logger LOG = LoggerFactory.getLogger(JaasContext.class);
    public static final String JAVA_LOGIN_CONFIG_PARAM = "java.security.auth.login.config";
    public static final String SASL_JAAS_CONFIG = "jaas.config";

    /**
     * The type of the SASL jaas context, it should be SERVER for the broker and CLIENT for the
     * clients (consumer, producer, etc.). This is used to validate behaviour (e.g. some
     * functionality is only available in the broker or clients).
     */
    public enum Type {
        CLIENT,
        SERVER
    }

    private final String name;
    private final Type type;
    private final Configuration configuration;
    private final List<AppConfigurationEntry> configurationEntries;
    private final @Nullable String dynamicJaasConfig;

    public JaasContext(
            String name,
            Type type,
            Configuration configuration,
            @Nullable String dynamicJaasConfig) {
        this.name = name;
        this.type = type;
        this.configuration = configuration;
        AppConfigurationEntry[] entries = configuration.getAppConfigurationEntry(name);
        if (entries == null) {
            throw new IllegalArgumentException(
                    "Could not find a '" + name + "' entry in this JAAS configuration.");
        }
        this.configurationEntries =
                Collections.unmodifiableList(new ArrayList<>(Arrays.asList(entries)));
        this.dynamicJaasConfig = dynamicJaasConfig;
    }

    public String name() {
        return name;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public List<AppConfigurationEntry> configurationEntries() {
        return configurationEntries;
    }

    public @Nullable String dynamicJaasConfig() {
        return dynamicJaasConfig;
    }

    // -------  static constructor------
    private static final String GLOBAL_CONTEXT_NAME_SERVER = "FlussServer";
    private static final String GLOBAL_CONTEXT_NAME_CLIENT = "FlussClient";

    /**
     * Returns a server-side {@link JaasContext} instance configured for a specific listener.
     *
     * <p>The JAAS configuration used by this context is determined in the following order:
     *
     * <ol>
     *   <li>Dynamic JAAS configuration provided via the `dynamicJaasConfig` parameter (inline
     *       string format).
     *   <li>Per-listener entry from the system-wide JAAS configuration, with context name: '{@code
     *       <listenerName>.toLowerCase(Locale.ROOT)}.FlussServer'
     *   <li>Fallback to the global server context '{@value #GLOBAL_CONTEXT_NAME_SERVER}' from the
     *       system-wide configuration.
     * </ol>
     *
     * <p>This allows fine-grained control of JAAS settings per listener, enabling different
     * authentication configurations for different endpoints, while still supporting a default
     * configuration.
     *
     * @param listenerName The name of the listener (e.g., "client") used for lookup in JAAS config.
     * @param dynamicJaasConfig Optional inline JAAS configuration string. If provided, it overrides
     *     any system-level config.
     * @return A constructed {@link JaasContext} instance for server authentication
     * @throws IllegalArgumentException if {@code listenerName} is null
     */
    public static JaasContext loadServerContext(
            String listenerName, @Nullable String dynamicJaasConfig) {
        if (listenerName == null) {
            throw new IllegalArgumentException("listenerName should not be null for SERVER");
        }

        String listenerContextName =
                listenerName.toLowerCase(Locale.ROOT) + "." + GLOBAL_CONTEXT_NAME_SERVER;
        return load(
                Type.SERVER, listenerContextName, GLOBAL_CONTEXT_NAME_SERVER, dynamicJaasConfig);
    }

    /**
     * Returns a client-side {@link JaasContext} instance.
     *
     * <p>If the JAAS configuration string is provided (e.g. via configuration property such as
     * `client.security.sasl.jaas.config`), a new {@link Configuration} will be created by parsing
     * that value. This allows specifying JAAS login modules and their options inline, without
     * relying on external files.
     *
     * <p>If no dynamic JAAS configuration is provided, this method falls back to the system-wide
     * JAAS configuration loaded from:
     *
     * <ul>
     *   <li>The JVM option `-Djava.security.auth.login.config`, or
     *   <li>The default JAAS configuration file (`jaas.conf`) if the JVM option is not set.
     * </ul>
     *
     * <p>The context name used for lookup in the JAAS configuration is always '{@value
     * #GLOBAL_CONTEXT_NAME_CLIENT}'.
     *
     * @param dynamicJaasConfig A string containing the inline JAAS configuration (optional)
     * @return A constructed {@link JaasContext} instance for client authentication
     * @throws IllegalArgumentException if the provided dynamic JAAS config is invalid or contains
     *     unexpected number of login modules
     */
    public static JaasContext loadClientContext(@Nullable String dynamicJaasConfig) {
        return load(Type.CLIENT, null, GLOBAL_CONTEXT_NAME_CLIENT, dynamicJaasConfig);
    }

    static JaasContext load(
            Type contextType,
            String listenerContextName,
            String globalContextName,
            @Nullable String dynamicJaasConfig) {
        if (dynamicJaasConfig != null) {
            JaasConfig jaasConfig = new JaasConfig(globalContextName, dynamicJaasConfig);
            AppConfigurationEntry[] contextModules =
                    jaasConfig.getAppConfigurationEntry(globalContextName);
            if (contextModules == null || contextModules.length == 0) {
                throw new IllegalArgumentException(
                        "JAAS config property does not contain any jaas modules");
            } else if (contextModules.length != 1) {
                throw new IllegalArgumentException(
                        "JAAS config property contains "
                                + contextModules.length
                                + " jaas modules, should be 1 module");
            }

            return new JaasContext(globalContextName, contextType, jaasConfig, dynamicJaasConfig);
        } else {
            return defaultContext(contextType, listenerContextName, globalContextName);
        }
    }

    /**
     * Creates a default {@link JaasContext} using the system-wide JAAS configuration.
     *
     * <p>The configuration is loaded in the following order:
     *
     * <ol>
     *   <li>Configuration entry for the given {@code listenerContextName}, if specified.
     *   <li>Fallback to the global context name {@code globalContextName} (e.g., "FlussServer").
     * </ol>
     *
     * <p>If no configuration file is specified via the JVM option {@code
     * java.security.auth.jaas.config}, the system's default JAAS configuration (typically
     * `jaas.conf`) will be used.
     */
    private static JaasContext defaultContext(
            Type contextType, String listenerContextName, String globalContextName) {
        String jaasConfigFile = System.getProperty(JAVA_LOGIN_CONFIG_PARAM);
        if (jaasConfigFile == null) {
            if (contextType == Type.CLIENT) {
                LOG.debug(
                        "System property '"
                                + JAVA_LOGIN_CONFIG_PARAM
                                + "' and Fluss SASL property '"
                                + SASL_JAAS_CONFIG
                                + "' are not set, using default JAAS configuration.");
            } else {
                LOG.debug(
                        "System property '"
                                + JAVA_LOGIN_CONFIG_PARAM
                                + "' is not set, using default JAAS "
                                + "configuration.");
            }
        }

        Configuration jaasConfig = Configuration.getConfiguration();

        AppConfigurationEntry[] configEntries = null;
        String contextName = globalContextName;

        if (listenerContextName != null) {
            configEntries = jaasConfig.getAppConfigurationEntry(listenerContextName);
            if (configEntries != null) {
                contextName = listenerContextName;
            }
        }

        if (configEntries == null) {
            configEntries = jaasConfig.getAppConfigurationEntry(globalContextName);
        }

        if (configEntries == null) {
            String listenerNameText =
                    listenerContextName == null ? "" : " or '" + listenerContextName + "'";
            String errorMessage =
                    "Could not find a '"
                            + globalContextName
                            + "'"
                            + listenerNameText
                            + " entry in the JAAS "
                            + "configuration. System property '"
                            + JAVA_LOGIN_CONFIG_PARAM
                            + "' is "
                            + (jaasConfigFile == null ? "not set" : jaasConfigFile);
            throw new IllegalArgumentException(errorMessage);
        }

        return new JaasContext(contextName, contextType, jaasConfig, null);
    }

    /**
     * Returns the configuration option for <code>key</code> from this context. If jaas module name
     * is specified, return option value only from that module.
     */
    public static String configEntryOption(
            List<AppConfigurationEntry> configurationEntries, String key, String loginModuleName) {
        for (AppConfigurationEntry entry : configurationEntries) {
            if (loginModuleName != null && !loginModuleName.equals(entry.getLoginModuleName())) {
                continue;
            }
            Object val = entry.getOptions().get(key);
            if (val != null) {
                return (String) val;
            }
        }
        return null;
    }
}
