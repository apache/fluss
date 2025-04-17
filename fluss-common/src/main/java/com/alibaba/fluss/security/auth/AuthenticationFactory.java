/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.security.auth;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.ValidationException;
import com.alibaba.fluss.plugin.PluginManager;
import com.alibaba.fluss.plugin.PluginUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A manager responsible for loading and configuring client and server authenticators based on the
 * provided configuration.
 *
 * <p>This class discovers authentication plugins via the classpath and configured plugins, and
 * creates suppliers for authenticators using plugin-specific configurations.
 *
 * <p>Key functionalities include:
 *
 * <ul>
 *   <li>Loading client authenticators based on {@link ConfigOptions#CLIENT_SECURITY_PROTOCOL}.
 *   <li>Loading server authenticators for multiple endpoints, using listener-specific protocols
 *       defined in {@link ConfigOptions#SERVER_SECURITY_PROTOCOL_MAP}.
 *   <li>Discovering plugins through {@link ServiceLoader} and custom {@link PluginManager} for
 *       extensibility.
 * </ul>
 *
 * @since 0.7
 */
public class AuthenticationFactory {
    private static final String SERVER_AUTHENTICATOR_PREFIX = "security.";

    /**
     * Loads a supplier for a client authenticator based on the configuration.
     *
     * @param configuration The configuration containing authentication settings and protocol
     *     definitions.
     * @return A supplier for creating the client authenticator.
     */
    public static Supplier<ClientAuthenticator> loadClientAuthenticatorSupplier(
            Configuration configuration) {
        String clientAuthenticateProtocol =
                configuration.getString(ConfigOptions.CLIENT_SECURITY_PROTOCOL);
        ClientAuthenticationPlugin authenticatorPlugin =
                discoverPlugin(clientAuthenticateProtocol, ClientAuthenticationPlugin.class, null);
        return () -> authenticatorPlugin.createClientAuthenticator(configuration);
    }

    /**
     * Loads suppliers for server authenticators for each endpoint, based on listener-specific
     * protocols.
     *
     * @param configuration The configuration containing authentication settings and protocol
     *     definitions.
     * @return A map mapping listener names to suppliers for their corresponding server
     *     authenticators.
     */
    public static Map<String, Supplier<ServerAuthenticator>> loadServerAuthenticatorSuppliers(
            Configuration configuration) {
        PluginManager pluginManager = PluginUtils.createPluginManagerFromRootFolder(configuration);
        Map<String, Supplier<ServerAuthenticator>> serverAuthenticators = new HashMap<>();
        Map<String, String> protocolMap =
                configuration.getMap(ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP);
        for (Map.Entry<String, String> protocolEntry : protocolMap.entrySet()) {
            String serverAuthenticateProtocol = protocolEntry.getValue();
            ServerAuthenticationPlugin serverAuthenticatorPlugin =
                    discoverPlugin(
                            serverAuthenticateProtocol,
                            ServerAuthenticationPlugin.class,
                            pluginManager);

            serverAuthenticators.put(
                    protocolEntry.getKey(),
                    () -> serverAuthenticatorPlugin.createServerAuthenticator(configuration));
        }
        return serverAuthenticators;
    }

    /**
     * Discovers an authentication plugin of the specified type and protocol from the classpath and
     * configured plugins.
     *
     * @param protocol The protocol name (e.g., "PLAINTEXT", "SASL_PLAIN") to match the plugin's
     *     {@link AuthenticationPlugin#authProtocol()}.
     * @return The discovered plugin instance.
     * @throws ValidationException If no plugin or multiple plugins match the given protocol and
     *     interface.
     */
    @SuppressWarnings("unchecked")
    private static <T extends AuthenticationPlugin> T discoverPlugin(
            String protocol, Class<T> pluginInterface, @Nullable PluginManager pluginManager) {

        Collection<Supplier<Iterator<AuthenticationPlugin>>> pluginSuppliers = new ArrayList<>(2);
        pluginSuppliers.add(() -> ServiceLoader.load(AuthenticationPlugin.class).iterator());
        if (pluginManager != null) {
            pluginSuppliers.add(() -> pluginManager.load(AuthenticationPlugin.class));
        }

        List<T> matchingPlugins = new ArrayList<>();
        for (Supplier<Iterator<AuthenticationPlugin>> pluginIteratorsSupplier : pluginSuppliers) {
            final Iterator<AuthenticationPlugin> foundPlugins = pluginIteratorsSupplier.get();
            while (foundPlugins.hasNext()) {
                AuthenticationPlugin plugin = foundPlugins.next();
                if (plugin.authProtocol().equalsIgnoreCase(protocol)
                        && pluginInterface.isAssignableFrom(plugin.getClass())) {
                    matchingPlugins.add((T) plugin);
                }
            }
        }
        if (matchingPlugins.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "No plugin for the protocol '%s' is found in the classpath.",
                            protocol));
        }

        if (matchingPlugins.size() > 1) {
            throw new ValidationException(
                    String.format(
                            "Multiple plugins for the same protocol '%s' are found in the classpath.\n\n"
                                    + "Available plugins are:\n\n"
                                    + "%s",
                            protocol,
                            matchingPlugins.stream()
                                    .map(f -> f.getClass().getName())
                                    .distinct()
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }

        return matchingPlugins.get(0);
    }
}
