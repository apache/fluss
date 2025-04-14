/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.authorizer;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.ValidationException;
import com.alibaba.fluss.plugin.PluginManager;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** authorizer loader. */
public class AuthorizerLoader {

    /** Load authorizer. */
    public static @Nullable Authorizer createAuthorizer(
            Configuration configuration, @Nullable PluginManager pluginManager) {
        String authorizerType = configuration.get(ConfigOptions.AUTHORIZER_PLUGIN_TYPE);
        if (authorizerType == null) {
            return null;
        }

        Collection<Supplier<Iterator<AuthorizationPlugin>>> pluginSuppliers = new ArrayList<>(2);
        pluginSuppliers.add(() -> ServiceLoader.load(AuthorizationPlugin.class).iterator());

        if (pluginManager != null) {
            pluginSuppliers.add(() -> pluginManager.load(AuthorizationPlugin.class));
        }

        List<AuthorizationPlugin> matchingPlugins = new ArrayList<>();
        for (Supplier<Iterator<AuthorizationPlugin>> pluginSupplier : pluginSuppliers) {
            Iterator<AuthorizationPlugin> plugins = pluginSupplier.get();
            while (plugins.hasNext()) {
                AuthorizationPlugin plugin = plugins.next();
                if (plugin.identifier().equals(authorizerType)) {
                    matchingPlugins.add(plugin);
                }
            }
        }

        if (matchingPlugins.size() != 1) {
            throw new ValidationException(
                    String.format(
                            "Could not find same authorizer plugin for protocol '%s' in the classpath.\n\n"
                                    + "Available factory protocols are:\n\n"
                                    + "%s",
                            authorizerType,
                            matchingPlugins.stream()
                                    .map(f -> f.getClass().getName())
                                    .distinct()
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }

        return matchingPlugins.get(0).createAuthorizer(configuration);
    }
}
