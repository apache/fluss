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

package org.apache.fluss.plugin;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.shaded.guava32.com.google.common.collect.Iterables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.apache.fluss.config.ConfigOptions.PLUGIN_ALWAYS_PARENT_FIRST_LOADER_PATTERNS;
import static org.apache.fluss.config.ConfigOptions.PLUGIN_ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL;

/** Stores the configuration for plugins mechanism. */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class PluginConfig {

    /** The environment variable name which contains the location of the plugins folder. */
    private static final String ENV_FLUSS_PLUGINS_DIR = "FLUSS_PLUGINS_DIR";

    /**
     * The default FLUSS plugins directory if none has been specified via {@link
     * #ENV_FLUSS_PLUGINS_DIR}.
     */
    private static final String DEFAULT_FLUSS_PLUGINS_DIRS = "plugins";

    private static final Logger LOG = LoggerFactory.getLogger(PluginConfig.class);

    private final Optional<Path> pluginsPath;

    private final String[] alwaysParentFirstPatterns;

    private PluginConfig(Optional<Path> pluginsPath, String[] alwaysParentFirstPatterns) {
        this.pluginsPath = pluginsPath;
        this.alwaysParentFirstPatterns = alwaysParentFirstPatterns;
    }

    public Optional<Path> getPluginsPath() {
        return pluginsPath;
    }

    public String[] getAlwaysParentFirstPatterns() {
        return alwaysParentFirstPatterns;
    }

    public static PluginConfig fromConfiguration(Configuration configuration) {
        return new PluginConfig(
                getPluginsDir().map(File::toPath),
                getPluginParentFirstLoaderPatterns(configuration));
    }

    private static String[] getPluginParentFirstLoaderPatterns(Configuration config) {
        List<String> base = config.get(PLUGIN_ALWAYS_PARENT_FIRST_LOADER_PATTERNS);
        List<String> append = config.get(PLUGIN_ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL);
        return Iterables.toArray(Iterables.concat(base, append), String.class);
    }

    public static Optional<File> getPluginsDir() {
        String pluginsDir =
                System.getenv().getOrDefault(ENV_FLUSS_PLUGINS_DIR, DEFAULT_FLUSS_PLUGINS_DIRS);

        File pluginsDirFile = new File(pluginsDir);
        if (!pluginsDirFile.isDirectory()) {
            LOG.warn("The plugins directory [{}] does not exist.", pluginsDirFile);
            return Optional.empty();
        }
        return Optional.of(pluginsDirFile);
    }
}
