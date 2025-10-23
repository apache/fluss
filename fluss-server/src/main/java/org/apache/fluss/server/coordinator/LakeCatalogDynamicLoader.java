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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.cluster.ServerReconfigurable;
import org.apache.fluss.exception.ConfigException;
import org.apache.fluss.lake.lakestorage.LakeCatalog;
import org.apache.fluss.lake.lakestorage.LakeStorage;
import org.apache.fluss.lake.lakestorage.LakeStoragePlugin;
import org.apache.fluss.lake.lakestorage.LakeStoragePluginSetUp;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.plugin.PluginManager;
import org.apache.fluss.server.utils.LakeStorageUtils;
import org.apache.fluss.utils.IOUtils;

import javax.annotation.Nullable;

import java.util.Map;

import static org.apache.fluss.config.ConfigOptions.DATALAKE_FORMAT;
import static org.apache.fluss.server.utils.LakeStorageUtils.extractLakeProperties;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * A dynamic loader for lake catalog. Each time when the datalake format is changed, the lake
 * catalog will be changed.
 */
public class LakeCatalogDynamicLoader implements ServerReconfigurable, AutoCloseable {
    private volatile LakeCatalogContainer lakeCatalogContainer;
    private Configuration currentConfiguration;
    private final PluginManager pluginManager;
    private final boolean isCoordinator;

    public LakeCatalogDynamicLoader(
            Configuration configuration, PluginManager pluginManager, boolean isCoordinator) {
        this.isCoordinator = isCoordinator;
        this.currentConfiguration = configuration;
        this.lakeCatalogContainer =
                new LakeCatalogContainer(configuration, pluginManager, isCoordinator);
        this.pluginManager = pluginManager;
    }

    @Override
    public void validate(Configuration newConfig) throws ConfigException {
        final DataLakeFormat newDatalakeFormat =
                newConfig.getOptional(DATALAKE_FORMAT).isPresent()
                        ? newConfig.get(DATALAKE_FORMAT)
                        : currentConfiguration.get(DATALAKE_FORMAT);
        Map<String, String> configMap = newConfig.toMap();
        String datalakePrefix = "datalake." + newDatalakeFormat + ".";
        configMap.forEach(
                (key, value) -> {
                    if (!key.equals(DATALAKE_FORMAT.key())
                            && key.startsWith("datalake.")
                            && !key.startsWith(datalakePrefix)) {
                        throw new ConfigException(
                                String.format(
                                        "Invalid configuration '%s' for '%s' datalake format",
                                        key, newDatalakeFormat));
                    }
                });
    }

    @Override
    public void reconfigure(Configuration newConfig) throws ConfigException {
        LakeCatalogContainer lastLakeCatalogContainer = lakeCatalogContainer;
        DataLakeFormat newLakeFormat = newConfig.getOptional(DATALAKE_FORMAT).orElse(null);
        if (newLakeFormat != lastLakeCatalogContainer.dataLakeFormat) {
            IOUtils.closeQuietly(
                    lastLakeCatalogContainer.lakeCatalog,
                    "Close lake catalog because config changes");
            this.lakeCatalogContainer =
                    new LakeCatalogContainer(newConfig, pluginManager, isCoordinator);
            this.currentConfiguration = newConfig;
        }
    }

    public LakeCatalogContainer getLakeCatalogContainer() {
        return lakeCatalogContainer;
    }

    @Override
    public void close() throws Exception {
        LakeCatalogContainer closedCatalogContainer = lakeCatalogContainer;
        if (closedCatalogContainer != null && closedCatalogContainer.lakeCatalog != null) {
            IOUtils.closeQuietly(closedCatalogContainer.lakeCatalog, "Close lake catalog.");
        }
    }

    @Nullable
    private static LakeCatalog createLakeCatalog(Configuration conf, PluginManager pluginManager) {
        DataLakeFormat dataLakeFormat = conf.get(ConfigOptions.DATALAKE_FORMAT);
        if (dataLakeFormat == null) {
            return null;
        }
        LakeStoragePlugin lakeStoragePlugin =
                LakeStoragePluginSetUp.fromDataLakeFormat(dataLakeFormat.toString(), pluginManager);
        Map<String, String> lakeProperties = extractLakeProperties(conf);
        LakeStorage lakeStorage =
                lakeStoragePlugin.createLakeStorage(
                        Configuration.fromMap(checkNotNull(lakeProperties)));
        return lakeStorage.createLakeCatalog();
    }

    /** A container for lake catalog. */
    public static class LakeCatalogContainer {
        // null if the cluster hasn't configured datalake format
        private final @Nullable DataLakeFormat dataLakeFormat;
        private final @Nullable LakeCatalog lakeCatalog;
        private final @Nullable Map<String, String> defaultTableLakeOptions;

        public LakeCatalogContainer(
                Configuration configuration,
                @Nullable PluginManager pluginManager,
                boolean isCoordinator) {
            this.dataLakeFormat = configuration.getOptional(DATALAKE_FORMAT).orElse(null);
            this.lakeCatalog =
                    isCoordinator ? createLakeCatalog(configuration, pluginManager) : null;
            this.defaultTableLakeOptions =
                    LakeStorageUtils.generateDefaultTableLakeOptions(configuration);
            if (isCoordinator && ((dataLakeFormat == null) != (lakeCatalog == null))) {
                throw new ConfigException(
                        String.format(
                                "dataLakeFormat and lakeCatalog must both be null or both non-null, but dataLakeFormat is %s, lakeCatalog is %s.",
                                dataLakeFormat, lakeCatalog));
            }
        }

        @Nullable
        public DataLakeFormat getDataLakeFormat() {
            return dataLakeFormat;
        }

        @Nullable
        public LakeCatalog getLakeCatalog() {
            return lakeCatalog;
        }

        @Nullable
        public Map<String, String> getDefaultTableLakeOptions() {
            return defaultTableLakeOptions;
        }
    }
}
