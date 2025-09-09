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
import org.apache.fluss.config.dynamic.ServerReconfigurable;
import org.apache.fluss.exception.ConfigException;
import org.apache.fluss.lake.lakestorage.LakeCatalog;
import org.apache.fluss.lake.lakestorage.LakeStorage;
import org.apache.fluss.lake.lakestorage.LakeStoragePlugin;
import org.apache.fluss.lake.lakestorage.LakeStoragePluginSetUp;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.plugin.PluginManager;
import org.apache.fluss.server.DynamicServerConfig;
import org.apache.fluss.server.utils.LakeStorageUtils;
import org.apache.fluss.utils.IOUtils;

import javax.annotation.Nullable;

import java.util.Map;

import static org.apache.fluss.config.ConfigOptions.DATALAKE_FORMAT;
import static org.apache.fluss.server.utils.LakeStorageUtils.extractLakeProperties;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * A dynamic loader for lake catalog. Each time when the datalake format is changed, the lake
 * catalog will be changed.
 */
public class LakeCatalogDynamicLoader implements ServerReconfigurable, AutoCloseable {
    // null if the cluster hasn't configured datalake format
    private @Nullable DataLakeFormat dataLakeFormat;
    private @Nullable LakeCatalog lakeCatalog;
    private @Nullable Map<String, String> defaultTableLakeOptions;
    private Configuration currentConfiguration;
    private final PluginManager pluginManager;
    private final boolean isCoordinator;

    public LakeCatalogDynamicLoader(
            DynamicServerConfig dynamicServerConfig,
            PluginManager pluginManager,
            boolean isCoordinator) {
        Configuration currentConfig = dynamicServerConfig.getCurrentConfig();
        this.isCoordinator = isCoordinator;
        this.currentConfiguration = currentConfig;
        this.dataLakeFormat = currentConfig.getOptional(DATALAKE_FORMAT).orElse(null);
        this.lakeCatalog = createLakeCatalog(currentConfig, pluginManager);
        this.defaultTableLakeOptions =
                LakeStorageUtils.generateDefaultTableLakeOptions(currentConfig);
        this.pluginManager = pluginManager;
        checkState(
                (dataLakeFormat == null) == (lakeCatalog == null),
                "dataLakeFormat and lakeCatalog must both be null or both non-null, but dataLakeFormat is %s, lakeCatalog is %s.",
                dataLakeFormat,
                lakeCatalog);
        dynamicServerConfig.register(this);
    }

    @Override
    public void validate(Configuration newConfig) throws ConfigException {
        DataLakeFormat newDatalakeFormat = null;
        try {
            if (newConfig.getOptional(DATALAKE_FORMAT).isPresent()) {
                newDatalakeFormat = newConfig.get(DATALAKE_FORMAT);
            } else {
                newDatalakeFormat = currentConfiguration.get(DATALAKE_FORMAT);
            }

            if (newDatalakeFormat == null) {
                return;
            }
        } catch (Exception e) {
            throw new ConfigException(
                    "Invalid configuration for datalake format "
                            + newDatalakeFormat
                            + ": "
                            + e.getMessage());
        }

        Map<String, String> configMap = newConfig.toMap();
        String datalakePrefix = "datalake." + newDatalakeFormat + ".";
        DataLakeFormat finalNewDatalakeFormat = newDatalakeFormat;
        configMap.forEach(
                (key, value) -> {
                    if (!key.equals(DATALAKE_FORMAT.key())
                            && key.startsWith("datalake.")
                            && !key.startsWith(datalakePrefix)) {
                        throw new ConfigException(
                                "Invalid configuration for datalake format "
                                        + finalNewDatalakeFormat
                                        + ": "
                                        + key);
                    }
                });
    }

    @Override
    public void reconfigure(Configuration newConfig) throws ConfigException {
        DataLakeFormat newLakeFormat = newConfig.getOptional(DATALAKE_FORMAT).orElse(null);
        if (newLakeFormat != dataLakeFormat) {
            IOUtils.closeQuietly(lakeCatalog, "Lake catalog because config changes");
            this.dataLakeFormat = newLakeFormat;
            if (isCoordinator) {
                this.lakeCatalog = createLakeCatalog(newConfig, pluginManager);
            }
            this.defaultTableLakeOptions =
                    LakeStorageUtils.generateDefaultTableLakeOptions(newConfig);
            this.currentConfiguration = newConfig;
        }
    }

    @Nullable
    private LakeCatalog createLakeCatalog(Configuration conf, PluginManager pluginManager) {
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

    public @Nullable DataLakeFormat getDataLakeFormat() {
        return dataLakeFormat;
    }

    public @Nullable LakeCatalog getLakeCatalog() {
        return lakeCatalog;
    }

    public @Nullable Map<String, String> getDefaultLakeProperties() {
        return defaultTableLakeOptions;
    }

    @Override
    public void close() throws Exception {
        if (lakeCatalog != null) {
            lakeCatalog.close();
        }
    }
}
