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

package org.apache.fluss.config;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.exception.IllegalConfigurationException;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Utilities of Fluss {@link ConfigOptions}. */
@Internal
public class FlussConfigUtils {

    public static final Map<String, ConfigOption<?>> TABLE_OPTIONS;
    public static final Map<String, ConfigOption<?>> CLIENT_OPTIONS;
    public static final String TABLE_PREFIX = "table.";
    public static final String CLIENT_PREFIX = "client.";
    public static final String CLIENT_SECURITY_PREFIX = "client.security.";

    public static final List<String> ALTERABLE_TABLE_OPTIONS;

    static {
        TABLE_OPTIONS = extractConfigOptions("table.");
        CLIENT_OPTIONS = extractConfigOptions("client.");
        ALTERABLE_TABLE_OPTIONS =
                Collections.singletonList(ConfigOptions.TABLE_DATALAKE_ENABLED.key());
    }

    public static boolean isTableStorageConfig(String key) {
        return key.startsWith(TABLE_PREFIX);
    }

    public static boolean isAlterableTableOption(String key) {
        return ALTERABLE_TABLE_OPTIONS.contains(key);
    }

    @VisibleForTesting
    static Map<String, ConfigOption<?>> extractConfigOptions(String prefix) {
        Map<String, ConfigOption<?>> options = new HashMap<>();
        Field[] fields = ConfigOptions.class.getFields();
        // use Java reflection to collect all options matches the prefix
        for (Field field : fields) {
            if (!ConfigOption.class.isAssignableFrom(field.getType())) {
                continue;
            }
            try {
                ConfigOption<?> configOption = (ConfigOption<?>) field.get(null);
                if (configOption.key().startsWith(prefix)) {
                    options.put(configOption.key(), configOption);
                }
            } catch (IllegalAccessException e) {
                throw new RuntimeException(
                        "Unable to extract ConfigOption fields from ConfigOptions class.", e);
            }
        }
        return options;
    }

    public static void validateCoordinatorConfigs(Configuration conf) {
        validServerConfigs(conf);

        if (conf.get(ConfigOptions.DEFAULT_REPLICATION_FACTOR) < 1) {
            throw new IllegalConfigurationException(
                    String.format(
                            "Invalid configuration for %s, it must be greater than or equal 1.",
                            ConfigOptions.DEFAULT_REPLICATION_FACTOR.key()));
        }

        if (conf.get(ConfigOptions.KV_MAX_RETAINED_SNAPSHOTS) < 1) {
            throw new IllegalConfigurationException(
                    String.format(
                            "Invalid configuration for %s, it must be greater than or equal 1.",
                            ConfigOptions.KV_MAX_RETAINED_SNAPSHOTS.key()));
        }

        if (conf.get(ConfigOptions.SERVER_IO_POOL_SIZE) < 1) {
            throw new IllegalConfigurationException(
                    String.format(
                            "Invalid configuration for %s, it must be greater than or equal 1.",
                            ConfigOptions.SERVER_IO_POOL_SIZE.key()));
        }

        // validate remote.data.dirs
        List<String> remoteDataDirs = conf.get(ConfigOptions.REMOTE_DATA_DIRS);
        ConfigOptions.RemoteDataDirStrategy remoteDataDirStrategy =
                conf.get(ConfigOptions.REMOTE_DATA_DIRS_STRATEGY);
        if (remoteDataDirStrategy == ConfigOptions.RemoteDataDirStrategy.WEIGHTED_ROUND_ROBIN) {
            List<Integer> weights = conf.get(ConfigOptions.REMOTE_DATA_DIRS_WEIGHTS);
            if (!remoteDataDirs.isEmpty() && !weights.isEmpty()) {
                if (remoteDataDirs.size() != weights.size()) {
                    throw new IllegalConfigurationException(
                            String.format(
                                    "The size of '%s' (%d) must match the size of '%s' (%d) when using WEIGHTED_ROUND_ROBIN strategy.",
                                    ConfigOptions.REMOTE_DATA_DIRS.key(),
                                    remoteDataDirs.size(),
                                    ConfigOptions.REMOTE_DATA_DIRS_WEIGHTS.key(),
                                    weights.size()));
                }
                // validate all weights are positive
                for (int i = 0; i < weights.size(); i++) {
                    if (weights.get(i) < 0) {
                        throw new IllegalConfigurationException(
                                String.format(
                                        "All weights in '%s' must be no less than 0, but found %d at index %d.",
                                        ConfigOptions.REMOTE_DATA_DIRS_WEIGHTS.key(),
                                        weights.get(i),
                                        i));
                    }
                }
            }
        }
    }

    public static void validateTabletConfigs(Configuration conf) {
        validServerConfigs(conf);

        Optional<Integer> serverId = conf.getOptional(ConfigOptions.TABLET_SERVER_ID);
        if (!serverId.isPresent()) {
            throw new IllegalConfigurationException(
                    String.format("Configuration %s must be set.", ConfigOptions.TABLET_SERVER_ID));
        }

        if (serverId.get() < 0) {
            throw new IllegalConfigurationException(
                    String.format(
                            "Invalid configuration for %s, it must be greater than or equal 0.",
                            ConfigOptions.TABLET_SERVER_ID.key()));
        }

        if (conf.get(ConfigOptions.BACKGROUND_THREADS) < 1) {
            throw new IllegalConfigurationException(
                    String.format(
                            "Invalid configuration for %s, it must be greater than or equal 1.",
                            ConfigOptions.BACKGROUND_THREADS.key()));
        }

        if (conf.get(ConfigOptions.LOG_SEGMENT_FILE_SIZE).getBytes() > Integer.MAX_VALUE) {
            throw new IllegalConfigurationException(
                    String.format(
                            "Invalid configuration for %s, it must be less than or equal %d bytes.",
                            ConfigOptions.LOG_SEGMENT_FILE_SIZE.key(), Integer.MAX_VALUE));
        }
    }

    private static void validServerConfigs(Configuration conf) {
        if (conf.get(ConfigOptions.REMOTE_DATA_DIR) == null) {
            throw new IllegalConfigurationException(
                    String.format("Configuration %s must be set.", ConfigOptions.REMOTE_DATA_DIR));
        }
    }
}
