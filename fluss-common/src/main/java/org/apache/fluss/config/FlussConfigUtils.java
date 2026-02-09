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
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.utils.FlussPaths;

import java.lang.reflect.Field;
import java.util.Arrays;
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
                Arrays.asList(
                        ConfigOptions.TABLE_DATALAKE_ENABLED.key(),
                        ConfigOptions.TABLE_DATALAKE_FRESHNESS.key(),
                        ConfigOptions.TABLE_TIERED_LOG_LOCAL_SEGMENTS.key());
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

        validMinValue(conf, ConfigOptions.DEFAULT_REPLICATION_FACTOR, 1);
        validMinValue(conf, ConfigOptions.KV_MAX_RETAINED_SNAPSHOTS, 1);
        validMinValue(conf, ConfigOptions.SERVER_IO_POOL_SIZE, 1);

        // Validate remote.data.dirs
        List<String> remoteDataDirs = conf.get(ConfigOptions.REMOTE_DATA_DIRS);
        for (int i = 0; i < remoteDataDirs.size(); i++) {
            String remoteDataDir = remoteDataDirs.get(i);
            try {
                new FsPath(remoteDataDir);
            } catch (Exception e) {
                throw new IllegalConfigurationException(
                        String.format(
                                "Invalid remote path for %s at index %d.",
                                ConfigOptions.REMOTE_DATA_DIRS.key(), i),
                        e);
            }
        }

        // Validate remote.data.dirs.strategy
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
                // Validate all weights are positive
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
        validMinValue(ConfigOptions.TABLET_SERVER_ID, serverId.get(), 0);

        validMinValue(conf, ConfigOptions.BACKGROUND_THREADS, 1);

        if (conf.get(ConfigOptions.LOG_SEGMENT_FILE_SIZE).getBytes() > Integer.MAX_VALUE) {
            throw new IllegalConfigurationException(
                    String.format(
                            "Invalid configuration for %s, it must be less than or equal %d bytes.",
                            ConfigOptions.LOG_SEGMENT_FILE_SIZE.key(), Integer.MAX_VALUE));
        }
    }

    /** Validate common server configs. */
    private static void validServerConfigs(Configuration conf) {
        if (conf.get(ConfigOptions.REMOTE_DATA_DIR) == null) {
            throw new IllegalConfigurationException(
                    String.format("Configuration %s must be set.", ConfigOptions.REMOTE_DATA_DIR));
        } else {
            // Must validate that remote.data.dir is a valid FsPath
            try {
                FlussPaths.remoteDataDir(conf);
            } catch (Exception e) {
                throw new IllegalConfigurationException(
                        String.format(
                                "Invalid configuration for %s.",
                                ConfigOptions.REMOTE_DATA_DIR.key()),
                        e);
            }
        }
    }

    private static void validMinValue(
            Configuration conf, ConfigOption<Integer> option, int minValue) {
        validMinValue(option, conf.get(option), minValue);
    }

    private static void validMinValue(ConfigOption<Integer> option, int value, int minValue) {
        if (value < minValue) {
            throw new IllegalConfigurationException(
                    String.format(
                            "Invalid configuration for %s, it must be greater than or equal %d.",
                            option.key(), minValue));
        }
    }
}
