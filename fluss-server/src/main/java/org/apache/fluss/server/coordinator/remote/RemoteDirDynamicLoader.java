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

package org.apache.fluss.server.coordinator.remote;

import org.apache.fluss.config.ConfigOption;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.cluster.ServerReconfigurable;
import org.apache.fluss.exception.ConfigException;
import org.apache.fluss.exception.IllegalConfigurationException;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.apache.fluss.config.FlussConfigUtils.validateRemoteDataDirs;

/**
 * Dynamic loader for remote data directories that supports runtime reconfiguration.
 *
 * <p>This class manages the lifecycle of remote data directories and provides a selector for
 * selecting remote data directories. It implements {@link ServerReconfigurable} to support dynamic
 * configuration updates at runtime without requiring a server restart.
 *
 * <p>When creating a new table or partition, the coordinator server uses this loader to select an
 * appropriate remote data directory based on the configured selection strategy (see {@link
 * org.apache.fluss.config.ConfigOptions#REMOTE_DATA_DIRS_STRATEGY}).
 */
public class RemoteDirDynamicLoader implements ServerReconfigurable, AutoCloseable {

    private volatile RemoteDirSelector remoteDirSelector;
    private Configuration currentConfiguration;

    public RemoteDirDynamicLoader(Configuration configuration) {
        this.currentConfiguration = configuration;
        this.remoteDirSelector = createRemoteDirSelector(configuration);
    }

    public RemoteDirSelector getRemoteDirSelector() {
        return remoteDirSelector;
    }

    @Override
    public void validate(Configuration newConfig) throws ConfigException {
        // Validate new remote data dirs contain all old remote data dirs
        Optional<List<String>> newRemoteDataDirsOp =
                newConfig.getOptional(ConfigOptions.REMOTE_DATA_DIRS);
        if (newRemoteDataDirsOp.isPresent()) {
            List<String> oldRemoteDataDirs =
                    currentConfiguration.get(ConfigOptions.REMOTE_DATA_DIRS);
            Set<String> newRemoteDataDirs = new HashSet<>(newRemoteDataDirsOp.get());
            if (!newRemoteDataDirs.containsAll(oldRemoteDataDirs)) {
                throw new ConfigException(
                        String.format(
                                "New %s: %s must contain all old %s: %s. "
                                        + "If you want the Fluss cluster to stop transferring data to a certain path, "
                                        + "keep it in %s and set its weight to 0 in %s.",
                                ConfigOptions.REMOTE_DATA_DIRS.key(),
                                newRemoteDataDirsOp.get(),
                                ConfigOptions.REMOTE_DATA_DIRS.key(),
                                oldRemoteDataDirs,
                                ConfigOptions.REMOTE_DATA_DIRS.key(),
                                ConfigOptions.REMOTE_DATA_DIRS_WEIGHTS.key()));
            }
        }

        Configuration mergedConfig = mergeConfigurations(currentConfiguration, newConfig);
        try {
            validateRemoteDataDirs(mergedConfig);
        } catch (IllegalConfigurationException e) {
            throw new ConfigException(e.getMessage());
        }
    }

    @Override
    public void reconfigure(Configuration newConfig) throws ConfigException {
        if (strategyChanged(newConfig)
                || remoteDataDirsChanged(newConfig)
                || weightsChanged(newConfig)) {
            // Create a new container with the merged configuration
            Configuration mergedConfig = mergeConfigurations(currentConfiguration, newConfig);
            this.remoteDirSelector = createRemoteDirSelector(mergedConfig);
            this.currentConfiguration = mergedConfig;
        }
    }

    private RemoteDirSelector createRemoteDirSelector(Configuration conf) {
        ConfigOptions.RemoteDataDirStrategy strategy =
                conf.get(ConfigOptions.REMOTE_DATA_DIRS_STRATEGY);
        String remoteDataDir = conf.get(ConfigOptions.REMOTE_DATA_DIR);
        List<String> remoteDataDirs = conf.get(ConfigOptions.REMOTE_DATA_DIRS);
        List<Integer> weights = conf.get(ConfigOptions.REMOTE_DATA_DIRS_WEIGHTS);

        switch (strategy) {
            case ROUND_ROBIN:
                return new RoundRobinRemoteDirSelector(remoteDataDir, remoteDataDirs);
            case WEIGHTED_ROUND_ROBIN:
                return new WeightedRoundRobinRemoteDirSelector(
                        remoteDataDir, remoteDataDirs, weights);
            default:
                throw new IllegalArgumentException(
                        "Unsupported remote data directory select strategy: " + strategy);
        }
    }

    private boolean strategyChanged(Configuration newConfig) {
        return hasConfigChanged(newConfig, ConfigOptions.REMOTE_DATA_DIRS_STRATEGY);
    }

    private boolean remoteDataDirsChanged(Configuration newConfig) {
        return hasConfigChanged(newConfig, ConfigOptions.REMOTE_DATA_DIRS);
    }

    private boolean weightsChanged(Configuration newConfig) {
        return hasConfigChanged(newConfig, ConfigOptions.REMOTE_DATA_DIRS_WEIGHTS);
    }

    /**
     * Checks if a specific configuration option has changed in the new config.
     *
     * @param newConfig the new configuration
     * @param option the configuration option to check
     * @param <T> the type of the configuration value
     * @return true if the configuration has changed
     */
    private <T> boolean hasConfigChanged(Configuration newConfig, ConfigOption<T> option) {
        return newConfig
                .getOptional(option)
                .map(newValue -> !Objects.equals(newValue, currentConfiguration.get(option)))
                .orElse(false);
    }

    /**
     * Merges the current configuration with new configuration values.
     *
     * @param current the current configuration
     * @param updates the configuration updates to apply
     * @return a new merged configuration
     */
    private Configuration mergeConfigurations(Configuration current, Configuration updates) {
        Configuration merged = new Configuration(current);
        updates.toMap().forEach(merged::setString);
        return merged;
    }

    @Override
    public void close() throws Exception {
        // do nothing
    }
}
