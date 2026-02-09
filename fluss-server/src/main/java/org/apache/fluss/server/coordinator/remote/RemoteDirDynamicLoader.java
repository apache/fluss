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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.ConfigOption;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.cluster.ServerReconfigurable;
import org.apache.fluss.exception.ConfigException;
import org.apache.fluss.fs.FsPath;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Dynamic loader for remote data directories that supports runtime reconfiguration.
 *
 * <p>This class manages the lifecycle of remote data directories and provides a container for
 * selecting remote data directories. It implements {@link ServerReconfigurable} to support dynamic
 * configuration updates at runtime without requiring a server restart.
 *
 * <p>The remote data directories are used for storing tiered storage data, including:
 *
 * <ul>
 *   <li>KV snapshot data files for primary key tables
 *   <li>Remote log segments for log tiered storage
 * </ul>
 *
 * <p>When creating a new table or partition, the coordinator server uses this loader to select an
 * appropriate remote data directory based on the configured selection strategy (see {@link
 * org.apache.fluss.config.ConfigOptions#REMOTE_DATA_DIRS_STRATEGY}).
 */
public class RemoteDirDynamicLoader implements ServerReconfigurable, AutoCloseable {

    private volatile RemoteDirContainer remoteDirContainer;
    private Configuration currentConfiguration;

    public RemoteDirDynamicLoader(Configuration configuration) {
        this.currentConfiguration = configuration;
        this.remoteDirContainer = new RemoteDirContainer(configuration);
    }

    /**
     * Gets a container for managing and selecting remote data directories.
     *
     * <p>The container encapsulates the remote data directories and the selector strategy used to
     * choose directories.
     *
     * @return a container for remote data directories
     */
    public RemoteDirContainer getRemoteDataDirContainer() {
        return remoteDirContainer;
    }

    @Override
    public void validate(Configuration newConfig) throws ConfigException {
        // Get and valid remote data dirs
        List<String> remoteDataDirs =
                newConfig
                        .getOptional(ConfigOptions.REMOTE_DATA_DIRS)
                        .orElseGet(() -> currentConfiguration.get(ConfigOptions.REMOTE_DATA_DIRS));
        for (int i = 0; i < remoteDataDirs.size(); i++) {
            String remoteDataDir = remoteDataDirs.get(i);
            try {
                new FsPath(remoteDataDir);
            } catch (Exception e) {
                throw new ConfigException(
                        String.format(
                                "Invalid remote path for %s at index %d.",
                                ConfigOptions.REMOTE_DATA_DIRS.key(), i));
            }
        }

        // Get the strategy from new config or fall back to current config
        ConfigOptions.RemoteDataDirStrategy strategy =
                newConfig
                        .getOptional(ConfigOptions.REMOTE_DATA_DIRS_STRATEGY)
                        .orElseGet(
                                () ->
                                        currentConfiguration.get(
                                                ConfigOptions.REMOTE_DATA_DIRS_STRATEGY));

        // Validate weighted round-robin strategy configuration
        if (strategy == ConfigOptions.RemoteDataDirStrategy.WEIGHTED_ROUND_ROBIN) {
            List<Integer> weights =
                    newConfig
                            .getOptional(ConfigOptions.REMOTE_DATA_DIRS_WEIGHTS)
                            .orElseGet(
                                    () ->
                                            currentConfiguration.get(
                                                    ConfigOptions.REMOTE_DATA_DIRS_WEIGHTS));

            if (!remoteDataDirs.isEmpty() && !weights.isEmpty()) {
                if (remoteDataDirs.size() != weights.size()) {
                    throw new ConfigException(
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
                        throw new ConfigException(
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

    @Override
    public void reconfigure(Configuration newConfig) throws ConfigException {
        // Check if any relevant configuration has changed
        boolean strategyChanged =
                hasConfigChanged(newConfig, ConfigOptions.REMOTE_DATA_DIRS_STRATEGY);
        boolean remoteDirsChanged = hasConfigChanged(newConfig, ConfigOptions.REMOTE_DATA_DIRS);
        boolean weightsChanged =
                hasConfigChanged(newConfig, ConfigOptions.REMOTE_DATA_DIRS_WEIGHTS);

        if (strategyChanged || remoteDirsChanged || weightsChanged) {
            // Create a new container with the merged configuration
            Configuration mergedConfig = mergeConfigurations(currentConfiguration, newConfig);
            this.remoteDirContainer = new RemoteDirContainer(mergedConfig);
            this.currentConfiguration = mergedConfig;
        }
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

    /** Container for managing remote data directories and selecting the next directory to use. */
    public static class RemoteDirContainer {

        private final FsPath defaultRemoteDataDir;
        private final RemoteDirSelector remoteDirSelector;

        public RemoteDirContainer(Configuration conf) {
            this.defaultRemoteDataDir = new FsPath(conf.get(ConfigOptions.REMOTE_DATA_DIR));
            this.remoteDirSelector =
                    createRemoteDirSelector(
                            conf.get(ConfigOptions.REMOTE_DATA_DIRS_STRATEGY),
                            new FsPath(conf.get(ConfigOptions.REMOTE_DATA_DIR)),
                            conf.get(ConfigOptions.REMOTE_DATA_DIRS).stream()
                                    .map(FsPath::new)
                                    .collect(Collectors.toList()),
                            conf.get(ConfigOptions.REMOTE_DATA_DIRS_WEIGHTS));
        }

        public FsPath getDefaultRemoteDataDir() {
            return defaultRemoteDataDir;
        }

        private RemoteDirSelector createRemoteDirSelector(
                ConfigOptions.RemoteDataDirStrategy strategy,
                FsPath defaultRemoteDataDir,
                List<FsPath> remoteDataDirs,
                List<Integer> weights) {
            switch (strategy) {
                case ROUND_ROBIN:
                    return new RoundRobinRemoteDirSelector(defaultRemoteDataDir, remoteDataDirs);
                case WEIGHTED_ROUND_ROBIN:
                    return new WeightedRoundRobinRemoteDirSelector(
                            defaultRemoteDataDir, remoteDataDirs, weights);
                default:
                    throw new IllegalArgumentException(
                            "Unsupported remote data directory select strategy: " + strategy);
            }
        }

        public FsPath nextDataDir() {
            return remoteDirSelector.nextDataDir();
        }

        @VisibleForTesting
        protected RemoteDirSelector getRemoteDirSelector() {
            return remoteDirSelector;
        }
    }
}
