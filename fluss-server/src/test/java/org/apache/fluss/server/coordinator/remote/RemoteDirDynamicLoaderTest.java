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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.ConfigException;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RemoteDirDynamicLoader}. */
class RemoteDirDynamicLoaderTest {

    @Test
    void testReconfigureWithStrategyChange() throws Exception {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.REMOTE_DATA_DIRS, Arrays.asList("hdfs://dir1", "hdfs://dir2"));
        try (RemoteDirDynamicLoader loader = new RemoteDirDynamicLoader(conf)) {
            RemoteDirSelector selector = loader.getRemoteDirSelector();
            assertThat(selector).isInstanceOf(RoundRobinRemoteDirSelector.class);

            // 1. Reconfigure with WEIGHTED_ROUND_ROBIN strategy
            Configuration newConfig = new Configuration();
            newConfig.set(
                    ConfigOptions.REMOTE_DATA_DIRS_STRATEGY,
                    ConfigOptions.RemoteDataDirStrategy.WEIGHTED_ROUND_ROBIN);
            newConfig.set(ConfigOptions.REMOTE_DATA_DIRS_WEIGHTS, Arrays.asList(1, 2));
            loader.reconfigure(newConfig);

            // Selector should be replaced
            assertThat(loader.getRemoteDirSelector()).isNotSameAs(selector);
            assertThat(loader.getRemoteDirSelector())
                    .isInstanceOf(WeightedRoundRobinRemoteDirSelector.class);

            selector = loader.getRemoteDirSelector();

            // 2. Reconfigure back to ROUND_ROBIN strategy
            newConfig.set(
                    ConfigOptions.REMOTE_DATA_DIRS_STRATEGY,
                    ConfigOptions.RemoteDataDirStrategy.ROUND_ROBIN);
            loader.reconfigure(newConfig);

            // Selector should be replaced
            assertThat(loader.getRemoteDirSelector()).isNotSameAs(selector);
            assertThat(loader.getRemoteDirSelector())
                    .isInstanceOf(RoundRobinRemoteDirSelector.class);
        }
    }

    @Test
    void testReconfigureWithWeightsChange() throws Exception {
        Configuration conf = new Configuration();
        conf.set(
                ConfigOptions.REMOTE_DATA_DIRS_STRATEGY,
                ConfigOptions.RemoteDataDirStrategy.WEIGHTED_ROUND_ROBIN);
        conf.set(ConfigOptions.REMOTE_DATA_DIRS, Arrays.asList("hdfs://dir1", "hdfs://dir2"));
        conf.set(ConfigOptions.REMOTE_DATA_DIRS_WEIGHTS, Arrays.asList(1, 2));

        try (RemoteDirDynamicLoader loader = new RemoteDirDynamicLoader(conf)) {
            RemoteDirSelector originalSelector = loader.getRemoteDirSelector();
            assertThat(originalSelector).isInstanceOf(WeightedRoundRobinRemoteDirSelector.class);

            // Reconfigure with weights change
            Configuration newConfig = new Configuration();
            newConfig.set(ConfigOptions.REMOTE_DATA_DIRS_WEIGHTS, Arrays.asList(3, 4));
            loader.reconfigure(newConfig);

            // Container should be replaced
            assertThat(loader.getRemoteDirSelector()).isNotSameAs(originalSelector);
            assertThat(loader.getRemoteDirSelector())
                    .isInstanceOf(WeightedRoundRobinRemoteDirSelector.class);
        }
    }

    @Test
    void testReconfigureWithRemoteDataDirsChange() throws Exception {
        // Test new dirs must contain all old dirs
        Configuration conf1 = new Configuration();
        conf1.set(ConfigOptions.REMOTE_DATA_DIRS, Arrays.asList("hdfs://dir1", "hdfs://dir2"));
        try (RemoteDirDynamicLoader loader = new RemoteDirDynamicLoader(conf1)) {
            Configuration newConfig = new Configuration();
            newConfig.set(
                    ConfigOptions.REMOTE_DATA_DIRS, Arrays.asList("hdfs://dir2", "hdfs://dir3"));

            assertThatThrownBy(() -> loader.validate(newConfig))
                    .isInstanceOf(ConfigException.class)
                    .hasMessageContaining("must contain all old remote.data.dirs");
        }
    }
}
