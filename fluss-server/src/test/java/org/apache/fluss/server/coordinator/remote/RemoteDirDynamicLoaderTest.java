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

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RemoteDirDynamicLoader}. */
class RemoteDirDynamicLoaderTest {

    private static final String DEFAULT_REMOTE_DIR = "hdfs://cluster/default";

    @Test
    void testReconfigureWithStrategyChange() throws Exception {
        Configuration conf = createBaseConfiguration();
        conf.set(ConfigOptions.REMOTE_DATA_DIRS, Arrays.asList("hdfs://dir1", "hdfs://dir2"));
        try (RemoteDirDynamicLoader loader = new RemoteDirDynamicLoader(conf)) {
            RemoteDirDynamicLoader.RemoteDirContainer originalContainer =
                    loader.getRemoteDataDirContainer();

            // Reconfigure with strategy change
            Configuration newConfig = new Configuration();
            newConfig.set(
                    ConfigOptions.REMOTE_DATA_DIRS_STRATEGY,
                    ConfigOptions.RemoteDataDirStrategy.WEIGHTED_ROUND_ROBIN);
            newConfig.set(ConfigOptions.REMOTE_DATA_DIRS_WEIGHTS, Arrays.asList(1, 2));
            loader.reconfigure(newConfig);

            // Container should be replaced
            assertThat(loader.getRemoteDataDirContainer()).isNotSameAs(originalContainer);
        }
    }

    @Test
    void testReconfigureWithWeightsChange() throws Exception {
        Configuration conf = createBaseConfiguration();
        conf.set(
                ConfigOptions.REMOTE_DATA_DIRS_STRATEGY,
                ConfigOptions.RemoteDataDirStrategy.WEIGHTED_ROUND_ROBIN);
        conf.set(ConfigOptions.REMOTE_DATA_DIRS, Arrays.asList("hdfs://dir1", "hdfs://dir2"));
        conf.set(ConfigOptions.REMOTE_DATA_DIRS_WEIGHTS, Arrays.asList(1, 2));

        try (RemoteDirDynamicLoader loader = new RemoteDirDynamicLoader(conf)) {
            RemoteDirDynamicLoader.RemoteDirContainer originalContainer =
                    loader.getRemoteDataDirContainer();

            // Reconfigure with weights change
            Configuration newConfig = new Configuration();
            newConfig.set(ConfigOptions.REMOTE_DATA_DIRS_WEIGHTS, Arrays.asList(3, 4));
            loader.reconfigure(newConfig);

            // Container should be replaced
            assertThat(loader.getRemoteDataDirContainer()).isNotSameAs(originalContainer);
        }
    }

    private Configuration createBaseConfiguration() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.REMOTE_DATA_DIR, DEFAULT_REMOTE_DIR);
        return conf;
    }
}
