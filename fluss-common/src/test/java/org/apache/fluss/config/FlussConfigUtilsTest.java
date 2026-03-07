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

import org.apache.fluss.exception.IllegalConfigurationException;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.apache.fluss.config.FlussConfigUtils.CLIENT_OPTIONS;
import static org.apache.fluss.config.FlussConfigUtils.TABLE_OPTIONS;
import static org.apache.fluss.config.FlussConfigUtils.extractConfigOptions;
import static org.apache.fluss.config.FlussConfigUtils.validateCoordinatorConfigs;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link FlussConfigUtils}. */
class FlussConfigUtilsTest {

    @Test
    void testExtractOptions() {
        Map<String, ConfigOption<?>> tableOptions = extractConfigOptions("table.");
        assertThat(tableOptions).isNotEmpty();
        tableOptions.forEach(
                (k, v) -> {
                    assertThat(k).startsWith("table.");
                    assertThat(v.key()).startsWith("table.");
                });
        assertThat(tableOptions.size()).isEqualTo(TABLE_OPTIONS.size());

        Map<String, ConfigOption<?>> clientOptions = extractConfigOptions("client.");
        assertThat(clientOptions).isNotEmpty();
        clientOptions.forEach(
                (k, v) -> {
                    assertThat(k).startsWith("client.");
                    assertThat(v.key()).startsWith("client.");
                });
        assertThat(clientOptions.size()).isEqualTo(CLIENT_OPTIONS.size());
    }

    @Test
    void testValidateCoordinatorConfigs() {
        // Test valid configuration
        Configuration validConf = new Configuration();
        validConf.set(ConfigOptions.REMOTE_DATA_DIR, "s3://bucket/path");
        validateCoordinatorConfigs(validConf);

        // Test invalid DEFAULT_REPLICATION_FACTOR
        Configuration invalidReplicationConf = new Configuration();
        invalidReplicationConf.set(ConfigOptions.REMOTE_DATA_DIR, "s3://bucket/path");
        invalidReplicationConf.set(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 0);
        assertThatThrownBy(() -> validateCoordinatorConfigs(invalidReplicationConf))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining(ConfigOptions.DEFAULT_REPLICATION_FACTOR.key())
                .hasMessageContaining("must be greater than or equal 1");

        // Test invalid KV_MAX_RETAINED_SNAPSHOTS
        Configuration invalidSnapshotConf = new Configuration();
        invalidSnapshotConf.set(ConfigOptions.REMOTE_DATA_DIR, "s3://bucket/path");
        invalidSnapshotConf.set(ConfigOptions.KV_MAX_RETAINED_SNAPSHOTS, 0);
        assertThatThrownBy(() -> validateCoordinatorConfigs(invalidSnapshotConf))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining(ConfigOptions.KV_MAX_RETAINED_SNAPSHOTS.key())
                .hasMessageContaining("must be greater than or equal 1");

        // Test invalid SERVER_IO_POOL_SIZE
        Configuration invalidIoPoolConf = new Configuration();
        invalidIoPoolConf.set(ConfigOptions.REMOTE_DATA_DIR, "s3://bucket/path");
        invalidIoPoolConf.set(ConfigOptions.SERVER_IO_POOL_SIZE, 0);
        assertThatThrownBy(() -> validateCoordinatorConfigs(invalidIoPoolConf))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining(ConfigOptions.SERVER_IO_POOL_SIZE.key())
                .hasMessageContaining("must be greater than or equal 1");

        // Test REMOTE_DATA_DIR not set
        Configuration noRemoteDirConf = new Configuration();
        assertThatThrownBy(() -> validateCoordinatorConfigs(noRemoteDirConf))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining(ConfigOptions.REMOTE_DATA_DIR.key())
                .hasMessageContaining("must be set");

        // Test invalid REMOTE_DATA_DIR
        Configuration invalidRemoteDirConf = new Configuration();
        invalidRemoteDirConf.set(ConfigOptions.REMOTE_DATA_DIR, "123://invalid.com");
        assertThatThrownBy(() -> validateCoordinatorConfigs(invalidRemoteDirConf))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining(ConfigOptions.REMOTE_DATA_DIR.key())
                .hasMessageContaining("Invalid configuration for remote.data.dir");

        // Test REMOTE_DATA_DIRS contains invalid path
        Configuration invalidRemoteDirsConf = new Configuration();
        invalidRemoteDirsConf.set(ConfigOptions.REMOTE_DATA_DIR, "s3://bucket/path");
        invalidRemoteDirsConf.set(
                ConfigOptions.REMOTE_DATA_DIRS, Arrays.asList("s3://bucket1", "123://invalid.com"));
        assertThatThrownBy(() -> validateCoordinatorConfigs(invalidRemoteDirsConf))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining(ConfigOptions.REMOTE_DATA_DIRS.key())
                .hasMessageContaining("Invalid remote path for remote.data.dirs");

        // Test WEIGHTED_ROUND_ROBIN with mismatched sizes
        Configuration mismatchedWeightsConf = new Configuration();
        mismatchedWeightsConf.set(ConfigOptions.REMOTE_DATA_DIR, "s3://bucket/path");
        mismatchedWeightsConf.set(
                ConfigOptions.REMOTE_DATA_DIRS_STRATEGY,
                ConfigOptions.RemoteDataDirStrategy.WEIGHTED_ROUND_ROBIN);
        mismatchedWeightsConf.set(
                ConfigOptions.REMOTE_DATA_DIRS, Arrays.asList("s3://bucket1", "s3://bucket2"));
        mismatchedWeightsConf.set(
                ConfigOptions.REMOTE_DATA_DIRS_WEIGHTS, Collections.singletonList(1));
        assertThatThrownBy(() -> validateCoordinatorConfigs(mismatchedWeightsConf))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining(ConfigOptions.REMOTE_DATA_DIRS_WEIGHTS.key())
                .hasMessageContaining(ConfigOptions.REMOTE_DATA_DIRS.key());

        // Test WEIGHTED_ROUND_ROBIN with matched sizes
        Configuration matchedWeightsConf = new Configuration();
        matchedWeightsConf.set(ConfigOptions.REMOTE_DATA_DIR, "s3://bucket/path");
        matchedWeightsConf.set(
                ConfigOptions.REMOTE_DATA_DIRS_STRATEGY,
                ConfigOptions.RemoteDataDirStrategy.WEIGHTED_ROUND_ROBIN);
        matchedWeightsConf.set(
                ConfigOptions.REMOTE_DATA_DIRS, Arrays.asList("s3://bucket1", "s3://bucket2"));
        matchedWeightsConf.set(ConfigOptions.REMOTE_DATA_DIRS_WEIGHTS, Arrays.asList(1, 2));
        validateCoordinatorConfigs(matchedWeightsConf);

        // Test negative weight
        Configuration negativeWeightConf = new Configuration();
        negativeWeightConf.set(ConfigOptions.REMOTE_DATA_DIR, "s3://bucket/path");
        negativeWeightConf.set(
                ConfigOptions.REMOTE_DATA_DIRS_STRATEGY,
                ConfigOptions.RemoteDataDirStrategy.WEIGHTED_ROUND_ROBIN);
        negativeWeightConf.set(
                ConfigOptions.REMOTE_DATA_DIRS, Arrays.asList("s3://bucket1", "s3://bucket2"));
        negativeWeightConf.set(ConfigOptions.REMOTE_DATA_DIRS_WEIGHTS, Arrays.asList(-1, 2));
        assertThatThrownBy(() -> validateCoordinatorConfigs(negativeWeightConf))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining(ConfigOptions.REMOTE_DATA_DIRS_WEIGHTS.key())
                .hasMessageContaining(
                        "All weights in 'remote.data.dirs.weights' must be no less than 0");
    }
}
