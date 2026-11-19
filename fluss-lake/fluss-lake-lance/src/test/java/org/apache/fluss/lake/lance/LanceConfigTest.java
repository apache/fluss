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

package org.apache.fluss.lake.lance;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link LanceConfig}. */
class LanceConfigTest {

    @Test
    void testGetBatchSizeDefault() {
        Map<String, String> cluster = new HashMap<>();
        cluster.put("warehouse", "/tmp/lance");
        LanceConfig config = LanceConfig.from(cluster, Collections.emptyMap(), "db", "t");
        assertThat(LanceConfig.getBatchSize(config)).isEqualTo(512);
    }

    @Test
    void testGetBatchSizeOverride() {
        Map<String, String> cluster = new HashMap<>();
        cluster.put("warehouse", "/tmp/lance");
        Map<String, String> tableProps = new HashMap<>();
        tableProps.put("batch_size", "128");
        LanceConfig config = LanceConfig.from(cluster, tableProps, "db", "t");
        assertThat(LanceConfig.getBatchSize(config)).isEqualTo(128);
    }

    @Test
    void testGetMaxBytesPerBatchDefault() {
        Map<String, String> cluster = new HashMap<>();
        cluster.put("warehouse", "/tmp/lance");
        LanceConfig config = LanceConfig.from(cluster, Collections.emptyMap(), "db", "t");
        assertThat(LanceConfig.getMaxBytesPerBatch(config)).isZero();
    }

    @Test
    void testGetMaxBytesPerBatchOverride() {
        Map<String, String> cluster = new HashMap<>();
        cluster.put("warehouse", "/tmp/lance");
        Map<String, String> tableProps = new HashMap<>();
        tableProps.put("max_bytes_per_batch", "1048576");
        LanceConfig config = LanceConfig.from(cluster, tableProps, "db", "t");
        assertThat(LanceConfig.getMaxBytesPerBatch(config)).isEqualTo(1_048_576L);
    }

    @Test
    void testGetMaxBytesPerBatchZeroExplicit() {
        Map<String, String> cluster = new HashMap<>();
        cluster.put("warehouse", "/tmp/lance");
        Map<String, String> tableProps = new HashMap<>();
        tableProps.put("max_bytes_per_batch", "0");
        LanceConfig config = LanceConfig.from(cluster, tableProps, "db", "t");
        assertThat(LanceConfig.getMaxBytesPerBatch(config)).isZero();
    }

    @Test
    void testGetMaxBytesPerBatchNegativeRejected() {
        Map<String, String> cluster = new HashMap<>();
        cluster.put("warehouse", "/tmp/lance");
        Map<String, String> tableProps = new HashMap<>();
        tableProps.put("max_bytes_per_batch", "-1");
        LanceConfig config = LanceConfig.from(cluster, tableProps, "db", "t");
        assertThatThrownBy(() -> LanceConfig.getMaxBytesPerBatch(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be non-negative");
    }

    @Test
    void testGetMaxBytesPerBatchInvalidRejected() {
        Map<String, String> cluster = new HashMap<>();
        cluster.put("warehouse", "/tmp/lance");
        Map<String, String> tableProps = new HashMap<>();
        tableProps.put("max_bytes_per_batch", "not-a-number");
        LanceConfig config = LanceConfig.from(cluster, tableProps, "db", "t");
        assertThatThrownBy(() -> LanceConfig.getMaxBytesPerBatch(config))
                .isInstanceOf(NumberFormatException.class);
    }

    @Test
    void testMissingWarehouseRejected() {
        assertThatThrownBy(
                        () ->
                                LanceConfig.from(
                                        Collections.emptyMap(), Collections.emptyMap(), "db", "t"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("warehouse");
    }
}
