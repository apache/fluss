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

package org.apache.fluss.flink.tiering;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FlussLakeTiering}. */
class FlussLakeTieringTest {

    @Test
    void testMultipleDecoratorsApplied() {
        String[] args = {
            "--fluss.bootstrap.servers",
            "localhost:9123",
            "--datalake.paimon.metastore",
            "rest",
            "--datalake.paimon.warehous",
            "fluss_test",
            "--datalake.format",
            "paimon",
        };
        FlussLakeTiering tiering = new FlussLakeTiering(args);

        // Apply decorators - method now uses internal Configuration fields
        tiering.loadAndApplyDecoratorPlugins();

        // Verify that TestLakeTieringDecorator modified the configurations
        assertThat(tiering.flussConfig.getRawValue(TestLakeTieringDecorator.TEST_FLUSS_CONFIG_KEY))
                .isPresent()
                .contains(TestLakeTieringDecorator.TEST_FLUSS_CONFIG_VALUE);
        assertThat(
                        tiering.lakeConfig.getRawValue(
                                TestLakeTieringDecorator.TEST_DATA_LAKE_CONFIG_KEY))
                .isPresent()
                .contains(TestLakeTieringDecorator.TEST_DATA_LAKE_CONFIG_VALUE);
        assertThat(
                        tiering.lakeTieringConfig.getRawValue(
                                TestLakeTieringDecorator.TEST_LAKE_TIERING_CONFIG_KEY))
                .isPresent()
                .contains(TestLakeTieringDecorator.TEST_LAKE_TIERING_CONFIG_VALUE);

        // Verify that TestLakeTieringDecorator2 also modified the configurations
        assertThat(tiering.flussConfig.getRawValue(TestLakeTieringDecorator2.TEST_FLUSS_CONFIG_KEY))
                .isPresent()
                .contains(TestLakeTieringDecorator2.TEST_FLUSS_CONFIG_VALUE);
        assertThat(
                        tiering.lakeConfig.getRawValue(
                                TestLakeTieringDecorator2.TEST_DATA_LAKE_CONFIG_KEY))
                .isPresent()
                .contains(TestLakeTieringDecorator2.TEST_DATA_LAKE_CONFIG_VALUE);
        assertThat(
                        tiering.lakeTieringConfig.getRawValue(
                                TestLakeTieringDecorator2.TEST_LAKE_TIERING_CONFIG_KEY))
                .isPresent()
                .contains(TestLakeTieringDecorator2.TEST_LAKE_TIERING_CONFIG_VALUE);
    }
}
