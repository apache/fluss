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

import org.apache.fluss.config.Configuration;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** Test implementation of {@link LakeTieringDecorator} for testing multiple decorators. */
class TestLakeTieringDecorator2 implements LakeTieringDecorator {

    static final String TEST_FLUSS_CONFIG_KEY = "test.decorator2.fluss.config";
    static final String TEST_FLUSS_CONFIG_VALUE = "decorator2-value";
    static final String TEST_DATA_LAKE_CONFIG_KEY = "test.decorator2.datalake.config";
    static final String TEST_DATA_LAKE_CONFIG_VALUE = "decorator2-datalake-value";
    static final String TEST_LAKE_TIERING_CONFIG_KEY = "test.decorator2.tiering.config";
    static final String TEST_LAKE_TIERING_CONFIG_VALUE = "decorator2-tiering-value";

    @Override
    public void decorate(
            StreamExecutionEnvironment env,
            Configuration flussConfig,
            Configuration dataLakeConfig,
            Configuration lakeTieringConfig,
            String dataLakeFormat) {
        // Modify configurations to verify decorator is called
        flussConfig.setString(TEST_FLUSS_CONFIG_KEY, TEST_FLUSS_CONFIG_VALUE);
        dataLakeConfig.setString(TEST_DATA_LAKE_CONFIG_KEY, TEST_DATA_LAKE_CONFIG_VALUE);
        lakeTieringConfig.setString(TEST_LAKE_TIERING_CONFIG_KEY, TEST_LAKE_TIERING_CONFIG_VALUE);
    }
}
