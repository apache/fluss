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

package org.apache.fluss.connector.trino.config;

import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link FlussConnectorConfig}.
 */
class FlussConnectorConfigTest {

    @Test
    void testDefaultValues() {
        FlussConnectorConfig config = new FlussConnectorConfig();
        
        assertThat(config.getConnectionMaxIdleTime())
                .isEqualTo(new Duration(10, TimeUnit.MINUTES));
        assertThat(config.getRequestTimeout())
                .isEqualTo(new Duration(60, TimeUnit.SECONDS));
        assertThat(config.isUnionReadEnabled()).isTrue();
        assertThat(config.isColumnPruningEnabled()).isTrue();
        assertThat(config.isPredicatePushdownEnabled()).isTrue();
        assertThat(config.isLimitPushdownEnabled()).isTrue();
        assertThat(config.isAggregatePushdownEnabled()).isFalse();
    }

    @Test
    void testSetBootstrapServers() {
        FlussConnectorConfig config = new FlussConnectorConfig();
        config.setBootstrapServers("localhost:9092");
        
        assertThat(config.getBootstrapServers()).isEqualTo("localhost:9092");
    }

    @Test
    void testSetUnionReadEnabled() {
        FlussConnectorConfig config = new FlussConnectorConfig();
        config.setUnionReadEnabled(false);
        
        assertThat(config.isUnionReadEnabled()).isFalse();
    }

    @Test
    void testSetOptimizationFlags() {
        FlussConnectorConfig config = new FlussConnectorConfig();
        
        config.setColumnPruningEnabled(false);
        config.setPredicatePushdownEnabled(false);
        config.setLimitPushdownEnabled(false);
        
        assertThat(config.isColumnPruningEnabled()).isFalse();
        assertThat(config.isPredicatePushdownEnabled()).isFalse();
        assertThat(config.isLimitPushdownEnabled()).isFalse();
    }

    @Test
    void testPerformanceSettings() {
        FlussConnectorConfig config = new FlussConnectorConfig();
        
        config.setMaxSplitsPerSecond(500);
        config.setMaxSplitsPerRequest(50);
        
        assertThat(config.getMaxSplitsPerSecond()).isEqualTo(500);
        assertThat(config.getMaxSplitsPerRequest()).isEqualTo(50);
    }
}
