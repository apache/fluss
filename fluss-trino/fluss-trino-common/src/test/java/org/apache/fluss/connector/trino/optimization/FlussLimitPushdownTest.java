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

package org.apache.fluss.connector.trino.optimization;

import org.apache.fluss.connector.trino.config.FlussConnectorConfig;
import org.apache.fluss.connector.trino.handle.FlussTableHandle;
import org.apache.fluss.metadata.TableInfo;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link FlussLimitPushdown}.
 */
class FlussLimitPushdownTest {

    private FlussConnectorConfig config;
    private FlussLimitPushdown limitPushdown;

    @BeforeEach
    void setUp() {
        config = new FlussConnectorConfig();
        config.setLimitPushdownEnabled(true);
        limitPushdown = new FlussLimitPushdown(config);
    }

    @Test
    void testApplyLimitPushdown() {
        TableInfo tableInfo = Mockito.mock(TableInfo.class);
        FlussTableHandle handle = new FlussTableHandle("db1", "table1", tableInfo);
        
        FlussTableHandle result = limitPushdown.applyLimitPushdown(handle, 100);
        
        assertThat(result.getLimit()).isPresent();
        assertThat(result.getLimit().get()).isEqualTo(100L);
    }

    @Test
    void testDisabledPushdown() {
        config.setLimitPushdownEnabled(false);
        limitPushdown = new FlussLimitPushdown(config);
        
        TableInfo tableInfo = Mockito.mock(TableInfo.class);
        FlussTableHandle handle = new FlussTableHandle("db1", "table1", tableInfo);
        
        FlussTableHandle result = limitPushdown.applyLimitPushdown(handle, 100);
        
        assertThat(result.getLimit()).isEmpty();
    }

    @Test
    void testIsBeneficial() {
        assertThat(limitPushdown.isBeneficial(100, 10000)).isTrue();
        assertThat(limitPushdown.isBeneficial(9000, 10000)).isFalse();
        assertThat(limitPushdown.isBeneficial(5000, 10000)).isFalse();
    }

    @Test
    void testEstimateDataReduction() {
        double reduction = limitPushdown.estimateDataReduction(100, 10000);
        assertThat(reduction).isEqualTo(0.99);
        
        reduction = limitPushdown.estimateDataReduction(5000, 10000);
        assertThat(reduction).isEqualTo(0.5);
    }
}
