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

package org.apache.fluss.flink.sink.serializer;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SinkOperationMode}. */
class SinkOperationModeTest {

    @Test
    void testAppendOnlyMode() {
        SinkOperationMode mode = SinkOperationMode.appendOnly(false);
        assertThat(mode.isAppendOnly()).isTrue();
        assertThat(mode.isIgnoreDelete()).isFalse();
        assertThat(mode.isAggregationTable()).isFalse();
        assertThat(mode.supportsRetract()).isFalse();
    }

    @Test
    void testAppendOnlyModeWithIgnoreDelete() {
        SinkOperationMode mode = SinkOperationMode.appendOnly(true);
        assertThat(mode.isAppendOnly()).isTrue();
        assertThat(mode.isIgnoreDelete()).isTrue();
        assertThat(mode.isAggregationTable()).isFalse();
        assertThat(mode.supportsRetract()).isFalse();
    }

    @Test
    void testUpsertMode() {
        SinkOperationMode mode = SinkOperationMode.upsert(false);
        assertThat(mode.isAppendOnly()).isFalse();
        assertThat(mode.isIgnoreDelete()).isFalse();
        assertThat(mode.isAggregationTable()).isFalse();
        assertThat(mode.supportsRetract()).isFalse();
    }

    @Test
    void testUpsertModeWithIgnoreDelete() {
        SinkOperationMode mode = SinkOperationMode.upsert(true);
        assertThat(mode.isAppendOnly()).isFalse();
        assertThat(mode.isIgnoreDelete()).isTrue();
        assertThat(mode.isAggregationTable()).isFalse();
        assertThat(mode.supportsRetract()).isFalse();
    }

    @Test
    void testAggregationModeWithoutRetract() {
        SinkOperationMode mode = SinkOperationMode.aggregation(false, false);
        assertThat(mode.isAppendOnly()).isFalse();
        assertThat(mode.isIgnoreDelete()).isFalse();
        assertThat(mode.isAggregationTable()).isTrue();
        assertThat(mode.supportsRetract()).isFalse();
    }

    @Test
    void testAggregationModeWithRetract() {
        SinkOperationMode mode = SinkOperationMode.aggregation(false, true);
        assertThat(mode.isAppendOnly()).isFalse();
        assertThat(mode.isIgnoreDelete()).isFalse();
        assertThat(mode.isAggregationTable()).isTrue();
        assertThat(mode.supportsRetract()).isTrue();
    }

    @Test
    void testAggregationModeWithIgnoreDeleteAndRetract() {
        SinkOperationMode mode = SinkOperationMode.aggregation(true, true);
        assertThat(mode.isAppendOnly()).isFalse();
        assertThat(mode.isIgnoreDelete()).isTrue();
        assertThat(mode.isAggregationTable()).isTrue();
        assertThat(mode.supportsRetract()).isTrue();
    }
}
