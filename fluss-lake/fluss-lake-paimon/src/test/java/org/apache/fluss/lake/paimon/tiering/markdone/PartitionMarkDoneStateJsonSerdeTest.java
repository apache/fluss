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

package org.apache.fluss.lake.paimon.tiering.markdone;

import org.apache.fluss.exception.UnsupportedVersionException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the persisted mark-done state format. */
class PartitionMarkDoneStateJsonSerdeTest {
    @Test
    void testStateJsonSerde() {
        Map<String, Long> trackedPartitionLastUpdateTimes = new HashMap<>();
        trackedPartitionLastUpdateTimes.put("20240101", 1234L);
        trackedPartitionLastUpdateTimes.put("2024-01-02", -1L);
        PartitionMarkDoneState state =
                new PartitionMarkDoneState(true, trackedPartitionLastUpdateTimes);
        int stateHashCode = state.hashCode();
        String stateJson = PartitionMarkDoneStateJsonSerde.toJson(state);
        assertThat(stateJson).contains("\"version\":1", "\"trackedPartitionLastUpdateTimes\":");
        trackedPartitionLastUpdateTimes.clear();
        assertThat(state.getTrackedPartitionLastUpdateTimes()).hasSize(2);
        assertThat(state.hashCode()).isEqualTo(stateHashCode);
        assertThat(PartitionMarkDoneStateJsonSerde.toJson(state)).isEqualTo(stateJson);
        assertThat(PartitionMarkDoneStateJsonSerde.fromJson(stateJson)).isEqualTo(state);

        assertThat(
                        PartitionMarkDoneStateJsonSerde.fromJson(
                                PartitionMarkDoneStateJsonSerde.toJson(
                                        PartitionMarkDoneState.empty())))
                .isEqualTo(PartitionMarkDoneState.empty());
        assertThat(
                        PartitionMarkDoneStateJsonSerde.fromJson(
                                "{\"version\":1,\"initialized\":true,"
                                        + "\"trackedPartitionLastUpdateTimes\":{},\"unknown\":\"x\"}"))
                .isEqualTo(new PartitionMarkDoneState(true, Collections.emptyMap()));
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "{\"initialized\":true,\"trackedPartitionLastUpdateTimes\":{}}",
                "{\"version\":1,\"trackedPartitionLastUpdateTimes\":{}}",
                "{\"version\":1,\"initialized\":true}",
                "{\"version\":\"1\",\"initialized\":true,\"trackedPartitionLastUpdateTimes\":{}}",
                "{\"version\":1,\"initialized\":\"true\",\"trackedPartitionLastUpdateTimes\":{}}",
                "{\"version\":1,\"initialized\":true,\"trackedPartitionLastUpdateTimes\":[]}",
                "{\"version\":1,\"initialized\":true,\"trackedPartitionLastUpdateTimes\":{\"p\":\"bad\"}}"
            })
    void testInvalidState(String stateJson) {
        assertThatThrownBy(() -> PartitionMarkDoneStateJsonSerde.fromJson(stateJson))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testUnsupportedVersion() {
        assertThatThrownBy(() -> PartitionMarkDoneStateJsonSerde.fromJson("{\"version\":2}"))
                .isInstanceOf(UnsupportedVersionException.class);
    }
}
