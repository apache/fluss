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

package org.apache.fluss.utils.json;

import org.apache.fluss.lake.committer.LakeTieringTableState;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link LakeTieringTableStateJsonSerde}. */
class LakeTieringTableStateJsonSerdeTest {

    @Test
    void testSerializeFormat() {
        Map<Long, Long> updateTimes = new HashMap<>();
        updateTimes.put(5L, 1704153550000L);
        LakeTieringTableState state = new LakeTieringTableState(true, updateTimes);

        String json = new String(state.toJsonBytes(), StandardCharsets.UTF_8);
        assertThat(json)
                .isEqualTo(
                        "{\"version\":1,\"partition_done_initialized\":true,"
                                + "\"partition_update_times\":{\"5\":1704153550000}}");
    }

    @Test
    void testRoundTripAndDefaults() {
        // populated state round-trips exactly.
        Map<Long, Long> updateTimes = new HashMap<>();
        updateTimes.put(5L, 1704153550000L);
        updateTimes.put(8L, 1704157200000L);
        LakeTieringTableState state = new LakeTieringTableState(true, updateTimes);
        LakeTieringTableState result = LakeTieringTableState.fromJsonBytes(state.toJsonBytes());
        assertThat(result).isEqualTo(state);
        assertThat(result.getVersion()).isEqualTo(LakeTieringTableState.VERSION_1);
        assertThat(result.isPartitionDoneInitialized()).isTrue();
        assertThat(result.getPartitionUpdateTimes())
                .containsEntry(5L, 1704153550000L)
                .containsEntry(8L, 1704157200000L);

        // with a version present, missing fields fall back to defaults.
        LakeTieringTableState defaults =
                LakeTieringTableState.fromJsonBytes(
                        "{\"version\":1}".getBytes(StandardCharsets.UTF_8));
        assertThat(defaults.getVersion()).isEqualTo(LakeTieringTableState.VERSION_1);
        assertThat(defaults.isPartitionDoneInitialized()).isFalse();
        assertThat(defaults.getPartitionUpdateTimes()).isEmpty();
    }

    @Test
    void testStrictValidationRejectsCorruptFields() {
        // version is mandatory and must be a positive integer; for a known version, corrupt known
        // fields must fail fast rather than be silently coerced.
        String[] corrupt = {
            "{}",
            "{\"version\":0}",
            "{\"version\":\"x\"}",
            "{\"version\":1,\"partition_done_initialized\":\"x\"}",
            "{\"version\":1,\"partition_update_times\":5}",
            "{\"version\":1,\"partition_update_times\":{\"x\":1}}",
            "{\"version\":1,\"partition_update_times\":{\"0\":1}}",
            "{\"version\":1,\"partition_update_times\":{\"5\":-1}}"
        };
        for (String bad : corrupt) {
            assertThatThrownBy(
                            () ->
                                    LakeTieringTableState.fromJsonBytes(
                                            bad.getBytes(StandardCharsets.UTF_8)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void testHigherVersionIsReadableButNotWritable() {
        // Reading a higher (unsupported) version and unknown fields must not raise; only the
        // version
        // is read (higher-version fields skipped) so an older build can detect it and degrade to
        // read-only.
        String json =
                "{\"version\":99,\"partition_done_initialized\":true,"
                        + "\"partition_update_times\":{\"5\":1000},\"future_field\":\"x\"}";
        LakeTieringTableState state =
                LakeTieringTableState.fromJsonBytes(json.getBytes(StandardCharsets.UTF_8));
        assertThat(state.getVersion()).isEqualTo(99);
        // higher-version fields are NOT interpreted.
        assertThat(state.isPartitionDoneInitialized()).isFalse();
        assertThat(state.getPartitionUpdateTimes()).isEmpty();
        // Writing it back is refused: an older build must not overwrite a newer-version state.
        assertThatThrownBy(state::toJsonBytes)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("read-only");
    }
}
