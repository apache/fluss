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

package org.apache.fluss.server.zk.data;

import org.apache.fluss.utils.json.JsonSerdeTestBase;
import org.apache.fluss.utils.json.JsonSerdeUtils;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests the persistent recovery snapshot ID format. */
class RecoverySnapshotJsonSerdeTest extends JsonSerdeTestBase<Long> {

    RecoverySnapshotJsonSerdeTest() {
        super(RecoverySnapshotJsonSerde.INSTANCE);
    }

    @Override
    protected Long[] createObjects() {
        return new Long[] {0L, Long.MAX_VALUE};
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"version\":1,\"snapshot_id\":0}",
            "{\"version\":1,\"snapshot_id\":9223372036854775807}"
        };
    }

    @ParameterizedTest
    @ValueSource(strings = {"{\"snapshot_id\":1}", "{\"version\":2,\"snapshot_id\":1}"})
    void testReadCompatibleVersions(String json) {
        assertThat(
                        JsonSerdeUtils.readValue(
                                json.getBytes(StandardCharsets.UTF_8),
                                RecoverySnapshotJsonSerde.INSTANCE))
                .isEqualTo(1L);
    }
}
