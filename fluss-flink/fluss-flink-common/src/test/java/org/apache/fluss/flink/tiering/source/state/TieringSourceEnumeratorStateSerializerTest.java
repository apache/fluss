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

package org.apache.fluss.flink.tiering.source.state;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link TieringSourceEnumeratorStateSerializer} and {@link
 * TieringSourceEnumeratorState}.
 */
class TieringSourceEnumeratorStateSerializerTest {

    private static final TieringSourceEnumeratorStateSerializer serializer =
            TieringSourceEnumeratorStateSerializer.INSTANCE;

    @Test
    void testSerDeserializeWithLeaseId() throws Exception {
        TieringSourceEnumeratorState state =
                new TieringSourceEnumeratorState("tiering-test-lease-123");
        byte[] serialized = serializer.serialize(state);
        assertThat(serialized.length).isGreaterThan(0);
        TieringSourceEnumeratorState deserialized =
                serializer.deserialize(serializer.getVersion(), serialized);
        assertThat(deserialized).isEqualTo(state);
        assertThat(deserialized.getKvSnapshotLeaseId()).isEqualTo("tiering-test-lease-123");
    }

    @Test
    void testSerDeserializeWithNullLeaseId() throws Exception {
        TieringSourceEnumeratorState state = new TieringSourceEnumeratorState(null);
        byte[] serialized = serializer.serialize(state);
        TieringSourceEnumeratorState deserialized =
                serializer.deserialize(serializer.getVersion(), serialized);
        assertThat(deserialized).isEqualTo(state);
        assertThat(deserialized.getKvSnapshotLeaseId()).isNull();
    }

    @Test
    void testV0Compatibility() throws Exception {
        // v0 serialized an empty byte array; deserialization should produce a state with null
        // lease id
        TieringSourceEnumeratorState deserialized = serializer.deserialize(0, new byte[0]);
        assertThat(deserialized.getKvSnapshotLeaseId()).isNull();
    }

    @Test
    void testDefaultConstructorHasNullLeaseId() {
        TieringSourceEnumeratorState state = new TieringSourceEnumeratorState();
        assertThat(state.getKvSnapshotLeaseId()).isNull();
    }
}
