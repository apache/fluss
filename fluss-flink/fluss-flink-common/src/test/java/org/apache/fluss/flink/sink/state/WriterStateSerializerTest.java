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

package org.apache.fluss.flink.sink.state;

import org.apache.fluss.metadata.TableBucket;

import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link WriterStateSerializer}'s TypeSerializer implementation. */
class WriterStateSerializerTest {

    private WriterStateSerializer serializer;

    @BeforeEach
    void setUp() {
        serializer = new WriterStateSerializer();
    }

    /** Tests that binary copy preserves both V1 legacy and V2 complete state formats. */
    @Test
    void testCopyPreservesV1AndV2State() throws IOException {
        byte[] v1Payload = createV1Payload();
        WriterState v1State = serializer.deserialize(new DataInputDeserializer(v1Payload));
        assertCopiedState(v1Payload, v1State);

        byte[] v2Payload = createV2Payload();
        WriterState v2State = serializer.deserialize(new DataInputDeserializer(v2Payload));
        assertCopiedState(v2Payload, v2State);
    }

    /** Tests TypeSerializerSnapshot creation and restoration. */
    @Test
    void testSnapshotAndRestore() {
        TypeSerializerSnapshot<WriterState> snapshot = serializer.snapshotConfiguration();

        assertThat(snapshot)
                .isInstanceOf(WriterStateSerializer.WriterStateSerializerSnapshot.class);

        TypeSerializer<WriterState> restored = snapshot.restoreSerializer();
        assertThat(restored).isInstanceOf(WriterStateSerializer.class);
        assertThat(restored).isEqualTo(serializer);
    }

    /** Tests compatibility with ListStateDescriptor (Flink 1.18). */
    @Test
    void testListStateDescriptorCompatibility() {
        ListStateDescriptor<WriterState> descriptor =
                new ListStateDescriptor<>("test-state", serializer);

        assertThat(descriptor.getName()).isEqualTo("test-state");
        assertThat(descriptor.getElementSerializer()).isEqualTo(serializer);
    }

    /** Tests serializer properties: immutable, duplicate, length, equals, createInstance, copy. */
    @Test
    void testSerializerProperties() {
        // Immutable type
        assertThat(serializer.isImmutableType()).isTrue();

        // Variable length
        assertThat(serializer.getLength()).isEqualTo(-1);

        // Duplicate returns equal serializer
        assertThat(serializer.duplicate()).isEqualTo(serializer);

        // Equals and hashCode
        WriterStateSerializer other = new WriterStateSerializer();
        assertThat(serializer).isEqualTo(other);
        assertThat(serializer.hashCode()).isEqualTo(other.hashCode());

        // CreateInstance returns empty state
        assertThat(serializer.createInstance()).isEqualTo(WriterState.empty());

        // Copy returns same instance for immutable types
        Map<TableBucket, Long> bucketOffsets = new HashMap<>();
        bucketOffsets.put(new TableBucket(1L, null, 0), 100L);
        WriterState original = new WriterState(bucketOffsets);
        assertThat(serializer.copy(original)).isSameAs(original);
        assertThat(serializer.copy(original, WriterState.empty())).isSameAs(original);
    }

    @Test
    void testDeserializeLiteralV1PayloadRemainsLegacy() throws IOException {
        Map<TableBucket, Long> expectedOffsets = new HashMap<>();
        expectedOffsets.put(new TableBucket(1L, null, 0), 100L);
        expectedOffsets.put(new TableBucket(1L, 10L, 1), 200L);

        WriterState reuse = WriterState.empty();
        WriterState state =
                serializer.deserialize(reuse, new DataInputDeserializer(createV1Payload()));

        assertThat(state.getStateFormat()).isEqualTo(WriterState.StateFormat.V1_LEGACY);
        assertThat(state.getBucketOffsets()).isEqualTo(expectedOffsets);
        assertThat(state).isNotSameAs(reuse);
    }

    @Test
    void testV2GoldenPayload() throws IOException {
        long tableId = 7L;
        Map<TableBucket, Long> offsets =
                Collections.singletonMap(new TableBucket(tableId, 12L, 1), 20L);
        WriterState original = WriterState.complete(tableId, offsets);

        byte[] expectedPayload = createV2Payload();
        WriterState restored = serializer.deserialize(new DataInputDeserializer(expectedPayload));
        DataOutputSerializer output = new DataOutputSerializer(128);
        serializer.serialize(original, output);

        assertThat(restored).isEqualTo(original);
        assertThat(output.getCopyOfBuffer()).containsExactly(expectedPayload);
    }

    @Test
    void testV2RoundTripWithEmptyCompleteMarker() throws IOException {
        WriterState original = WriterState.complete(7L, Collections.emptyMap());

        DataOutputSerializer output = new DataOutputSerializer(32);
        serializer.serialize(original, output);
        WriterState restored =
                serializer.deserialize(new DataInputDeserializer(output.getCopyOfBuffer()));

        assertThat(restored.getStateFormat()).isEqualTo(WriterState.StateFormat.V2_COMPLETE);
        assertThat(restored.getTableId()).isEqualTo(7L);
        assertThat(restored.getBucketOffsets()).isEmpty();
        assertThat(restored).isNotEqualTo(WriterState.empty());
    }

    @Test
    void testRejectsUnsupportedVersion() throws IOException {
        assertMalformedPayload(payloadWithVersion(99), "version");
    }

    private void assertCopiedState(byte[] sourceBytes, WriterState expectedState)
            throws IOException {
        DataOutputSerializer copiedOutput = new DataOutputSerializer(128);
        serializer.copy(new DataInputDeserializer(sourceBytes), copiedOutput);

        WriterState copied =
                serializer.deserialize(new DataInputDeserializer(copiedOutput.getCopyOfBuffer()));
        assertThat(copied).isEqualTo(expectedState);
    }

    private void assertMalformedPayload(byte[] payload, String expectedMessage) {
        assertThatThrownBy(() -> serializer.deserialize(new DataInputDeserializer(payload)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining(expectedMessage);
    }

    private static byte[] createV1Payload() throws IOException {
        DataOutputSerializer output = new DataOutputSerializer(128);
        output.writeInt(1);
        output.writeInt(2);
        writeV1Entry(output, 1L, null, 0, 100L);
        writeV1Entry(output, 1L, 10L, 1, 200L);
        return output.getCopyOfBuffer();
    }

    private static byte[] createV2Payload() throws IOException {
        DataOutputSerializer output = new DataOutputSerializer(64);
        output.writeInt(2);
        output.writeLong(7L);
        output.writeInt(1);
        output.writeBoolean(true);
        output.writeLong(12L);
        output.writeInt(1);
        output.writeLong(20L);
        return output.getCopyOfBuffer();
    }

    private static byte[] payloadWithVersion(int version) throws IOException {
        DataOutputSerializer output = new DataOutputSerializer(8);
        output.writeInt(version);
        return output.getCopyOfBuffer();
    }

    private static void writeV1Entry(
            DataOutputSerializer output, long tableId, Long partitionId, int bucketId, long offset)
            throws IOException {
        output.writeLong(tableId);
        output.writeBoolean(partitionId != null);
        if (partitionId != null) {
            output.writeLong(partitionId);
        }
        output.writeInt(bucketId);
        output.writeLong(offset);
    }
}
