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
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link WriterStateSerializer}'s TypeSerializer implementation. */
class WriterStateSerializerTest {

    private WriterStateSerializer serializer;

    @BeforeEach
    void setUp() {
        serializer = new WriterStateSerializer();
    }

    /** Tests serialize/deserialize with normal state, empty state, and reuse scenarios. */
    @Test
    void testSerializeDeserialize() throws IOException {
        // Test with multiple bucket offsets (including partitioned table)
        Map<TableBucket, Long> bucketOffsets = new HashMap<>();
        bucketOffsets.put(new TableBucket(1L, null, 0), 100L);
        bucketOffsets.put(new TableBucket(1L, null, 1), 200L);
        bucketOffsets.put(new TableBucket(2L, 10L, 0), 300L);
        WriterState original = new WriterState(bucketOffsets);

        DataOutputSerializer output = new DataOutputSerializer(256);
        serializer.serialize(original, output);

        DataInputDeserializer input = new DataInputDeserializer(output.getCopyOfBuffer());
        WriterState deserialized = serializer.deserialize(input);

        assertThat(deserialized).isEqualTo(original);
        assertThat(deserialized.getBucketOffsets()).hasSize(3);

        // Test empty state
        WriterState emptyState = WriterState.empty();
        output = new DataOutputSerializer(64);
        serializer.serialize(emptyState, output);
        input = new DataInputDeserializer(output.getCopyOfBuffer());
        assertThat(serializer.deserialize(input)).isEqualTo(emptyState);

        // Test deserialize with reuse (should be ignored for immutable types)
        output = new DataOutputSerializer(256);
        serializer.serialize(original, output);
        input = new DataInputDeserializer(output.getCopyOfBuffer());
        WriterState reuse = WriterState.empty();
        deserialized = serializer.deserialize(reuse, input);
        assertThat(deserialized).isEqualTo(original);
        assertThat(deserialized).isNotSameAs(reuse);
    }

    /** Tests copy between DataInputView and DataOutputView. */
    @Test
    void testCopyBetweenViews() throws IOException {
        Map<TableBucket, Long> bucketOffsets = new HashMap<>();
        bucketOffsets.put(new TableBucket(1L, null, 0), 100L);
        bucketOffsets.put(new TableBucket(2L, 10L, 1), 200L);
        WriterState original = new WriterState(bucketOffsets);

        DataOutputSerializer sourceOutput = new DataOutputSerializer(256);
        serializer.serialize(original, sourceOutput);

        DataInputDeserializer sourceInput =
                new DataInputDeserializer(sourceOutput.getCopyOfBuffer());
        DataOutputSerializer targetOutput = new DataOutputSerializer(256);
        serializer.copy(sourceInput, targetOutput);

        DataInputDeserializer targetInput =
                new DataInputDeserializer(targetOutput.getCopyOfBuffer());
        assertThat(serializer.deserialize(targetInput)).isEqualTo(original);
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
        WriterState instance = serializer.createInstance();
        assertThat(instance.getBucketOffsets()).isEmpty();

        // Copy returns same instance for immutable types
        Map<TableBucket, Long> bucketOffsets = new HashMap<>();
        bucketOffsets.put(new TableBucket(1L, null, 0), 100L);
        WriterState original = new WriterState(bucketOffsets);
        assertThat(serializer.copy(original)).isSameAs(original);
        assertThat(serializer.copy(original, WriterState.empty())).isSameAs(original);
    }

    /** Tests that TypeSerializer and SimpleVersionedSerializer produce compatible bytes. */
    @Test
    void testSimpleVersionedSerializerCompatibility() throws IOException {
        Map<TableBucket, Long> bucketOffsets = new HashMap<>();
        bucketOffsets.put(new TableBucket(1L, null, 0), 100L);
        bucketOffsets.put(new TableBucket(2L, 10L, 1), 200L);
        WriterState original = new WriterState(bucketOffsets);

        // Serialize using TypeSerializer
        DataOutputSerializer output = new DataOutputSerializer(256);
        serializer.serialize(original, output);
        byte[] typeSerializerBytes = output.getCopyOfBuffer();

        // Both should deserialize to equal results
        DataInputDeserializer input = new DataInputDeserializer(typeSerializerBytes);
        WriterState fromTypeSerializer = serializer.deserialize(input);

        assertThat(fromTypeSerializer).isEqualTo(original);
    }
}
