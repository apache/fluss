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

package org.apache.fluss.flink.functions.bitmap;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.RoaringBitmap;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link RoaringBitmapSerializer} and {@link RoaringBitmapTypeInfo}. */
class RoaringBitmapSerializerTest {

    private final RoaringBitmapSerializer serializer = RoaringBitmapSerializer.INSTANCE;

    @Test
    void testCreateInstance() {
        RoaringBitmap instance = serializer.createInstance();
        assertThat(instance).isNotNull();
        assertThat(instance.isEmpty()).isTrue();
    }

    @Test
    void testIsNotImmutable() {
        assertThat(serializer.isImmutableType()).isFalse();
    }

    @Test
    void testGetLengthIsMinusOne() {
        assertThat(serializer.getLength()).isEqualTo(-1);
    }

    @Test
    void testSerializeDeserializeRoundTrip() throws Exception {
        RoaringBitmap original = new RoaringBitmap();
        original.add(1);
        original.add(100);
        original.add(100_000);

        DataOutputSerializer out = new DataOutputSerializer(256);
        serializer.serialize(original, out);

        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        RoaringBitmap restored = serializer.deserialize(in);

        assertThat(restored).isEqualTo(original);
        assertThat(restored.getLongCardinality()).isEqualTo(3L);
    }

    @Test
    void testDeserializeWithReuse() throws Exception {
        RoaringBitmap original = RoaringBitmap.bitmapOf(42, 99, 1000);

        DataOutputSerializer out = new DataOutputSerializer(256);
        serializer.serialize(original, out);

        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        RoaringBitmap reuse = new RoaringBitmap();
        RoaringBitmap restored = serializer.deserialize(reuse, in);

        assertThat(restored).isEqualTo(original);
    }

    @Test
    void testCopy() {
        RoaringBitmap original = RoaringBitmap.bitmapOf(1, 2, 3);
        RoaringBitmap copy = serializer.copy(original);

        assertThat(copy).isEqualTo(original);
        // Verify it is a deep copy
        copy.add(999);
        assertThat(original.contains(999)).isFalse();
    }

    @Test
    void testCopyWithReuse() {
        RoaringBitmap original = RoaringBitmap.bitmapOf(10, 20, 30);
        RoaringBitmap reuse = new RoaringBitmap();
        RoaringBitmap copy = serializer.copy(original, reuse);

        assertThat(copy).isEqualTo(original);
    }

    @Test
    void testEmptyBitmapRoundTrip() throws Exception {
        RoaringBitmap empty = new RoaringBitmap();

        DataOutputSerializer out = new DataOutputSerializer(64);
        serializer.serialize(empty, out);

        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        RoaringBitmap restored = serializer.deserialize(in);

        assertThat(restored.isEmpty()).isTrue();
    }

    @Test
    void testSnapshotConfiguration() {
        assertThat(serializer.snapshotConfiguration()).isNotNull();
    }

    // RoaringBitmapTypeInfo tests

    @Test
    void testTypeInfoGetTypeClass() {
        assertThat(RoaringBitmapTypeInfo.INSTANCE.getTypeClass()).isEqualTo(RoaringBitmap.class);
    }

    @Test
    void testTypeInfoCreateSerializer() {
        TypeSerializer<RoaringBitmap> s =
                RoaringBitmapTypeInfo.INSTANCE.createSerializer(new ExecutionConfig());
        assertThat(s).isInstanceOf(RoaringBitmapSerializer.class);
    }

    @Test
    void testTypeInfoEquality() {
        assertThat(RoaringBitmapTypeInfo.INSTANCE.equals(RoaringBitmapTypeInfo.INSTANCE)).isTrue();
        assertThat(RoaringBitmapTypeInfo.INSTANCE.equals("other")).isFalse();
    }

    @Test
    void testTypeInfoIsNotKeyType() {
        assertThat(RoaringBitmapTypeInfo.INSTANCE.isKeyType()).isFalse();
    }

    @Test
    void testTypeInfoArity() {
        assertThat(RoaringBitmapTypeInfo.INSTANCE.getArity()).isEqualTo(1);
        assertThat(RoaringBitmapTypeInfo.INSTANCE.getTotalFields()).isEqualTo(1);
    }
}
