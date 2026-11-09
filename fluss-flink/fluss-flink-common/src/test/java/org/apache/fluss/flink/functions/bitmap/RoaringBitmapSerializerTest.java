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

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link RoaringBitmapSerializer} and {@link RoaringBitmapTypeInfo}. */
class RoaringBitmapSerializerTest {

    private final RoaringBitmapSerializer serializer = RoaringBitmapSerializer.INSTANCE;

    /** Minimal concrete implementation used only for testing AbstractRbAggFunction. */
    private static final class TestRbAggFunction extends AbstractRbAggFunction {

        public void accumulate(RoaringBitmap acc, Integer value) {
            if (value != null) {
                acc.add(value);
            }
        }
    }

    private final TestRbAggFunction aggFunction = new TestRbAggFunction();

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
    void testSerializeWithRunOptimizableBitmap() throws Exception {
        RoaringBitmap original = new RoaringBitmap();
        original.add(0L, 50_000L);

        DataOutputSerializer out = new DataOutputSerializer(64 * 1024);
        serializer.serialize(original, out);

        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        RoaringBitmap restored = serializer.deserialize(in);

        assertThat(restored).isEqualTo(original);
        assertThat(restored.getLongCardinality()).isEqualTo(50_000L);
        // The deserializer must have consumed exactly the bytes we wrote.
        assertThat(in.available()).isZero();
    }

    @Test
    void testCopyStreamWithRunOptimizableBitmap() throws Exception {
        RoaringBitmap original = new RoaringBitmap();
        original.add(0L, 50_000L);

        DataOutputSerializer out = new DataOutputSerializer(64 * 1024);
        serializer.serialize(original, out);

        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        DataOutputSerializer copied = new DataOutputSerializer(64 * 1024);
        serializer.copy(in, copied);

        RoaringBitmap restored =
                serializer.deserialize(new DataInputDeserializer(copied.getCopyOfBuffer()));
        assertThat(restored).isEqualTo(original);
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

    @Test
    void testTypeInfoIsNotBasicType() {
        assertThat(RoaringBitmapTypeInfo.INSTANCE.isBasicType()).isFalse();
    }

    @Test
    void testTypeInfoIsNotTupleType() {
        assertThat(RoaringBitmapTypeInfo.INSTANCE.isTupleType()).isFalse();
    }

    @Test
    void testTypeInfoToString() {
        assertThat(RoaringBitmapTypeInfo.INSTANCE.toString()).isEqualTo("RoaringBitmapTypeInfo");
    }

    @Test
    void testTypeInfoHashCode() {
        assertThat(RoaringBitmapTypeInfo.INSTANCE.hashCode()).isNotZero();
    }

    @Test
    void testTypeInfoCanEqual() {
        assertThat(RoaringBitmapTypeInfo.INSTANCE.canEqual(RoaringBitmapTypeInfo.INSTANCE))
                .isTrue();
        assertThat(RoaringBitmapTypeInfo.INSTANCE.canEqual("other")).isFalse();
    }

    @Test
    void testAggCreateAccumulator() {
        RoaringBitmap acc = aggFunction.createAccumulator();
        assertThat(acc).isNotNull();
        assertThat(acc.isEmpty()).isTrue();
    }

    @Test
    void testAggGetValue() throws Exception {
        RoaringBitmap acc = aggFunction.createAccumulator();
        aggFunction.accumulate(acc, 1);
        aggFunction.accumulate(acc, 2);

        byte[] result = aggFunction.getValue(acc);

        assertThat(result).isNotNull();
        RoaringBitmap restored = BitmapUtils.fromBytes(result);
        assertThat(restored).isNotNull();
        assertThat(restored.getLongCardinality()).isEqualTo(2L);
        assertThat(restored.contains(1)).isTrue();
        assertThat(restored.contains(2)).isTrue();
    }

    @Test
    void testAggGetValueNullOnEmpty() {
        RoaringBitmap acc = aggFunction.createAccumulator();
        assertThat(aggFunction.getValue(acc)).isNull();
    }

    @Test
    void testAggMerge() {
        RoaringBitmap acc1 = aggFunction.createAccumulator();
        aggFunction.accumulate(acc1, 1);
        aggFunction.accumulate(acc1, 2);

        RoaringBitmap acc2 = aggFunction.createAccumulator();
        aggFunction.accumulate(acc2, 3);

        aggFunction.merge(acc1, Collections.singletonList(acc2));
        assertThat(acc1.getLongCardinality()).isEqualTo(3L);
        assertThat(acc1.contains(3)).isTrue();
    }

    @Test
    void testAggResetAccumulator() {
        RoaringBitmap acc = aggFunction.createAccumulator();
        acc.add(1);
        acc.add(2);

        aggFunction.resetAccumulator(acc);

        assertThat(acc.isEmpty()).isTrue();
    }

    @Test
    void testAggGetAccumulatorType() {
        assertThat(aggFunction.getAccumulatorType()).isInstanceOf(RoaringBitmapTypeInfo.class);
    }
}
