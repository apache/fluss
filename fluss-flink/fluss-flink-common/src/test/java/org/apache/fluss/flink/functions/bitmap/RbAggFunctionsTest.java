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

import java.io.IOException;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link RbBuildAggFunction}, {@link RbOrAggFunction}, {@link RbAndAggFunction},
 * {@link RbXorAggFunction}.
 */
class RbAggFunctionsTest {

    // -------------------------------------------------------------------------
    // RbBuildAggFunction
    // -------------------------------------------------------------------------

    @Test
    void testBuildAggBasic() throws IOException {
        RbBuildAggFunction fn = new RbBuildAggFunction();
        RoaringBitmap acc = fn.createAccumulator();

        fn.accumulate(acc, 1);
        fn.accumulate(acc, 2);
        fn.accumulate(acc, 1); // duplicate — ignored
        fn.accumulate(acc, null); // null — ignored

        byte[] result = fn.getValue(acc);
        assertThat(result).isNotNull();
        RoaringBitmap restored = BitmapUtils.fromBytes(result);
        assertThat(restored.getLongCardinality()).isEqualTo(2L);
        assertThat(restored.contains(1)).isTrue();
        assertThat(restored.contains(2)).isTrue();
    }

    @Test
    void testBuildAggEmptyReturnsNull() {
        RbBuildAggFunction fn = new RbBuildAggFunction();
        RoaringBitmap acc = fn.createAccumulator();
        assertThat(fn.getValue(acc)).isNull();
    }

    @Test
    void testBuildAggMerge() throws IOException {
        RbBuildAggFunction fn = new RbBuildAggFunction();
        RoaringBitmap acc1 = fn.createAccumulator();
        fn.accumulate(acc1, 1);
        fn.accumulate(acc1, 2);

        RoaringBitmap acc2 = fn.createAccumulator();
        fn.accumulate(acc2, 3);

        fn.merge(acc1, Collections.singletonList(acc2));
        assertThat(acc1.getLongCardinality()).isEqualTo(3L);
        assertThat(acc1.contains(3)).isTrue();
    }

    @Test
    void testBuildAggRetractThrows() {
        RbBuildAggFunction fn = new RbBuildAggFunction();
        assertThatThrownBy(() -> fn.retract(fn.createAccumulator(), 1))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    // -------------------------------------------------------------------------
    // RbOrAggFunction
    // -------------------------------------------------------------------------

    @Test
    void testOrAggBasic() throws IOException {
        RbOrAggFunction fn = new RbOrAggFunction();
        RoaringBitmap acc = fn.createAccumulator();

        byte[] bitmap1 = BitmapUtils.toBytes(RoaringBitmap.bitmapOf(1, 2, 3));
        byte[] bitmap2 = BitmapUtils.toBytes(RoaringBitmap.bitmapOf(3, 4, 5));

        fn.accumulate(acc, bitmap1);
        fn.accumulate(acc, bitmap2);
        fn.accumulate(acc, null); // ignored
        fn.accumulate(acc, new byte[0]); // ignored

        byte[] result = fn.getValue(acc);
        assertThat(result).isNotNull();
        RoaringBitmap restored = BitmapUtils.fromBytes(result);
        assertThat(restored.getLongCardinality()).isEqualTo(5L);
        assertThat(restored.contains(1)).isTrue();
        assertThat(restored.contains(5)).isTrue();
    }

    @Test
    void testOrAggEmptyReturnsNull() {
        RbOrAggFunction fn = new RbOrAggFunction();
        assertThat(fn.getValue(fn.createAccumulator())).isNull();
    }

    @Test
    void testOrAggMerge() throws IOException {
        RbOrAggFunction fn = new RbOrAggFunction();
        RoaringBitmap acc1 = fn.createAccumulator();
        fn.accumulate(acc1, BitmapUtils.toBytes(RoaringBitmap.bitmapOf(1, 2)));

        RoaringBitmap acc2 = fn.createAccumulator();
        fn.accumulate(acc2, BitmapUtils.toBytes(RoaringBitmap.bitmapOf(3, 4)));

        fn.merge(acc1, Collections.singletonList(acc2));
        assertThat(acc1.getLongCardinality()).isEqualTo(4L);
    }

    // -------------------------------------------------------------------------
    // RbAndAggFunction
    // -------------------------------------------------------------------------

    @Test
    void testAndAggBasic() throws IOException {
        RbAndAggFunction fn = new RbAndAggFunction();
        RbAndAggFunction.Accumulator acc = fn.createAccumulator();

        fn.accumulate(acc, BitmapUtils.toBytes(RoaringBitmap.bitmapOf(1, 2, 3)));
        fn.accumulate(acc, BitmapUtils.toBytes(RoaringBitmap.bitmapOf(2, 3, 4)));
        fn.accumulate(acc, BitmapUtils.toBytes(RoaringBitmap.bitmapOf(3, 5, 6)));

        byte[] result = fn.getValue(acc);
        assertThat(result).isNotNull();
        RoaringBitmap restored = BitmapUtils.fromBytes(result);
        assertThat(restored.getLongCardinality()).isEqualTo(1L);
        assertThat(restored.contains(3)).isTrue();
    }

    @Test
    void testAndAggNullInputsIgnored() throws IOException {
        RbAndAggFunction fn = new RbAndAggFunction();
        RbAndAggFunction.Accumulator acc = fn.createAccumulator();

        fn.accumulate(acc, null);
        fn.accumulate(acc, new byte[0]);
        assertThat(fn.getValue(acc)).isNull(); // no real input

        fn.accumulate(acc, BitmapUtils.toBytes(RoaringBitmap.bitmapOf(1, 2)));
        assertThat(fn.getValue(acc)).isNotNull();
    }

    @Test
    void testAndAggEmptyIntersectionReturnsNull() throws IOException {
        RbAndAggFunction fn = new RbAndAggFunction();
        RbAndAggFunction.Accumulator acc = fn.createAccumulator();

        fn.accumulate(acc, BitmapUtils.toBytes(RoaringBitmap.bitmapOf(1, 2)));
        fn.accumulate(acc, BitmapUtils.toBytes(RoaringBitmap.bitmapOf(3, 4)));
        // Intersection of {1,2} and {3,4} is empty
        assertThat(fn.getValue(acc)).isNull();
    }

    @Test
    void testAndAggMerge() throws IOException {
        RbAndAggFunction fn = new RbAndAggFunction();

        RbAndAggFunction.Accumulator acc1 = fn.createAccumulator();
        fn.accumulate(acc1, BitmapUtils.toBytes(RoaringBitmap.bitmapOf(1, 2, 3)));

        RbAndAggFunction.Accumulator acc2 = fn.createAccumulator();
        fn.accumulate(acc2, BitmapUtils.toBytes(RoaringBitmap.bitmapOf(2, 3, 4)));

        fn.merge(acc1, Collections.singletonList(acc2));
        byte[] result = fn.getValue(acc1);
        assertThat(result).isNotNull();
        RoaringBitmap restored = BitmapUtils.fromBytes(result);
        assertThat(restored.getLongCardinality()).isEqualTo(2L);
        assertThat(restored.contains(2)).isTrue();
        assertThat(restored.contains(3)).isTrue();
    }

    @Test
    void testAndAggResetAccumulator() throws IOException {
        RbAndAggFunction fn = new RbAndAggFunction();
        RbAndAggFunction.Accumulator acc = fn.createAccumulator();

        fn.accumulate(acc, BitmapUtils.toBytes(RoaringBitmap.bitmapOf(1, 2)));
        fn.resetAccumulator(acc);
        assertThat(acc.initialized).isFalse();
        assertThat(fn.getValue(acc)).isNull();
    }

    @Test
    void testAndAggAccumulatorSerializerRoundTrip() throws Exception {
        RbAndAggFunction.AccumulatorSerializer ser =
                RbAndAggFunction.AccumulatorSerializer.INSTANCE;

        RbAndAggFunction.Accumulator original = new RbAndAggFunction.Accumulator();
        original.initialized = true;
        original.value = RoaringBitmap.bitmapOf(10, 20, 30);

        DataOutputSerializer out = new DataOutputSerializer(256);
        ser.serialize(original, out);

        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        RbAndAggFunction.Accumulator restored = ser.deserialize(in);

        assertThat(restored.initialized).isTrue();
        assertThat(restored.value).isEqualTo(original.value);
    }

    @Test
    void testAndAggAccumulatorSerializerUninitializedRoundTrip() throws Exception {
        RbAndAggFunction.AccumulatorSerializer ser =
                RbAndAggFunction.AccumulatorSerializer.INSTANCE;

        RbAndAggFunction.Accumulator original = new RbAndAggFunction.Accumulator();

        DataOutputSerializer out = new DataOutputSerializer(64);
        ser.serialize(original, out);

        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        RbAndAggFunction.Accumulator restored = ser.deserialize(in);
        assertThat(restored.initialized).isFalse();
    }

    // -------------------------------------------------------------------------
    // RbAndAggFunction inner class coverage
    // -------------------------------------------------------------------------

    @Test
    void testAndAggAccumulatorTypeInfoProperties() {
        RbAndAggFunction.AccumulatorTypeInfo info = RbAndAggFunction.AccumulatorTypeInfo.INSTANCE;
        assertThat(info.getTypeClass()).isEqualTo(RbAndAggFunction.Accumulator.class);
        assertThat(info.isBasicType()).isFalse();
        assertThat(info.isTupleType()).isFalse();
        assertThat(info.getArity()).isEqualTo(1);
        assertThat(info.getTotalFields()).isEqualTo(1);
        assertThat(info.isKeyType()).isFalse();
        assertThat(info.toString()).isEqualTo("RbAndAccumulatorTypeInfo");
        assertThat(info.hashCode()).isNotZero();
        assertThat(info.equals(info)).isTrue();
        assertThat(info.equals("other")).isFalse();
        assertThat(info.canEqual(info)).isTrue();
        assertThat(info.canEqual("other")).isFalse();
    }

    @Test
    void testAndAggAccumulatorTypeInfoCreateSerializer() {
        TypeSerializer<RbAndAggFunction.Accumulator> s =
                RbAndAggFunction.AccumulatorTypeInfo.INSTANCE.createSerializer(
                        new ExecutionConfig());
        assertThat(s).isInstanceOf(RbAndAggFunction.AccumulatorSerializer.class);
    }

    @Test
    void testAndAggAccumulatorSerializerCreateInstance() {
        RbAndAggFunction.AccumulatorSerializer ser =
                RbAndAggFunction.AccumulatorSerializer.INSTANCE;
        RbAndAggFunction.Accumulator acc = ser.createInstance();
        assertThat(acc.initialized).isFalse();
        assertThat(acc.value.isEmpty()).isTrue();
    }

    @Test
    void testAndAggAccumulatorSerializerIsNotImmutable() {
        assertThat(RbAndAggFunction.AccumulatorSerializer.INSTANCE.isImmutableType()).isFalse();
    }

    @Test
    void testAndAggAccumulatorSerializerGetLength() {
        assertThat(RbAndAggFunction.AccumulatorSerializer.INSTANCE.getLength()).isEqualTo(-1);
    }

    @Test
    void testAndAggAccumulatorSerializerCopy() {
        RbAndAggFunction.AccumulatorSerializer ser =
                RbAndAggFunction.AccumulatorSerializer.INSTANCE;
        RbAndAggFunction.Accumulator original = new RbAndAggFunction.Accumulator();
        original.initialized = true;
        original.value = RoaringBitmap.bitmapOf(1, 2, 3);

        RbAndAggFunction.Accumulator copy = ser.copy(original);
        assertThat(copy.initialized).isTrue();
        assertThat(copy.value).isEqualTo(original.value);
        // Verify deep copy
        copy.value.add(999);
        assertThat(original.value.contains(999)).isFalse();
    }

    @Test
    void testAndAggAccumulatorSerializerCopyWithReuse() {
        RbAndAggFunction.AccumulatorSerializer ser =
                RbAndAggFunction.AccumulatorSerializer.INSTANCE;
        RbAndAggFunction.Accumulator original = new RbAndAggFunction.Accumulator();
        original.initialized = true;
        original.value = RoaringBitmap.bitmapOf(10, 20);

        RbAndAggFunction.Accumulator reuse = new RbAndAggFunction.Accumulator();
        RbAndAggFunction.Accumulator copy = ser.copy(original, reuse);
        assertThat(copy.initialized).isTrue();
        assertThat(copy.value).isEqualTo(original.value);
    }

    @Test
    void testAndAggAccumulatorSerializerCopyStream() throws Exception {
        RbAndAggFunction.AccumulatorSerializer ser =
                RbAndAggFunction.AccumulatorSerializer.INSTANCE;
        RbAndAggFunction.Accumulator original = new RbAndAggFunction.Accumulator();
        original.initialized = true;
        original.value = RoaringBitmap.bitmapOf(5, 10, 15);

        DataOutputSerializer out = new DataOutputSerializer(256);
        ser.serialize(original, out);

        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        DataOutputSerializer copied = new DataOutputSerializer(256);
        ser.copy(in, copied);

        DataInputDeserializer copiedIn = new DataInputDeserializer(copied.getCopyOfBuffer());
        RbAndAggFunction.Accumulator restored = ser.deserialize(copiedIn);
        assertThat(restored.initialized).isTrue();
        assertThat(restored.value).isEqualTo(original.value);
    }

    @Test
    void testAndAggAccumulatorSerializerSnapshotNotNull() {
        assertThat(RbAndAggFunction.AccumulatorSerializer.INSTANCE.snapshotConfiguration())
                .isNotNull();
    }

    // -------------------------------------------------------------------------
    // RbXorAggFunction
    // -------------------------------------------------------------------------

    @Test
    void testXorAggBasic() throws IOException {
        RbXorAggFunction fn = new RbXorAggFunction();
        RbXorAggFunction.Accumulator acc = fn.createAccumulator();
        fn.accumulate(acc, BitmapUtils.toBytes(RoaringBitmap.bitmapOf(1, 2, 3)));
        fn.accumulate(acc, BitmapUtils.toBytes(RoaringBitmap.bitmapOf(2, 3, 4)));
        // XOR: elements in exactly one bitmap = {1, 4}
        byte[] result = fn.getValue(acc);
        assertThat(result).isNotNull();
        RoaringBitmap restored = BitmapUtils.fromBytes(result);
        assertThat(restored.getLongCardinality()).isEqualTo(2L);
        assertThat(restored.contains(1)).isTrue();
        assertThat(restored.contains(4)).isTrue();
        assertThat(restored.contains(2)).isFalse();
        assertThat(restored.contains(3)).isFalse();
    }

    @Test
    void testXorAggNullInputIgnored() throws IOException {
        RbXorAggFunction fn = new RbXorAggFunction();
        RbXorAggFunction.Accumulator acc = fn.createAccumulator();
        fn.accumulate(acc, null);
        fn.accumulate(acc, new byte[0]);
        fn.accumulate(acc, BitmapUtils.toBytes(RoaringBitmap.bitmapOf(1, 2)));
        assertThat(acc.nonNullCount).isEqualTo(1L);
        byte[] result = fn.getValue(acc);
        assertThat(result).isNotNull();
        assertThat(BitmapUtils.fromBytes(result).getLongCardinality()).isEqualTo(2L);
    }

    @Test
    void testXorAggNoInputReturnsNull() {
        RbXorAggFunction fn = new RbXorAggFunction();
        // nonNullCount == 0 → null
        assertThat(fn.getValue(fn.createAccumulator())).isNull();
    }

    @Test
    void testXorAggIdenticalBitmapsCancelToEmptyBitmap() throws IOException {
        // nonNullCount=2 after two accumulates, bitmap empty → empty bitmap, not null
        RbXorAggFunction fn = new RbXorAggFunction();
        RbXorAggFunction.Accumulator acc = fn.createAccumulator();
        fn.accumulate(acc, BitmapUtils.toBytes(RoaringBitmap.bitmapOf(1, 2, 3)));
        fn.accumulate(acc, BitmapUtils.toBytes(RoaringBitmap.bitmapOf(1, 2, 3)));
        assertThat(acc.nonNullCount).isEqualTo(2L);
        byte[] result = fn.getValue(acc);
        assertThat(result).isNotNull();
        assertThat(BitmapUtils.fromBytes(result).isEmpty()).isTrue();
    }

    @Test
    void testXorAggRetractRestoresNull() throws IOException {
        // accumulate(x) then retract(x): nonNullCount=0 → null
        // boolean initialized would incorrectly return empty bitmap here
        RbXorAggFunction fn = new RbXorAggFunction();
        RbXorAggFunction.Accumulator acc = fn.createAccumulator();
        byte[] bitmap = BitmapUtils.toBytes(RoaringBitmap.bitmapOf(1, 2, 3));
        fn.accumulate(acc, bitmap);
        assertThat(acc.nonNullCount).isEqualTo(1L);
        fn.retract(acc, bitmap);
        assertThat(acc.nonNullCount).isEqualTo(0L);
        assertThat(fn.getValue(acc)).isNull();
    }

    @Test
    void testXorAggRetractPartial() throws IOException {
        RbXorAggFunction fn = new RbXorAggFunction();
        RbXorAggFunction.Accumulator acc = fn.createAccumulator();
        byte[] a = BitmapUtils.toBytes(RoaringBitmap.bitmapOf(1, 2));
        byte[] b = BitmapUtils.toBytes(RoaringBitmap.bitmapOf(2, 3));
        fn.accumulate(acc, a);
        fn.accumulate(acc, b);
        // XOR so far: {1, 3}
        fn.retract(acc, b);
        // After retract b: XOR = {1, 2}, nonNullCount=1
        assertThat(acc.nonNullCount).isEqualTo(1L);
        RoaringBitmap result = BitmapUtils.fromBytes(fn.getValue(acc));
        assertThat(result.getLongCardinality()).isEqualTo(2L);
        assertThat(result.contains(1)).isTrue();
        assertThat(result.contains(2)).isTrue();
    }

    @Test
    void testXorAggRetractNullIgnored() throws IOException {
        RbXorAggFunction fn = new RbXorAggFunction();
        RbXorAggFunction.Accumulator acc = fn.createAccumulator();
        fn.accumulate(acc, BitmapUtils.toBytes(RoaringBitmap.bitmapOf(1, 2)));
        long countBefore = acc.nonNullCount;
        fn.retract(acc, null);
        fn.retract(acc, new byte[0]);
        assertThat(acc.nonNullCount).isEqualTo(countBefore);
    }

    @Test
    void testXorAggMerge() throws IOException {
        RbXorAggFunction fn = new RbXorAggFunction();
        RbXorAggFunction.Accumulator acc1 = fn.createAccumulator();
        fn.accumulate(acc1, BitmapUtils.toBytes(RoaringBitmap.bitmapOf(1, 2, 3)));
        RbXorAggFunction.Accumulator acc2 = fn.createAccumulator();
        fn.accumulate(acc2, BitmapUtils.toBytes(RoaringBitmap.bitmapOf(3, 4, 5)));
        fn.merge(acc1, Collections.singletonList(acc2));
        // XOR: {1,2,3} XOR {3,4,5} = {1,2,4,5}, nonNullCount=2
        assertThat(acc1.nonNullCount).isEqualTo(2L);
        RoaringBitmap result = BitmapUtils.fromBytes(fn.getValue(acc1));
        assertThat(result.getLongCardinality()).isEqualTo(4L);
        assertThat(result.contains(1)).isTrue();
        assertThat(result.contains(2)).isTrue();
        assertThat(result.contains(4)).isTrue();
        assertThat(result.contains(5)).isTrue();
        assertThat(result.contains(3)).isFalse();
    }

    @Test
    void testXorAggMergeEmptyPartialIgnored() throws IOException {
        RbXorAggFunction fn = new RbXorAggFunction();
        RbXorAggFunction.Accumulator acc1 = fn.createAccumulator();
        fn.accumulate(acc1, BitmapUtils.toBytes(RoaringBitmap.bitmapOf(1, 2)));
        RbXorAggFunction.Accumulator acc2 = fn.createAccumulator();
        fn.merge(acc1, Collections.singletonList(acc2));
        assertThat(acc1.nonNullCount).isEqualTo(1L);
        RoaringBitmap result = BitmapUtils.fromBytes(fn.getValue(acc1));
        assertThat(result.getLongCardinality()).isEqualTo(2L);
    }

    @Test
    void testXorAggThreeInputs() throws IOException {
        RbXorAggFunction fn = new RbXorAggFunction();
        RbXorAggFunction.Accumulator acc = fn.createAccumulator();
        fn.accumulate(acc, BitmapUtils.toBytes(RoaringBitmap.bitmapOf(1, 2)));
        fn.accumulate(acc, BitmapUtils.toBytes(RoaringBitmap.bitmapOf(2, 3)));
        fn.accumulate(acc, BitmapUtils.toBytes(RoaringBitmap.bitmapOf(3, 4)));
        // {1,2} XOR {2,3} XOR {3,4} = {1,4}
        RoaringBitmap result = BitmapUtils.fromBytes(fn.getValue(acc));
        assertThat(result.getLongCardinality()).isEqualTo(2L);
        assertThat(result.contains(1)).isTrue();
        assertThat(result.contains(4)).isTrue();
    }

    @Test
    void testXorAggResetAccumulator() throws IOException {
        RbXorAggFunction fn = new RbXorAggFunction();
        RbXorAggFunction.Accumulator acc = fn.createAccumulator();
        fn.accumulate(acc, BitmapUtils.toBytes(RoaringBitmap.bitmapOf(1, 2)));
        fn.resetAccumulator(acc);
        assertThat(acc.nonNullCount).isEqualTo(0L);
        assertThat(acc.value.isEmpty()).isTrue();
        assertThat(fn.getValue(acc)).isNull();
    }

    // -------------------------------------------------------------------------
    // RbXorAggFunction serializer and type info coverage
    // -------------------------------------------------------------------------

    @Test
    void testXorAggAccumulatorSerializerRoundTrip() throws Exception {
        RbXorAggFunction.AccumulatorSerializer ser =
                RbXorAggFunction.AccumulatorSerializer.INSTANCE;

        RbXorAggFunction.Accumulator original = new RbXorAggFunction.Accumulator();
        original.nonNullCount = 2L;
        original.value = RoaringBitmap.bitmapOf(10, 20, 30);

        DataOutputSerializer out = new DataOutputSerializer(256);
        ser.serialize(original, out);

        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        RbXorAggFunction.Accumulator restored = ser.deserialize(in);

        assertThat(restored.nonNullCount).isEqualTo(2L);
        assertThat(restored.value).isEqualTo(original.value);
    }

    @Test
    void testXorAggAccumulatorSerializerZeroCountRoundTrip() throws Exception {
        RbXorAggFunction.AccumulatorSerializer ser =
                RbXorAggFunction.AccumulatorSerializer.INSTANCE;

        RbXorAggFunction.Accumulator original = new RbXorAggFunction.Accumulator();
        // nonNullCount == 0: bitmap is not serialized

        DataOutputSerializer out = new DataOutputSerializer(64);
        ser.serialize(original, out);

        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        RbXorAggFunction.Accumulator restored = ser.deserialize(in);
        assertThat(restored.nonNullCount).isEqualTo(0L);
        assertThat(restored.value.isEmpty()).isTrue();
    }

    @Test
    void testXorAggAccumulatorSerializerDeserializeWithReuse() throws Exception {
        RbXorAggFunction.AccumulatorSerializer ser =
                RbXorAggFunction.AccumulatorSerializer.INSTANCE;

        RbXorAggFunction.Accumulator original = new RbXorAggFunction.Accumulator();
        original.nonNullCount = 1L;
        original.value = RoaringBitmap.bitmapOf(7, 8, 9);

        DataOutputSerializer out = new DataOutputSerializer(256);
        ser.serialize(original, out);

        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        RbXorAggFunction.Accumulator reuse = new RbXorAggFunction.Accumulator();
        RbXorAggFunction.Accumulator restored = ser.deserialize(reuse, in);

        assertThat(restored.nonNullCount).isEqualTo(1L);
        assertThat(restored.value).isEqualTo(original.value);
    }

    @Test
    void testXorAggAccumulatorSerializerCopyStream() throws Exception {
        RbXorAggFunction.AccumulatorSerializer ser =
                RbXorAggFunction.AccumulatorSerializer.INSTANCE;

        // Test copy stream with nonNullCount > 0
        RbXorAggFunction.Accumulator original = new RbXorAggFunction.Accumulator();
        original.nonNullCount = 3L;
        original.value = RoaringBitmap.bitmapOf(5, 10, 15);

        DataOutputSerializer out = new DataOutputSerializer(256);
        ser.serialize(original, out);
        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        DataOutputSerializer copied = new DataOutputSerializer(256);
        ser.copy(in, copied);

        DataInputDeserializer copiedIn = new DataInputDeserializer(copied.getCopyOfBuffer());
        RbXorAggFunction.Accumulator restored = ser.deserialize(copiedIn);
        assertThat(restored.nonNullCount).isEqualTo(3L);
        assertThat(restored.value).isEqualTo(original.value);
    }

    @Test
    void testXorAggAccumulatorSerializerCopyStreamZeroCount() throws Exception {
        RbXorAggFunction.AccumulatorSerializer ser =
                RbXorAggFunction.AccumulatorSerializer.INSTANCE;

        // Test copy stream with nonNullCount == 0 (no bitmap written)
        RbXorAggFunction.Accumulator original = new RbXorAggFunction.Accumulator();

        DataOutputSerializer out = new DataOutputSerializer(64);
        ser.serialize(original, out);
        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        DataOutputSerializer copied = new DataOutputSerializer(64);
        ser.copy(in, copied);

        DataInputDeserializer copiedIn = new DataInputDeserializer(copied.getCopyOfBuffer());
        RbXorAggFunction.Accumulator restored = ser.deserialize(copiedIn);
        assertThat(restored.nonNullCount).isEqualTo(0L);
    }

    @Test
    void testXorAggAccumulatorTypeInfoProperties() {
        RbXorAggFunction.AccumulatorTypeInfo info = RbXorAggFunction.AccumulatorTypeInfo.INSTANCE;
        assertThat(info.getTypeClass()).isEqualTo(RbXorAggFunction.Accumulator.class);
        assertThat(info.isBasicType()).isFalse();
        assertThat(info.isTupleType()).isFalse();
        assertThat(info.getArity()).isEqualTo(1);
        assertThat(info.getTotalFields()).isEqualTo(1);
        assertThat(info.isKeyType()).isFalse();
        assertThat(info.toString()).isEqualTo("RbXorAccumulatorTypeInfo");
        assertThat(info.hashCode()).isNotZero();
        assertThat(info.equals(info)).isTrue();
        assertThat(info.equals("other")).isFalse();
        assertThat(info.canEqual(info)).isTrue();
        assertThat(info.canEqual("other")).isFalse();
    }

    @Test
    void testXorAggAccumulatorTypeInfoCreateSerializer() {
        TypeSerializer<RbXorAggFunction.Accumulator> s =
                RbXorAggFunction.AccumulatorTypeInfo.INSTANCE.createSerializer(
                        new ExecutionConfig());
        assertThat(s).isInstanceOf(RbXorAggFunction.AccumulatorSerializer.class);
    }

    @Test
    void testXorAggAccumulatorSerializerCreateInstance() {
        RbXorAggFunction.Accumulator acc =
                RbXorAggFunction.AccumulatorSerializer.INSTANCE.createInstance();
        assertThat(acc.nonNullCount).isEqualTo(0L);
        assertThat(acc.value.isEmpty()).isTrue();
    }

    @Test
    void testXorAggAccumulatorSerializerIsNotImmutable() {
        assertThat(RbXorAggFunction.AccumulatorSerializer.INSTANCE.isImmutableType()).isFalse();
    }

    @Test
    void testXorAggAccumulatorSerializerGetLength() {
        assertThat(RbXorAggFunction.AccumulatorSerializer.INSTANCE.getLength()).isEqualTo(-1);
    }

    @Test
    void testXorAggAccumulatorSerializerCopy() {
        RbXorAggFunction.AccumulatorSerializer ser =
                RbXorAggFunction.AccumulatorSerializer.INSTANCE;
        RbXorAggFunction.Accumulator original = new RbXorAggFunction.Accumulator();
        original.nonNullCount = 1L;
        original.value = RoaringBitmap.bitmapOf(1, 2, 3);

        RbXorAggFunction.Accumulator copy = ser.copy(original);
        assertThat(copy.nonNullCount).isEqualTo(1L);
        assertThat(copy.value).isEqualTo(original.value);
        // verify deep copy
        copy.value.add(999);
        assertThat(original.value.contains(999)).isFalse();
    }

    @Test
    void testXorAggAccumulatorSerializerCopyWithReuse() {
        RbXorAggFunction.AccumulatorSerializer ser =
                RbXorAggFunction.AccumulatorSerializer.INSTANCE;
        RbXorAggFunction.Accumulator original = new RbXorAggFunction.Accumulator();
        original.nonNullCount = 1L;
        original.value = RoaringBitmap.bitmapOf(10, 20);

        RbXorAggFunction.Accumulator reuse = new RbXorAggFunction.Accumulator();
        RbXorAggFunction.Accumulator copy = ser.copy(original, reuse);
        assertThat(copy.nonNullCount).isEqualTo(1L);
        assertThat(copy.value).isEqualTo(original.value);
    }

    @Test
    void testXorAggAccumulatorSerializerSnapshotNotNull() {
        assertThat(RbXorAggFunction.AccumulatorSerializer.INSTANCE.snapshotConfiguration())
                .isNotNull();
    }

    @Test
    void testXorAggAccumulatorEqualsAndHashCode() {
        RbXorAggFunction.Accumulator acc1 = new RbXorAggFunction.Accumulator();
        acc1.nonNullCount = 2L;
        acc1.value = RoaringBitmap.bitmapOf(1, 2, 3);

        RbXorAggFunction.Accumulator acc2 = new RbXorAggFunction.Accumulator();
        acc2.nonNullCount = 2L;
        acc2.value = RoaringBitmap.bitmapOf(1, 2, 3);

        RbXorAggFunction.Accumulator differentCount = new RbXorAggFunction.Accumulator();
        differentCount.nonNullCount = 3L;
        differentCount.value = RoaringBitmap.bitmapOf(1, 2, 3);

        RbXorAggFunction.Accumulator differentValue = new RbXorAggFunction.Accumulator();
        differentValue.nonNullCount = 2L;
        differentValue.value = RoaringBitmap.bitmapOf(9, 9, 9);

        // same reference
        assertThat(acc1.equals(acc1)).isTrue();
        // equal fields, different instances
        assertThat(acc1).isEqualTo(acc2);
        assertThat(acc1.hashCode()).isEqualTo(acc2.hashCode());
        // different nonNullCount
        assertThat(acc1).isNotEqualTo(differentCount);
        // different value
        assertThat(acc1).isNotEqualTo(differentValue);
        // null and wrong-class checks
        assertThat(acc1.equals(null)).isFalse();
        assertThat(acc1.equals("not an accumulator")).isFalse();
    }
}
