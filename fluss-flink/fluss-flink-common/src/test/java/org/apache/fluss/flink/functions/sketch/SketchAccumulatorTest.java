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

package org.apache.fluss.flink.functions.sketch;

import org.apache.datasketches.hll.Union;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Covers accumulator restore/copy contracts, type metadata and append-only restrictions. */
class SketchAccumulatorTest {
    @Test
    void testSerializerContracts() throws Exception {
        Union hll = new Union(12);
        hll.update(1L);
        checkSerializer(HllUnionSerializer.INSTANCE, hll);
        KllDoublesSketch kll = KllDoublesSketch.newHeapInstance();
        kll.update(42.0);
        checkSerializer(KllDoublesSerializer.INSTANCE, kll);
    }

    private <T> void checkSerializer(TypeSerializer<T> serializer, T value) throws Exception {
        assertThat(serializer.isImmutableType()).isFalse();
        assertThat(serializer.getLength()).isEqualTo(-1);
        assertThat(serializer.createInstance()).isNotNull();
        T copy = serializer.copy(value);
        assertThat(copy).isNotSameAs(value);
        assertThat(bytes(serializer, copy)).isEqualTo(bytes(serializer, value));
        assertThat(bytes(serializer, serializer.copy(value, serializer.createInstance())))
                .isEqualTo(bytes(serializer, value));
        byte[] original = bytes(serializer, value);
        T restored =
                serializer.deserialize(
                        serializer.createInstance(), new DataInputDeserializer(original));
        assertThat(bytes(serializer, restored)).isEqualTo(original);
        DataOutputSerializer copied = new DataOutputSerializer(64);
        serializer.copy(new DataInputDeserializer(original), copied);
        assertThat(copied.getCopyOfBuffer()).isEqualTo(original);

        TypeSerializerSnapshot<T> snapshot = serializer.snapshotConfiguration();
        DataOutputSerializer snapshotBytes = new DataOutputSerializer(64);
        snapshot.writeSnapshot(snapshotBytes);
        TypeSerializerSnapshot<T> loaded = serializer.snapshotConfiguration();
        loaded.readSnapshot(
                snapshot.getCurrentVersion(),
                new DataInputDeserializer(snapshotBytes.getCopyOfBuffer()),
                getClass().getClassLoader());
        assertThat(bytes(loaded.restoreSerializer(), value)).isEqualTo(original);
        assertThat(loaded.resolveSchemaCompatibility(serializer).isCompatibleAsIs()).isTrue();

        DataOutputSerializer negative = new DataOutputSerializer(4);
        negative.writeInt(-1);
        assertThatThrownBy(
                        () ->
                                serializer.deserialize(
                                        new DataInputDeserializer(negative.getCopyOfBuffer())))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Negative sketch length");
        assertThatThrownBy(() -> serializer.deserialize(new DataInputDeserializer(new byte[0])))
                .isInstanceOf(IOException.class);
    }

    private <T> byte[] bytes(TypeSerializer<T> serializer, T value) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(64);
        serializer.serialize(value, out);
        return out.getCopyOfBuffer();
    }

    @Test
    void testTypeMetadata() {
        for (TypeInformation<?> info :
                Arrays.asList(HllUnionTypeInfo.INSTANCE, KllDoublesTypeInfo.INSTANCE)) {
            assertThat(info.isBasicType()).isFalse();
            assertThat(info.isTupleType()).isFalse();
            assertThat(info.isKeyType()).isFalse();
            assertThat(info.getArity()).isEqualTo(1);
            assertThat(info.getTotalFields()).isEqualTo(1);
            assertThat(info.getTypeClass()).isNotNull();
            assertThat(info.createSerializer(new ExecutionConfig())).isNotNull();
            assertThat(info.canEqual(info)).isTrue();
            assertThat(info.canEqual(new Object())).isFalse();
            assertThat(info.equals(info)).isTrue();
            assertThat(info.equals(null)).isFalse();
            assertThat(info.hashCode()).isEqualTo(info.hashCode());
            assertThat(info.toString()).isNotEmpty();
        }
    }

    @Test
    void testResetAndRetractionContracts() {
        HllBuildAggFunction hllBuild = new HllBuildAggFunction();
        Union hll = hllBuild.createAccumulator();
        hllBuild.accumulate(hll, "user");
        hllBuild.accumulate(hll, (String) null);
        hllBuild.accumulate(hll, (Long) null);
        hllBuild.merge(hll, Collections.singletonList(hllBuild.createAccumulator()));
        assertThat(hllBuild.getAccumulatorType()).isEqualTo(HllUnionTypeInfo.INSTANCE);
        assertThatThrownBy(() -> hllBuild.retract(hll, 1L))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> hllBuild.retract(hll, "user"))
                .isInstanceOf(UnsupportedOperationException.class);

        HllMergeAggFunction hllMerge = new HllMergeAggFunction();
        hllMerge.accumulate(hll, null);
        assertThat(hllMerge.getAccumulatorType()).isEqualTo(HllUnionTypeInfo.INSTANCE);
        assertThatThrownBy(() -> hllMerge.retract(hll, new byte[0]))
                .isInstanceOf(UnsupportedOperationException.class);
        hllMerge.resetAccumulator(hll);
        assertThat(hllMerge.getValue(hll)).isNull();

        KllDoubleBuildAggFunction kllBuild = new KllDoubleBuildAggFunction();
        KllDoublesSketch kll = kllBuild.createAccumulator();
        kllBuild.accumulate(kll, null);
        kllBuild.merge(kll, Collections.singletonList(kllBuild.createAccumulator()));
        assertThat(kllBuild.getAccumulatorType()).isEqualTo(KllDoublesTypeInfo.INSTANCE);
        assertThatThrownBy(() -> kllBuild.retract(kll, 1.0))
                .isInstanceOf(UnsupportedOperationException.class);
        kllBuild.resetAccumulator(kll);
        assertThat(kllBuild.getValue(kll)).isNull();

        KllDoubleMergeAggFunction kllMerge = new KllDoubleMergeAggFunction();
        kllMerge.accumulate(kll, null);
        assertThat(kllMerge.getAccumulatorType()).isEqualTo(KllDoublesTypeInfo.INSTANCE);
        assertThatThrownBy(() -> kllMerge.retract(kll, new byte[0]))
                .isInstanceOf(UnsupportedOperationException.class);
        kllMerge.resetAccumulator(kll);
        assertThat(kllMerge.getValue(kll)).isNull();
    }
}
