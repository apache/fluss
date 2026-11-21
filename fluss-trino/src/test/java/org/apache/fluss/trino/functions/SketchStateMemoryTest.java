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

package org.apache.fluss.trino.functions;

import io.airlift.slice.Slices;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.GroupedAccumulatorState;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.kll.KllSketch;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static org.assertj.core.api.Assertions.assertThat;

/** Checks retained KLL capacity independently of the compact serialization size. */
class SketchStateMemoryTest {
    @Test
    void testSparseKllMemoryAccounting() throws Exception {
        SketchStateFactory factory = new SketchStateFactory();
        SketchState single = factory.createSingleState();
        assertThat(SketchSupport.estimatedSize(null)).isZero();
        KllDoublesSketch sketch = SketchSupport.kll(single);
        assertThat(SketchSupport.estimatedSize(sketch))
                .isGreaterThanOrEqualTo(arrayPayload(sketch));
        KllDoubleBuildAggregation.input(single, 1.0);
        assertThat(single.getEstimatedSize()).isGreaterThanOrEqualTo(arrayPayload(sketch));

        SketchState grouped = factory.createGroupedState();
        GroupedAccumulatorState groups = (GroupedAccumulatorState) grouped;
        int count = 10_000;
        groups.ensureCapacity(count);
        long emptySize = grouped.getEstimatedSize();
        for (int group = 0; group < count; group++) {
            groups.setGroupId(group);
            KllDoubleBuildAggregation.input(grouped, (double) group);
        }
        assertThat(grouped.getEstimatedSize() - emptySize)
                .isGreaterThanOrEqualTo(count * arrayPayload(sketch));
        assertThat(grouped.getEstimatedSize())
                .isEqualTo(emptySize + count * SketchSupport.estimatedSize(sketch));
    }

    @Test
    void testKllMemoryAccountingAfterGrowthRestoreAndMerge() throws Exception {
        SketchStateFactory factory = new SketchStateFactory();
        SketchStateSerializer serializer = new SketchStateSerializer();
        for (int k : new int[] {8, 200, 1024}) {
            SketchState source = factory.createSingleState();
            KllDoublesSketch sketch = KllDoublesSketch.newHeapInstance(k);
            source.setSketch(sketch);
            long initialSize = source.getEstimatedSize();
            SketchState grouped = factory.createGroupedState();
            GroupedAccumulatorState groups = (GroupedAccumulatorState) grouped;
            groups.ensureCapacity(2);
            long emptySize = grouped.getEstimatedSize();
            groups.setGroupId(1);
            KllDoubleBuildAggregation.input(grouped, -1.0);
            long otherSize = SketchSupport.estimatedSize(grouped.getSketch());
            groups.setGroupId(0);
            KllDoubleBuildAggregation.input(grouped, -2.0);

            for (int count : new int[] {0, 1, 2, 200, 201, 2000, 20_000, 100_000}) {
                while (sketch.getN() < count) {
                    KllDoubleBuildAggregation.input(source, (double) sketch.getN());
                }
                assertThat(SketchSupport.estimatedSize(sketch))
                        .as("k=%s, N=%s", k, count)
                        .isGreaterThanOrEqualTo(arrayPayload(sketch));
                BlockBuilder out = VARBINARY.createBlockBuilder(null, 1);
                serializer.serialize(source, out);
                serializer.deserialize(out.build(), 0, grouped);
                KllDoublesSketch restored = (KllDoublesSketch) grouped.getSketch();
                assertThat(restored.getN()).isEqualTo(count);
                assertThat(SketchSupport.estimatedSize(restored))
                        .isGreaterThanOrEqualTo(arrayPayload(restored));
                assertThat(grouped.getEstimatedSize())
                        .isEqualTo(emptySize + otherSize + SketchSupport.estimatedSize(restored));
            }
            assertThat(source.getEstimatedSize()).isGreaterThan(initialSize);

            KllDoublesSketch partial = KllDoublesSketch.newHeapInstance(k);
            for (int value = 0; value < 100_000; value++) {
                partial.update((double) value);
            }
            KllDoubleMergeAggregation.input(grouped, Slices.wrappedBuffer(partial.toByteArray()));
            KllDoubleBuildAggregation.combine(grouped, source);
            KllDoublesSketch merged = (KllDoublesSketch) grouped.getSketch();
            assertThat(merged.getN()).isEqualTo(300_000);
            assertThat(SketchSupport.estimatedSize(merged))
                    .isGreaterThanOrEqualTo(arrayPayload(merged));
            assertThat(grouped.getEstimatedSize())
                    .isEqualTo(emptySize + otherSize + SketchSupport.estimatedSize(merged));

            BlockBuilder empty = VARBINARY.createBlockBuilder(null, 1);
            serializer.serialize(factory.createSingleState(), empty);
            serializer.deserialize(empty.build(), 0, grouped);
            assertThat(grouped.getEstimatedSize()).isEqualTo(emptySize + otherSize);
        }
    }

    @Test
    void testKllSizeBoundsAcrossCountRanges() throws Exception {
        for (int k : new int[] {8, 200, 1024}) {
            KllDoublesSketch sketch = KllDoublesSketch.newHeapInstance(k);
            Field n = sketch.getClass().getDeclaredField("n");
            n.setAccessible(true);
            long previous = SketchSupport.estimatedSize(sketch);
            for (int bit = 0; bit < Long.SIZE - 1; bit++) {
                for (long count : new long[] {1L << bit, -1L >>> (Long.SIZE - bit - 1)}) {
                    n.setLong(sketch, count);
                    long size = SketchSupport.estimatedSize(sketch);
                    assertThat(size).isGreaterThanOrEqualTo(previous);
                    assertThat(size).isGreaterThanOrEqualTo(arrayPayload(sketch));
                    assertThat(sketch.getN()).isEqualTo(count);
                    previous = size;
                }
            }
        }
    }

    private static long arrayPayload(KllDoublesSketch sketch) throws Exception {
        // Read actual capacity so compact bytes cannot make the memory check pass incorrectly.
        Field items = sketch.getClass().getDeclaredField("doubleItems");
        items.setAccessible(true);
        Field levels = KllSketch.class.getDeclaredField("levelsArr");
        levels.setAccessible(true);
        return (long) Double.BYTES * ((double[]) items.get(sketch)).length
                + (long) Integer.BYTES * ((int[]) levels.get(sketch)).length;
    }
}
