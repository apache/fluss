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
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import org.apache.datasketches.hll.HllUnion;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.kll.KllSketch;

import java.lang.foreign.MemorySegment;
import java.util.concurrent.atomic.AtomicLongArray;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;

/** Shared sketch byte-format and aggregate-state operations. */
final class SketchSupport {
    private static final AtomicLongArray DEFAULT_KLL_SIZES = new AtomicLongArray(Long.SIZE);

    private SketchSupport() {}

    static long estimatedSize(Object sketch) {
        if (sketch == null) {
            return 0;
        }
        if (sketch instanceof HllUnion) {
            return 256L + 2L * ((HllUnion) sketch).getUpdatableSerializationBytes();
        }
        KllDoublesSketch kll = (KllDoublesSketch) sketch;
        if (kll.getK() != KllSketch.DEFAULT_K) {
            return kllSizeBound(kll.getK(), kll.getN());
        }
        int bucket = Long.SIZE - Long.numberOfLeadingZeros(kll.getN());
        long size = DEFAULT_KLL_SIZES.get(bucket);
        if (size == 0) {
            // Cache an upper bound per count range, avoiding sizing allocations on every update.
            long maxN = bucket == 0 ? 0 : -1L >>> (Long.SIZE - bucket);
            size = kllSizeBound(kll.getK(), maxN);
            DEFAULT_KLL_SIZES.compareAndSet(bucket, 0, size);
        }
        return size;
    }

    private static long kllSizeBound(int k, long n) {
        if (n > (1L << 50)) {
            // The sizing API overflows for large N. Across at most 64 levels, geometric
            // capacities sum to less than 3*k plus the minimum capacity (8) per level.
            return 256L
                    + Double.BYTES * (3L * k + 8L * Long.SIZE)
                    + Integer.BYTES * (Long.SIZE + 1L);
        }
        // Updatable size includes unused capacity omitted by compact serialization.
        return 256L
                + KllSketch.getMaxSerializedSizeBytes(
                        k, n, KllSketch.SketchType.KLL_DOUBLES_SKETCH, true);
    }

    static byte[] bytes(Object sketch) {
        return sketch instanceof HllUnion
                ? ((HllUnion) sketch).toCompactByteArray()
                : ((KllDoublesSketch) sketch).toByteArray();
    }

    static Object copy(Object sketch) {
        return sketch instanceof HllUnion
                ? HllUnion.heapify(bytes(sketch))
                : KllDoublesSketch.heapify(MemorySegment.ofArray(bytes(sketch)));
    }

    static HllUnion hll(SketchState state) {
        if (state.getSketch() == null) {
            state.setSketch(new HllUnion(12));
        }
        return (HllUnion) state.getSketch();
    }

    static KllDoublesSketch kll(SketchState state) {
        if (state.getSketch() == null) {
            state.setSketch(KllDoublesSketch.newHeapInstance());
        }
        return (KllDoublesSketch) state.getSketch();
    }

    static void mutate(SketchState state, Runnable update) {
        long before = estimatedSize(state.getSketch());
        try {
            update.run();
        } catch (RuntimeException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Invalid DataSketches input", e);
        } finally {
            state.addMemoryUsage(estimatedSize(state.getSketch()) - before);
        }
    }

    static void combine(SketchState state, SketchState other) {
        if (other.getSketch() == null) {
            return;
        }
        if (state.getSketch() == null) {
            state.setSketch(copy(other.getSketch()));
            return;
        }
        mutate(
                state,
                () -> {
                    if (state.getSketch() instanceof HllUnion) {
                        ((HllUnion) state.getSketch())
                                .update(((HllUnion) other.getSketch()).getResult());
                    } else {
                        ((KllDoublesSketch) state.getSketch())
                                .merge((KllDoublesSketch) other.getSketch());
                    }
                });
    }

    static void output(SketchState state, BlockBuilder out) {
        Object sketch = state.getSketch();
        if (sketch == null
                || (sketch instanceof HllUnion && ((HllUnion) sketch).isEmpty())
                || (sketch instanceof KllDoublesSketch && ((KllDoublesSketch) sketch).isEmpty())) {
            out.appendNull();
        } else {
            VARBINARY.writeSlice(out, Slices.wrappedBuffer(bytes(sketch)));
        }
    }
}
