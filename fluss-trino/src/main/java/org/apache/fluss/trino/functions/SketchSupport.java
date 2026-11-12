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

import java.lang.foreign.MemorySegment;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;

/** Shared sketch byte-format and aggregate-state operations. */
final class SketchSupport {
    private SketchSupport() {}

    static long estimatedSize(Object sketch) {
        if (sketch == null) {
            return 0;
        }
        if (sketch instanceof HllUnion) {
            return 256L + 2L * ((HllUnion) sketch).getUpdatableSerializationBytes();
        }
        return 256L + 2L * ((KllDoublesSketch) sketch).getSerializedSizeBytes();
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
