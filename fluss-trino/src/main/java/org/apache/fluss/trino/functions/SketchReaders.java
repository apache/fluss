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

import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.kll.KllDoublesSketch;

import java.lang.foreign.MemorySegment;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

/** Readers for Apache DataSketches bytes, independent of Trino's native sketch types. */
public final class SketchReaders {
    private SketchReaders() {}

    /** Returns null for empty sketches; null arguments are handled by Trino. */
    @ScalarFunction("fluss_hll_estimate")
    @SqlNullable
    @SqlType(StandardTypes.DOUBLE)
    public static Double hllEstimate(@SqlType(StandardTypes.VARBINARY) Slice bytes) {
        try {
            HllSketch sketch = HllSketch.heapify(bytes.getBytes());
            return sketch.isEmpty() ? null : sketch.getEstimate();
        } catch (RuntimeException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Invalid HLL sketch bytes", e);
        }
    }

    /** Returns a quantile at a rank in [0, 1], or null for an empty sketch. */
    @ScalarFunction("fluss_kll_double_quantile")
    @SqlNullable
    @SqlType(StandardTypes.DOUBLE)
    public static Double kllQuantile(
            @SqlType(StandardTypes.VARBINARY) Slice bytes,
            @SqlType(StandardTypes.DOUBLE) double rank) {
        if (!(rank >= 0.0 && rank <= 1.0)) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Quantile rank must be in [0, 1]");
        }
        try {
            KllDoublesSketch sketch =
                    KllDoublesSketch.heapify(MemorySegment.ofArray(bytes.getBytes()));
            return sketch.isEmpty() ? null : sketch.getQuantile(rank);
        } catch (RuntimeException e) {
            throw new TrinoException(
                    INVALID_FUNCTION_ARGUMENT, "Invalid KLL doubles sketch bytes", e);
        }
    }
}
