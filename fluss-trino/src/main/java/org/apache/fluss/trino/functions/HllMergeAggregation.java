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
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import org.apache.datasketches.hll.HllSketch;

/** Merges serialized DataSketches HLL sketches. */
@AggregationFunction("fluss_hll_merge_agg")
public final class HllMergeAggregation {
    private HllMergeAggregation() {}

    /** Accumulates a non-null serialized sketch. */
    @InputFunction
    public static void input(
            @AggregationState SketchState state, @SqlType(StandardTypes.VARBINARY) Slice value) {
        SketchSupport.hll(state);
        SketchSupport.mutate(
                state, () -> SketchSupport.hll(state).update(HllSketch.heapify(value.getBytes())));
    }

    /** Combines independent partial aggregates. */
    @CombineFunction
    public static void combine(
            @AggregationState SketchState state, @AggregationState SketchState other) {
        SketchSupport.combine(state, other);
    }

    /** Returns native sketch bytes, or null for an empty aggregate. */
    @OutputFunction(StandardTypes.VARBINARY)
    public static void output(@AggregationState SketchState state, BlockBuilder out) {
        SketchSupport.output(state, out);
    }
}
