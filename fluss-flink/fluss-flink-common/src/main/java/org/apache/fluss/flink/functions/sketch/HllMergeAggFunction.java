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

import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.Union;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.AggregateFunction;

/** Unions serialized HLL sketches from append-only inputs at lgK 12 or lower. */
@FunctionHint(
        accumulator =
                @DataTypeHint(
                        value = "RAW",
                        bridgedTo = Union.class,
                        rawSerializer = HllUnionSerializer.class))
public class HllMergeAggFunction extends AggregateFunction<byte[], Union> {
    /** Sketch updates cannot be retracted; only append-only inputs are supported. */
    public void retract(Union acc, byte[] value) {
        throw new UnsupportedOperationException("Sketch aggregation requires append-only inputs.");
    }

    @Override
    public Union createAccumulator() {
        return new Union(12);
    }

    /** Merges a serialized sketch, ignoring nulls. */
    public void accumulate(Union acc, byte[] bytes) {
        if (bytes != null) {
            acc.update(HllSketch.heapify(bytes));
        }
    }

    /** Combines partial accumulators. */
    public void merge(Union acc, Iterable<Union> others) {
        for (Union other : others) {
            if (other != null) {
                acc.update(other.getResult());
            }
        }
    }

    /** Resets the accumulator. */
    public void resetAccumulator(Union acc) {
        acc.reset();
    }

    @Override
    public byte[] getValue(Union acc) {
        return acc.isEmpty() ? null : acc.toCompactByteArray();
    }

    @Override
    public TypeInformation<Union> getAccumulatorType() {
        return HllUnionTypeInfo.INSTANCE;
    }
}
