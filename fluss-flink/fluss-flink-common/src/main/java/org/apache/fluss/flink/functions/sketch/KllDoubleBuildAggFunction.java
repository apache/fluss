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

import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.AggregateFunction;

/** Builds serialized KLL doubles sketches from DOUBLE values on append-only inputs. */
@FunctionHint(
        accumulator =
                @DataTypeHint(
                        value = "RAW",
                        bridgedTo = KllDoublesSketch.class,
                        rawSerializer = KllDoublesSerializer.class))
public class KllDoubleBuildAggFunction extends AggregateFunction<byte[], KllDoublesSketch> {
    /** Sketch updates cannot be retracted; only append-only inputs are supported. */
    public void retract(KllDoublesSketch acc, Double value) {
        throw new UnsupportedOperationException("Sketch aggregation requires append-only inputs.");
    }

    @Override
    public KllDoublesSketch createAccumulator() {
        return KllDoublesSketch.newHeapInstance();
    }

    /** Adds non-null input to the sketch. */
    public void accumulate(KllDoublesSketch acc, Double value) {
        if (value != null) {
            acc.update(value);
        }
    }

    /** Combines partial sketches for distributed aggregation. */
    public void merge(KllDoublesSketch acc, Iterable<KllDoublesSketch> others) {
        for (KllDoublesSketch other : others) {
            if (other != null) {
                acc.merge(other);
            }
        }
    }

    /** Resets the accumulator. */
    public void resetAccumulator(KllDoublesSketch acc) {
        acc.reset();
    }

    @Override
    public byte[] getValue(KllDoublesSketch acc) {
        return acc.isEmpty() ? null : acc.toByteArray();
    }

    @Override
    public TypeInformation<KllDoublesSketch> getAccumulatorType() {
        return KllDoublesTypeInfo.INSTANCE;
    }
}
