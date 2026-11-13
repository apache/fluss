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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.AggregateFunction;

/** Builds serialized HLL sketches from append-only BIGINT or STRING inputs. */
@FunctionHint(
        accumulator =
                @DataTypeHint(
                        value = "RAW",
                        bridgedTo = Union.class,
                        rawSerializer = HllUnionSerializer.class))
public class HllBuildAggFunction extends AggregateFunction<byte[], Union> {
    /** Sketch updates cannot be retracted; only append-only inputs are supported. */
    public void retract(Union acc, Long value) {
        throw new UnsupportedOperationException("Sketch aggregation requires append-only inputs.");
    }

    /** String sketch updates cannot be retracted. */
    public void retract(Union acc, String value) {
        throw new UnsupportedOperationException("Sketch aggregation requires append-only inputs.");
    }

    @Override
    public Union createAccumulator() {
        return new Union(12);
    }

    /** Adds a BIGINT value, ignoring nulls. */
    public void accumulate(Union acc, Long value) {
        if (value != null) {
            acc.update(value.longValue());
        }
    }

    /** Adds a STRING value, ignoring nulls. */
    public void accumulate(Union acc, String value) {
        if (value != null) {
            acc.update(value);
        }
    }

    /** Combines partial sketches for distributed aggregation. */
    public void merge(Union acc, Iterable<Union> others) {
        for (Union other : others) {
            if (other != null) {
                acc.update(other.getResult());
            }
        }
    }

    /** Resets the accumulator without changing its precision. */
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
