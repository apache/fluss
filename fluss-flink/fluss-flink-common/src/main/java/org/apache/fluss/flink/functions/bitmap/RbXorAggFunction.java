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

import org.apache.fluss.exception.FlussRuntimeException;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.AggregateFunction;
import org.roaringbitmap.RoaringBitmap;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * {@code rb_xor_agg(bitmap BYTES) -> BYTES}
 *
 * <p>Aggregates multiple serialized {@link RoaringBitmap} values using bitwise XOR across rows.
 * Returns elements that appear in an odd number of input bitmaps — useful for change detection and
 * symmetric difference analysis.
 *
 * <p>Returns an empty serialized bitmap when all inputs cancel (e.g., two identical bitmaps XOR to
 * empty). Returns {@code null} only when no non-null input was accumulated.
 *
 * <p>Note: there is no server-side {@code FieldRoaringBitmapXorAgg} counterpart. This function
 * executes entirely in Flink. Users should be aware that combining it with {@code
 * table.merge-engine=aggregation} may produce unexpected results during server-side compaction. Use
 * only on append-only streams.
 */
@FunctionHint(
        accumulator = @DataTypeHint(value = "RAW", bridgedTo = RbXorAggFunction.Accumulator.class))
public class RbXorAggFunction extends AggregateFunction<byte[], RbXorAggFunction.Accumulator> {

    /** Accumulator that tracks whether any non-null input has been seen. */
    public static final class Accumulator {
        public boolean initialized = false;
        public RoaringBitmap value = new RoaringBitmap();
    }

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator();
    }

    /**
     * XORs the input bitmap into the accumulator.
     *
     * @param acc the running accumulator
     * @param bitmapBytes serialized RoaringBitmap bytes; null and empty arrays are ignored
     */
    public void accumulate(Accumulator acc, @Nullable byte[] bitmapBytes) throws IOException {
        if (bitmapBytes == null || bitmapBytes.length == 0) {
            return;
        }
        acc.value.xor(BitmapUtils.fromBytes(bitmapBytes));
        acc.initialized = true;
    }

    /**
     * XOR is self-inverse, so retraction applies the same XOR operation.
     *
     * @param acc the running accumulator
     * @param bitmapBytes serialized RoaringBitmap bytes; null and empty arrays are ignored
     */
    public void retract(Accumulator acc, @Nullable byte[] bitmapBytes) throws IOException {
        if (bitmapBytes == null || bitmapBytes.length == 0) {
            return;
        }
        acc.value.xor(BitmapUtils.fromBytes(bitmapBytes));
        acc.initialized = true;
    }

    /** Merges partial accumulators using XOR, required for two-phase aggregation. */
    public void merge(Accumulator acc, Iterable<Accumulator> it) {
        for (Accumulator other : it) {
            if (other.initialized) {
                acc.value.xor(other.value);
                acc.initialized = true;
            }
        }
    }

    public void resetAccumulator(Accumulator acc) {
        acc.initialized = false;
        acc.value.clear();
    }

    @Override
    @Nullable
    public byte[] getValue(Accumulator acc) {
        if (!acc.initialized) {
            return null;
        }
        try {
            return BitmapUtils.toBytes(acc.value);
        } catch (IOException e) {
            throw new FlussRuntimeException("Failed to serialize rb_xor_agg accumulator.", e);
        }
    }

    @Override
    public TypeInformation<Accumulator> getAccumulatorType() {
        return new RbXorAggAccumulatorTypeInfo();
    }
}
