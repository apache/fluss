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
 * <p>Note: there is no server-side {@code FieldRoaringBitmapXorAgg} counterpart. This function
 * executes entirely in Flink. Users should be aware that combining it with {@code
 * table.merge-engine=aggregation} may produce unexpected results during server-side compaction.
 *
 * <p>Null and empty inputs are ignored. Returns {@code null} when all inputs are null or the result
 * fully cancels to an empty bitmap.
 */
public class RbXorAggFunction extends AbstractRbAggFunction {

    /**
     * XORs the input bitmap into the accumulator.
     *
     * @param acc the running bitmap accumulator
     * @param bitmapBytes serialized RoaringBitmap bytes; null and empty arrays are ignored
     */
    public void accumulate(RoaringBitmap acc, @Nullable byte[] bitmapBytes) throws IOException {
        if (bitmapBytes == null || bitmapBytes.length == 0) {
            return;
        }
        acc.xor(BitmapUtils.fromBytes(bitmapBytes));
    }

    /**
     * Retraction is not supported for bitmap XOR aggregation.
     *
     * @throws UnsupportedOperationException always
     */
    public void retract(RoaringBitmap acc, @Nullable byte[] bitmapBytes) {
        throw new UnsupportedOperationException(
                "rb_xor_agg does not support retraction. " + "Use it only on append-only streams.");
    }

    @Override
    public void merge(RoaringBitmap acc, Iterable<RoaringBitmap> it) {
        for (RoaringBitmap other : it) {
            if (other != null) {
                acc.xor(other);
            }
        }
    }
}
