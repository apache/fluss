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
 * {@code rb_or_agg(bitmap BYTES) -> BYTES}
 *
 * <p>Aggregates multiple serialized {@link RoaringBitmap} values using bitwise OR (union). Used in
 * roll-up aggregation where per-group bitmaps need to be merged into a coarser granularity.
 *
 * <p>Null and empty inputs are ignored. Returns {@code null} when all inputs are null.
 */
public class RbOrAggFunction extends AbstractRbAggFunction {

    /**
     * Unions the input bitmap into the accumulator.
     *
     * @param acc the running bitmap accumulator
     * @param bitmapBytes serialized RoaringBitmap bytes; null and empty arrays are ignored
     */
    public void accumulate(RoaringBitmap acc, @Nullable byte[] bitmapBytes) throws IOException {
        if (bitmapBytes == null || bitmapBytes.length == 0) {
            return;
        }
        acc.or(BitmapUtils.fromBytes(bitmapBytes));
    }

    /**
     * Retraction is not supported for bitmap OR — union is not reversible.
     *
     * @throws UnsupportedOperationException always
     */
    public void retract(RoaringBitmap acc, @Nullable byte[] bitmapBytes) {
        throw new UnsupportedOperationException(
                "rb_or_agg does not support retraction. " + "Use it only on append-only streams.");
    }
}
