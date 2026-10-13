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

/**
 * {@code rb_build_agg(value INT) -> BYTES}
 *
 * <p>Aggregates a column of 32-bit integer values into a single serialized {@link RoaringBitmap}.
 * Each non-null integer is added to the bitmap during accumulation. Duplicate values are silently
 * ignored — the bitmap stores each integer at most once.
 *
 * <p>Returns {@code null} when the input set is empty.
 */
public class RbBuildAggFunction extends AbstractRbAggFunction {

    /**
     * Adds a single integer value to the accumulator bitmap.
     *
     * @param acc the running bitmap accumulator
     * @param value the integer value to add; null values are ignored
     */
    public void accumulate(RoaringBitmap acc, @Nullable Integer value) {
        if (value == null) {
            return;
        }
        acc.add(value);
    }

    /**
     * Retraction is not supported for bitmap build — integer addition is not reversible.
     *
     * @throws UnsupportedOperationException always
     */
    public void retract(RoaringBitmap acc, @Nullable Integer value) {
        throw new UnsupportedOperationException(
                "rb_build_agg does not support retraction. "
                        + "Use it only on append-only streams.");
    }
}
