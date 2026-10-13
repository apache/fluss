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

import org.apache.flink.table.functions.ScalarFunction;
import org.roaringbitmap.RoaringBitmap;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * {@code rb_cardinality(bitmap BYTES) -> BIGINT}
 *
 * <p>Returns the number of distinct integers in the serialized {@link RoaringBitmap}. Returns
 * {@code null} for null or empty input.
 */
public class RbCardinalityFunction extends ScalarFunction {

    /**
     * @param bitmapBytes serialized RoaringBitmap; null returns null
     * @return number of distinct integers, or null if input is null
     */
    @Nullable
    public Long eval(@Nullable byte[] bitmapBytes) throws IOException {
        if (bitmapBytes == null || bitmapBytes.length == 0) {
            return null;
        }
        RoaringBitmap bitmap = BitmapUtils.fromBytes(bitmapBytes);
        return bitmap.getLongCardinality();
    }
}
