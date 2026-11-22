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
 * {@code rb_contains(bitmap BYTES, value INT) -> BOOLEAN}
 *
 * <p>Returns {@code true} if the serialized {@link RoaringBitmap} contains the given integer.
 * Returns {@code null} if either argument is null.
 */
public class RbContainsFunction extends ScalarFunction {

    /**
     * @param bitmapBytes serialized RoaringBitmap
     * @param value the integer to check
     * @return true if the bitmap contains the value, null if either argument is null
     */
    @Nullable
    public Boolean eval(@Nullable byte[] bitmapBytes, @Nullable Integer value) throws IOException {
        if (bitmapBytes == null || value == null) {
            return null;
        }
        RoaringBitmap bitmap = BitmapUtils.fromBytes(bitmapBytes);
        return bitmap.contains(value);
    }
}
