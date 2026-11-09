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
 * {@code rb_xor(left BYTES, right BYTES) -> BYTES}
 *
 * <p>Returns the bitwise XOR (symmetric difference) of two serialized {@link RoaringBitmap} values.
 * The result contains elements that appear in exactly one of the two bitmaps. Returns {@code null}
 * if either argument is null.
 */
public class RbXorFunction extends ScalarFunction {

    /**
     * @param leftBytes serialized left bitmap
     * @param rightBytes serialized right bitmap
     * @return symmetric difference of left and right, or null if either argument is null
     */
    @Nullable
    public byte[] eval(@Nullable byte[] leftBytes, @Nullable byte[] rightBytes) throws IOException {
        if (leftBytes == null || rightBytes == null) {
            return null;
        }
        RoaringBitmap left = BitmapUtils.fromBytes(leftBytes);
        RoaringBitmap right = BitmapUtils.fromBytes(rightBytes);
        left.xor(right);
        return BitmapUtils.toBytes(left);
    }
}
