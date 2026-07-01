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
 * {@code rb_build(v1 INT, v2 INT, ...) -> BYTES}
 *
 * <p>Builds a serialized {@link RoaringBitmap} from a variadic list of 32-bit integer values within
 * a single row. Unlike {@code rb_build_agg}, this function operates on individual column values in
 * the same row rather than aggregating across rows. Null values are ignored. Returns {@code null}
 * if all inputs are null or no inputs are provided.
 */
public class RbBuildFunction extends ScalarFunction {

    /**
     * @param values variadic integer values to add to the bitmap; null values are ignored
     * @return serialized bitmap, or null if all inputs are null
     */
    @Nullable
    public byte[] eval(@Nullable Integer... values) throws IOException {
        if (values == null || values.length == 0) {
            return null;
        }
        RoaringBitmap bitmap = new RoaringBitmap();
        for (Integer v : values) {
            if (v != null) {
                bitmap.add(v);
            }
        }
        return bitmap.isEmpty() ? null : BitmapUtils.toBytes(bitmap);
    }
}
