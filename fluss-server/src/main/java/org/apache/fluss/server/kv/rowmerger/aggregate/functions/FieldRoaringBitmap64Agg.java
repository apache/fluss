/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.kv.rowmerger.aggregate.functions;

/* This file is based on source code of Apache Paimon Project (https://paimon.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

import org.apache.fluss.server.utils.RoaringBitmapUtils;
import org.apache.fluss.types.DataType;

import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.IOException;

/** Roaring bitmap aggregator for serialized 64-bit bitmaps. */
public class FieldRoaringBitmap64Agg extends FieldAggregator {

    private static final long serialVersionUID = 1L;
    private final Roaring64Bitmap roaringBitmapAcc;
    private final Roaring64Bitmap roaringBitmapInput;

    public FieldRoaringBitmap64Agg(DataType dataType) {
        super(dataType);
        this.roaringBitmapAcc = new Roaring64Bitmap();
        this.roaringBitmapInput = new Roaring64Bitmap();
    }

    @Override
    public Object agg(Object accumulator, Object inputField) {
        if (accumulator == null || inputField == null) {
            return accumulator == null ? inputField : accumulator;
        }

        try {
            RoaringBitmapUtils.deserializeRoaringBitmap64(roaringBitmapAcc, (byte[]) accumulator);
            RoaringBitmapUtils.deserializeRoaringBitmap64(roaringBitmapInput, (byte[]) inputField);
            roaringBitmapAcc.or(roaringBitmapInput);
            return RoaringBitmapUtils.serializeRoaringBitmap64(roaringBitmapAcc);
        } catch (IOException e) {
            throw new RuntimeException("Unable to se/deserialize roaring bitmap.", e);
        } finally {
            roaringBitmapAcc.clear();
            roaringBitmapInput.clear();
        }
    }
}
