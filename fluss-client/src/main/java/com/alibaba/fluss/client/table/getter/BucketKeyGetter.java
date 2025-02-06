/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.client.table.getter;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.encode.KeyEncoder;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.Preconditions;

import java.util.List;

/** A getter to get bucket key from a row. */
@Internal
public class BucketKeyGetter {

    private final KeyEncoder bucketKeyEncoder;

    public BucketKeyGetter(RowType rowType, List<String> bucketKeyNames) {
        this.bucketKeyEncoder = KeyEncoder.createKeyEncoder(rowType, bucketKeyNames);
    }

    public byte[] getBucketKey(InternalRow row) {
        byte[] bucketKeyValue = bucketKeyEncoder.encode(row);
        Preconditions.checkNotNull(bucketKeyValue, "Bucket key shouldn't be null.");
        return bucketKeyValue;
    }
}
