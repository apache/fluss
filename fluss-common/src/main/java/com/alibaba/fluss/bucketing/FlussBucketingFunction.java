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

package com.alibaba.fluss.bucketing;

import com.alibaba.fluss.utils.MathUtils;
import com.alibaba.fluss.utils.MurmurHashUtils;

import static com.alibaba.fluss.utils.Preconditions.checkArgument;
import static com.alibaba.fluss.utils.UnsafeUtils.BYTE_ARRAY_BASE_OFFSET;

/** The Fluss default bucketing implementation. */
public class FlussBucketingFunction implements BucketingFunction {

    @Override
    public int bucketing(byte[] bucketKey, int numBuckets) {
        return bucketForRowKey(bucketKey, numBuckets);
    }

    /**
     * If the table contains primary key, the default hashing function to choose a bucket from the
     * serialized key bytes.
     */
    public static int bucketForRowKey(final byte[] key, final int numBuckets) {
        checkArgument(key.length != 0, "Assigned key must not be empty!");
        int keyHash = MurmurHashUtils.hashUnsafeBytes(key, BYTE_ARRAY_BASE_OFFSET, key.length);
        return MathUtils.murmurHash(keyHash) % numBuckets;
    }
}
