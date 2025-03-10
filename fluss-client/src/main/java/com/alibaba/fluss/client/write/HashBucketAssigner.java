/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.client.write;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.bucketing.BucketingFunction;
import com.alibaba.fluss.bucketing.FlussBucketingFunction;

/** Hash bucket assigner. */
@Internal
public class HashBucketAssigner extends StaticBucketAssigner {

    private final int numBuckets;
    private final BucketingFunction function;

    public HashBucketAssigner(int numBuckets) {
        this(numBuckets, new FlussBucketingFunction());
    }

    public HashBucketAssigner(int numBuckets, BucketingFunction function) {
        this.numBuckets = numBuckets;
        this.function = function;
    }

    public int assignBucket(byte[] bucketKeys) {
        return function.bucketing(bucketKeys, numBuckets);
    }
}
