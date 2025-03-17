/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.bucketing;

import com.alibaba.fluss.metadata.DataLakeFormat;

import javax.annotation.Nullable;

/** An interface to assign a bucket according to the bucket key byte array. */
public interface BucketingFunction {

    /**
     * Assign a bucket according to the bucket key byte array.
     *
     * @param bucketKey the bucket key byte array
     * @param numBuckets the number of buckets
     * @return the bucket id
     */
    int bucketing(byte[] bucketKey, int numBuckets);

    /** Create a bucketing function according to the optional datalake format. */
    static BucketingFunction of(@Nullable DataLakeFormat lakeFormat) {
        if (lakeFormat == null) {
            return new FlussBucketingFunction();
        } else if (lakeFormat == DataLakeFormat.PAIMON) {
            return new PaimonBucketingFunction();
        } else {
            throw new UnsupportedOperationException("Unsupported lake format: " + lakeFormat);
        }
    }
}
