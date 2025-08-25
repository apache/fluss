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

package org.apache.fluss.bucketing;

import org.apache.fluss.shaded.guava32.com.google.common.hash.HashFunction;
import org.apache.fluss.shaded.guava32.com.google.common.hash.Hashing;

/** An implementation of {@link BucketingFunction} to follow Iceberg's bucketing strategy. */
public class IcebergBucketingFunction implements BucketingFunction {

    private static final HashFunction MURMUR3 = Hashing.murmur3_32_fixed();

    @Override
    public int bucketing(byte[] bucketKey, int numBuckets) {
        if (bucketKey == null || bucketKey.length == 0) {
            throw new IllegalArgumentException("bucketKey must not be null or empty");
        }
        if (numBuckets <= 0) {
            throw new IllegalArgumentException("numBuckets must be positive");
        }
        return (MURMUR3.hashBytes(bucketKey).asInt() & Integer.MAX_VALUE) % numBuckets;
    }
}
