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

/**
 * An implementation of {@link BucketingFunction} to follow Hudi's bucketing strategy.
 *
 * <p>The bucket id is computed in the same way as Hudi's {@code
 * org.apache.hudi.index.bucket.BucketIdentifier#getBucketId(String, String, int)}: take a 32-bit
 * integer hash that is encoded as a fixed 4-byte big-endian array by {@code HudiKeyEncoder},
 * mask its sign bit and modulo by {@code numBuckets}.
 */
public class HudiBucketingFunction implements BucketingFunction {

    /** Length of a Hudi-encoded bucket key, in bytes (a single big-endian {@code int}). */
    private static final int ENCODED_KEY_LENGTH = 4;

    @Override
    public int bucketing(byte[] bucketKey, int numBuckets) {
        if (bucketKey == null) {
            throw new IllegalArgumentException("bucketKey must not be null");
        }
        if (bucketKey.length != ENCODED_KEY_LENGTH) {
            throw new IllegalArgumentException(
                    "bucketKey must be exactly "
                            + ENCODED_KEY_LENGTH
                            + " bytes for Hudi bucketing, but got "
                            + bucketKey.length
                            + " bytes. The bucket key bytes are expected to be produced by HudiKeyEncoder.");
        }
        if (numBuckets <= 0) {
            throw new IllegalArgumentException(
                    "numBuckets must be positive, but got " + numBuckets);
        }

        // Decode 4-byte big-endian int produced by HudiKeyEncoder via bit operations
        // to avoid allocating a ByteBuffer instance on this hot path.
        int restored =
                ((bucketKey[0] & 0xFF) << 24)
                        | ((bucketKey[1] & 0xFF) << 16)
                        | ((bucketKey[2] & 0xFF) << 8)
                        | (bucketKey[3] & 0xFF);

        return (restored & Integer.MAX_VALUE) % numBuckets;
    }
}