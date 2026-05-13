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

package org.apache.fluss.server.kv;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.utils.types.Tuple2;

import java.nio.charset.StandardCharsets;

/**
 * Static utility class for encoding and decoding composite keys used by historical partitions.
 *
 * <p>A composite key combines a partition name with the original key so that records from multiple
 * partitions can be stored in a single RocksDB instance per bucket.
 *
 * <p>The binary format is:
 *
 * <pre>
 * [4 bytes: partitionName length, big-endian int]
 * [N bytes: partitionName, UTF-8]
 * [remaining bytes: original key]
 * </pre>
 */
@Internal
public final class CompositeKeyEncoder {

    private CompositeKeyEncoder() {}

    /**
     * Encodes a partition name and an original key into a composite key.
     *
     * @param partitionName the partition name (e.g. the auto-partition column value like
     *     "20240101")
     * @param originalKey the original key bytes
     * @return the composite key bytes
     */
    public static byte[] encode(String partitionName, byte[] originalKey) {
        byte[] partitionBytes = partitionName.getBytes(StandardCharsets.UTF_8);
        int partitionLen = partitionBytes.length;
        byte[] composite = new byte[4 + partitionLen + originalKey.length];

        // Write partition name length as big-endian int
        composite[0] = (byte) (partitionLen >>> 24);
        composite[1] = (byte) (partitionLen >>> 16);
        composite[2] = (byte) (partitionLen >>> 8);
        composite[3] = (byte) partitionLen;

        // Write partition name bytes
        System.arraycopy(partitionBytes, 0, composite, 4, partitionLen);

        // Write original key bytes
        System.arraycopy(originalKey, 0, composite, 4 + partitionLen, originalKey.length);

        return composite;
    }

    /**
     * Decodes a composite key into the partition name and the original key.
     *
     * @param compositeKey the composite key bytes produced by {@link #encode}
     * @return a tuple of (partitionName, originalKey)
     */
    public static Tuple2<String, byte[]> decode(byte[] compositeKey) {
        // Read partition name length (big-endian int)
        int partitionLen =
                ((compositeKey[0] & 0xFF) << 24)
                        | ((compositeKey[1] & 0xFF) << 16)
                        | ((compositeKey[2] & 0xFF) << 8)
                        | (compositeKey[3] & 0xFF);

        String partitionName = new String(compositeKey, 4, partitionLen, StandardCharsets.UTF_8);

        int originalKeyLen = compositeKey.length - 4 - partitionLen;
        byte[] originalKey = new byte[originalKeyLen];
        System.arraycopy(compositeKey, 4 + partitionLen, originalKey, 0, originalKeyLen);

        return Tuple2.of(partitionName, originalKey);
    }
}
