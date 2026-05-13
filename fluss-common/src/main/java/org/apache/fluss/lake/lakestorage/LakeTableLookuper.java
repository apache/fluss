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

package org.apache.fluss.lake.lakestorage;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.metadata.ResolvedPartitionSpec;

import javax.annotation.Nullable;

/**
 * Interface for looking up a single key from lake storage. This is used for:
 *
 * <ul>
 *   <li>PK table historical write: old-value fallback from lake when RocksDB doesn't have the key
 *   <li>Expired partition point lookup: local-first lookup (RocksDB → lake fallback)
 * </ul>
 *
 * @since 0.7
 */
@PublicEvolving
public interface LakeTableLookuper extends AutoCloseable {

    /**
     * Lookup a single key from lake storage.
     *
     * <p>Key bytes are already encoded in the lake storage's native key format. Returned value
     * bytes should be encoded with schema ID prefix.
     *
     * @param key encoded key bytes in lake storage's native format
     * @param context lookup context containing partition, bucket, and schema information
     * @return encoded value bytes with schema ID prefix, or null if not found
     * @throws Exception if lookup fails
     */
    @Nullable
    byte[] lookup(byte[] key, LookupContext context) throws Exception;

    /** Context for a single lake lookup operation. */
    class LookupContext {
        @Nullable private final ResolvedPartitionSpec partitionSpec;
        private final int bucketId;
        private final int schemaId;

        public LookupContext(
                @Nullable ResolvedPartitionSpec partitionSpec, int bucketId, int schemaId) {
            this.partitionSpec = partitionSpec;
            this.bucketId = bucketId;
            this.schemaId = schemaId;
        }

        /** Returns the partition spec, or null for non-partitioned tables. */
        @Nullable
        public ResolvedPartitionSpec getPartitionSpec() {
            return partitionSpec;
        }

        /** Returns the bucket ID. */
        public int getBucketId() {
            return bucketId;
        }

        /** Returns the schema ID used for value encoding. */
        public int getSchemaId() {
            return schemaId;
        }
    }
}
