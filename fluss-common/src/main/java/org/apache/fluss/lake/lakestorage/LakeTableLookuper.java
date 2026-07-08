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
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * A table-level point lookuper for data stored in lake storage.
 *
 * @since 1.0
 */
@PublicEvolving
public interface LakeTableLookuper extends AutoCloseable {

    /**
     * Looks up one key from the lake table.
     *
     * @param key lake-format encoded primary key bytes
     * @param context lookup context
     * @return Fluss value bytes, or null if the key does not exist
     */
    @Nullable
    byte[] lookup(byte[] key, LookupContext context) throws Exception;

    /** Context for a lake table point lookup. */
    final class LookupContext {
        private final ResolvedPartitionSpec partitionSpec;
        private final int bucketId;
        private final short schemaId;
        private final RowType valueRowType;

        /**
         * Creates a lookup context.
         *
         * @param partitionSpec resolved Fluss partition spec for the lookup
         * @param bucketId target bucket id in the lake table
         * @param schemaId schema id to encode the returned Fluss value with
         * @param valueRowType row type to encode the returned Fluss value with
         */
        public LookupContext(
                ResolvedPartitionSpec partitionSpec,
                int bucketId,
                short schemaId,
                RowType valueRowType) {
            this.partitionSpec = checkNotNull(partitionSpec, "partitionSpec must not be null.");
            this.bucketId = bucketId;
            this.schemaId = schemaId;
            this.valueRowType = checkNotNull(valueRowType, "valueRowType must not be null.");
        }

        /** Returns the resolved Fluss partition spec for the lookup. */
        public ResolvedPartitionSpec partitionSpec() {
            return partitionSpec;
        }

        /** Returns the target bucket id in the lake table. */
        public int bucketId() {
            return bucketId;
        }

        /** Returns the schema id to encode the returned Fluss value with. */
        public short schemaId() {
            return schemaId;
        }

        /** Returns the row type to encode the returned Fluss value with. */
        public RowType valueRowType() {
            return valueRowType;
        }
    }
}
