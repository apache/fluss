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

    /** Records metrics for a lake table point lookup. */
    @FunctionalInterface
    interface LookupMetricRecorder {

        /**
         * Records a completed lake table point lookup.
         *
         * @param lookupTimeNanos time spent on the lake table point lookup, in nanoseconds
         * @param lookupFileDownloaded whether the lookup downloaded a lookup file
         */
        void recordLookup(long lookupTimeNanos, boolean lookupFileDownloaded);
    }

    /**
     * Looks up one key from the lake table.
     *
     * <p>This method may be called concurrently. Implementations must ensure that it is
     * thread-safe.
     *
     * @param key lake-format encoded primary key bytes
     * @param context lookup context
     * @return plain Fluss value bytes encoded as {@code [schema-id][BinaryRow]}, or null if the key
     *     does not exist
     */
    @Nullable
    byte[] lookup(byte[] key, LookupContext context) throws Exception;

    /**
     * Requests that registered lake data files be refreshed before the next lookup.
     *
     * <p>This method must not perform I/O and may be called concurrently with {@link
     * #lookup(byte[], LookupContext)}. Implementations must ensure that it is thread-safe.
     */
    default void requestRefresh() {
        throw new UnsupportedOperationException(
                "Refreshing registered files is not supported by this lake table lookuper.");
    }

    /** Context for a lake table point lookup. */
    final class LookupContext {
        private final ResolvedPartitionSpec partitionSpec;
        private final int bucketId;
        private final short schemaId;
        private final RowType valueRowType;
        private final LookupMetricRecorder lookupMetricRecorder;

        /**
         * Creates a lookup context.
         *
         * @param partitionSpec resolved Fluss partition spec for the lookup
         * @param bucketId target bucket id in the lake table
         * @param schemaId schema id to encode the returned Fluss value with
         * @param valueRowType row type to encode the returned Fluss value with
         * @param lookupMetricRecorder recorder for lake table point lookup metrics
         */
        public LookupContext(
                ResolvedPartitionSpec partitionSpec,
                int bucketId,
                short schemaId,
                RowType valueRowType,
                LookupMetricRecorder lookupMetricRecorder) {
            this.partitionSpec = checkNotNull(partitionSpec, "partitionSpec must not be null.");
            this.bucketId = bucketId;
            this.schemaId = schemaId;
            this.valueRowType = checkNotNull(valueRowType, "valueRowType must not be null.");
            this.lookupMetricRecorder =
                    checkNotNull(lookupMetricRecorder, "lookupMetricRecorder must not be null.");
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

        /** Returns the recorder for lake table point lookup metrics. */
        public LookupMetricRecorder lookupMetricRecorder() {
            return lookupMetricRecorder;
        }
    }
}
