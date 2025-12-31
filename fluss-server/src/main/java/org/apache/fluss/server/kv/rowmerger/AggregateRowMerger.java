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

package org.apache.fluss.server.kv.rowmerger;

import org.apache.fluss.config.TableConfig;
import org.apache.fluss.metadata.DeleteBehavior;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.server.kv.rowmerger.aggregate.AggregateFieldsProcessor;
import org.apache.fluss.server.kv.rowmerger.aggregate.AggregationContext;
import org.apache.fluss.server.kv.rowmerger.aggregate.AggregationContextCache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A row merger that aggregates rows with the same primary key using field-level aggregate
 * functions.
 *
 * <p>Each field can have its own aggregate function (e.g., sum, max, min, etc.). This allows for
 * flexible aggregation semantics at the field level.
 *
 * <p>This merger supports schema evolution by dynamically retrieving schemas based on schema IDs
 * when merging rows with different schema versions.
 *
 * <p>This class is thread-safe as it is guaranteed to be accessed by a single thread at a time
 * (protected by KvTablet's write lock).
 */
public class AggregateRowMerger implements RowMerger {

    // Cache configuration constants
    private static final int PARTIAL_MERGER_CACHE_MAX_SIZE = 4;
    private static final Duration PARTIAL_MERGER_CACHE_EXPIRE_DURATION = Duration.ofMinutes(5);

    private final SchemaGetter schemaGetter;
    private final DeleteBehavior deleteBehavior;
    private final AggregationContextCache contextCache;

    // Cache for PartialAggregateRowMerger instances to avoid repeated creation
    private final Cache<CacheKey, PartialAggregateRowMerger> partialMergerCache;

    // the current target schema id which is updated before merge() operation
    private short targetSchemaId = -1;

    public AggregateRowMerger(
            TableConfig tableConfig, KvFormat kvFormat, SchemaGetter schemaGetter) {
        this.schemaGetter = schemaGetter;
        // Extract configuration from TableConfig to ensure single source of truth
        this.deleteBehavior = tableConfig.getDeleteBehavior().orElse(DeleteBehavior.IGNORE);
        this.contextCache = new AggregationContextCache(schemaGetter, kvFormat);
        // Initialize cache with same settings as PartialUpdaterCache and AggregationContextCache
        this.partialMergerCache =
                Caffeine.newBuilder()
                        .maximumSize(PARTIAL_MERGER_CACHE_MAX_SIZE)
                        .expireAfterAccess(PARTIAL_MERGER_CACHE_EXPIRE_DURATION)
                        .build();
    }

    @Override
    public BinaryValue merge(BinaryValue oldValue, BinaryValue newValue) {
        // First write: no existing row
        if (oldValue == null || oldValue.row == null) {
            return newValue;
        }

        // Get contexts for schema evolution support
        AggregationContext oldContext = contextCache.getContext(oldValue.schemaId);
        AggregationContext newContext = contextCache.getContext(newValue.schemaId);
        AggregationContext targetContext = contextCache.getContext(targetSchemaId);

        // Use target schema encoder to ensure merged row uses latest schema
        RowEncoder encoder = targetContext.getRowEncoder();
        encoder.startNewRow();

        // Aggregate using target schema context to ensure output uses server's latest schema
        AggregateFieldsProcessor.aggregateAllFieldsWithTargetSchema(
                oldValue.row, newValue.row, oldContext, newContext, targetContext, encoder);
        BinaryRow mergedRow = encoder.finishRow();

        return new BinaryValue(targetSchemaId, mergedRow);
    }

    @Override
    public BinaryValue delete(BinaryValue oldValue) {
        // Remove the entire row (returns null to indicate deletion)
        return null;
    }

    @Override
    public DeleteBehavior deleteBehavior() {
        return deleteBehavior;
    }

    @Override
    public RowMerger configureTargetColumns(
            @Nullable int[] targetColumns, short latestSchemaId, Schema latestSchema) {
        if (targetColumns == null) {
            this.targetSchemaId = latestSchemaId;
            return this;
        }

        // Use cache to get or create PartialAggregateRowMerger
        // This avoids repeated object creation and BitSet construction
        CacheKey cacheKey = new CacheKey(latestSchemaId, targetColumns);
        return partialMergerCache.get(
                cacheKey,
                k -> {
                    // TODO: Currently, this conversion is broken when DROP COLUMN is supported,
                    //  because `targetColumns` still references column indexes from an outdated
                    //  schema, which no longer align with the current (latest) schema.
                    //  In #2239, we plan to refactor `targetColumns` to use column IDs instead of
                    //  indexes. Once that change is in place, this conversion logic can be safely
                    //  removed.
                    List<Schema.Column> columns = latestSchema.getColumns();
                    Set<Integer> targetColumnIds = new HashSet<>();
                    for (int colIdx : targetColumns) {
                        targetColumnIds.add(columns.get(colIdx).getColumnId());
                    }

                    // Build BitSet for fast target column lookup
                    BitSet targetColumnIdBitSet = new BitSet();
                    for (Integer columnId : targetColumnIds) {
                        targetColumnIdBitSet.set(columnId);
                    }

                    // Create the PartialAggregateRowMerger instance
                    return new PartialAggregateRowMerger(
                            targetColumnIdBitSet,
                            deleteBehavior,
                            schemaGetter,
                            contextCache,
                            latestSchema,
                            latestSchemaId);
                });
    }

    /**
     * Cache key for PartialAggregateRowMerger instances.
     *
     * <p>Efficiently encodes schema ID and target column indices for cache lookup. Uses array
     * content-based equality and hashCode for correct cache behavior.
     */
    private static class CacheKey {
        private final short schemaId;
        private final int[] targetColumns;
        private final int hashCode;

        CacheKey(short schemaId, int[] targetColumns) {
            this.schemaId = schemaId;
            this.targetColumns = targetColumns;
            // Pre-compute hash code for efficiency
            this.hashCode = computeHashCode();
        }

        private int computeHashCode() {
            int result = schemaId;
            result = 31 * result + Arrays.hashCode(targetColumns);
            return result;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CacheKey cacheKey = (CacheKey) o;
            return schemaId == cacheKey.schemaId
                    && Arrays.equals(targetColumns, cacheKey.targetColumns);
        }

        @Override
        public int hashCode() {
            return hashCode;
        }
    }

    /**
     * A merger that partially aggregates specified columns with the new row while keeping other
     * columns unchanged.
     *
     * <p>This merger is used when only a subset of columns need to be aggregated during merge
     * operations, allowing for partial updates while preserving other column values.
     *
     * <p>This merger supports schema evolution by using column IDs to identify target columns,
     * rather than relying on field positions.
     *
     * <p>Performance optimization: Uses BitSet for O(1) target column lookup and fast path for same
     * schema scenarios.
     *
     * <p>This class is thread-safe as it is guaranteed to be accessed by a single thread at a time
     * (protected by KvTablet's write lock).
     */
    private static class PartialAggregateRowMerger implements RowMerger {
        // Cache size and expiration configuration
        private static final int TARGET_POS_BITSET_CACHE_MAX_SIZE = 4;
        private static final Duration TARGET_POS_BITSET_CACHE_EXPIRE_DURATION =
                Duration.ofMinutes(5);

        // BitSet for fast O(1) lookup of target columns by columnId
        private final BitSet targetColumnIdBitSet;

        private final DeleteBehavior deleteBehavior;

        // Schema evolution support
        private final SchemaGetter schemaGetter;
        private final AggregationContextCache contextCache;
        // The schema ID this PartialAggregateRowMerger was created for (used for delete operations)
        private final short targetSchemaId;

        // Cache for target position BitSets by schema ID to support schema evolution in delete
        // operations
        private final Cache<Short, BitSet> targetPosBitSetCache;

        PartialAggregateRowMerger(
                BitSet targetColumnIdBitSet,
                DeleteBehavior deleteBehavior,
                SchemaGetter schemaGetter,
                AggregationContextCache contextCache,
                Schema schema,
                short schemaId) {
            this.targetColumnIdBitSet = targetColumnIdBitSet;
            this.deleteBehavior = deleteBehavior;
            this.schemaGetter = schemaGetter;
            this.contextCache = contextCache;
            this.targetSchemaId = schemaId;

            // Perform sanity check using the provided schema
            AggregationContext context = contextCache.getOrCreateContext(schemaId, schema);
            context.sanityCheckTargetColumns(targetColumnIdBitSet);

            // Initialize cache for target position BitSets
            this.targetPosBitSetCache =
                    Caffeine.newBuilder()
                            .maximumSize(TARGET_POS_BITSET_CACHE_MAX_SIZE)
                            .expireAfterAccess(TARGET_POS_BITSET_CACHE_EXPIRE_DURATION)
                            .build();
        }

        @Override
        public BinaryValue merge(BinaryValue oldValue, BinaryValue newValue) {
            // First write: no existing row
            if (oldValue == null || oldValue.row == null) {
                return newValue;
            }

            // Get contexts for schema evolution support
            AggregationContext oldContext = contextCache.getContext(oldValue.schemaId);
            AggregationContext newContext = contextCache.getContext(newValue.schemaId);
            AggregationContext targetContext = contextCache.getContext(targetSchemaId);

            // Use target schema encoder to ensure merged row uses latest schema
            RowEncoder encoder = targetContext.getRowEncoder();
            encoder.startNewRow();

            // Aggregate using target schema to ensure output uses server's latest schema
            AggregateFieldsProcessor.aggregateTargetFieldsWithTargetSchema(
                    oldValue.row,
                    newValue.row,
                    oldContext,
                    newContext,
                    targetContext,
                    targetColumnIdBitSet,
                    encoder);
            BinaryRow mergedRow = encoder.finishRow();

            return new BinaryValue(targetSchemaId, mergedRow);
        }

        @Override
        public BinaryValue delete(BinaryValue oldValue) {
            // Fast path: if oldValue uses the same schema as target, use simple logic
            if (oldValue.schemaId == targetSchemaId) {
                BitSet targetPosBitSet = getOrComputeTargetPosBitSet(targetSchemaId);

                // Check if all non-target columns are null
                if (!AggregateFieldsProcessor.hasNonTargetNonNullField(
                        oldValue.row, targetPosBitSet)) {
                    return null;
                }

                // Partial delete: set target columns (except primary key) to null
                AggregationContext context = contextCache.getContext(targetSchemaId);
                BitSet pkPosBitSet = context.getPrimaryKeyColsBitSet();

                RowEncoder encoder = context.getRowEncoder();
                encoder.startNewRow();
                AggregateFieldsProcessor.encodePartialDeleteWithSameSchema(
                        oldValue.row, context, targetPosBitSet, pkPosBitSet, encoder);
                BinaryRow deletedRow = encoder.finishRow();
                return new BinaryValue(targetSchemaId, deletedRow);
            }

            // Schema evolution path: oldValue uses different schema
            // Check non-target columns using old schema
            BitSet oldTargetPosBitSet = getOrComputeTargetPosBitSet(oldValue.schemaId);
            if (!AggregateFieldsProcessor.hasNonTargetNonNullField(
                    oldValue.row, oldTargetPosBitSet)) {
                return null;
            }

            // Get contexts for both schemas
            AggregationContext oldContext = contextCache.getContext(oldValue.schemaId);
            AggregationContext targetContext = contextCache.getContext(targetSchemaId);
            BitSet targetPkPosBitSet = targetContext.getPrimaryKeyColsBitSet();

            RowEncoder encoder = targetContext.getRowEncoder();
            encoder.startNewRow();
            AggregateFieldsProcessor.encodePartialDeleteWithDifferentSchema(
                    oldValue.row,
                    oldContext,
                    targetContext,
                    targetColumnIdBitSet,
                    targetPkPosBitSet,
                    encoder);
            BinaryRow deletedRow = encoder.finishRow();
            return new BinaryValue(targetSchemaId, deletedRow);
        }

        @Override
        public DeleteBehavior deleteBehavior() {
            return deleteBehavior;
        }

        @Override
        public RowMerger configureTargetColumns(
                @Nullable int[] targetColumns, short latestSchemaId, Schema latestSchema) {
            throw new IllegalStateException(
                    "PartialAggregateRowMerger does not support reconfigure target merge columns.");
        }

        /**
         * Get or compute target position BitSet for a given schema ID.
         *
         * <p>This method uses a cache to avoid repeated computation of target position BitSets for
         * different schemas during delete operations.
         *
         * @param schemaId the schema ID
         * @return BitSet marking target column positions
         */
        private BitSet getOrComputeTargetPosBitSet(short schemaId) {
            return targetPosBitSetCache.get(
                    schemaId,
                    sid -> {
                        Schema schema = schemaGetter.getSchema(sid);
                        if (schema == null) {
                            throw new IllegalStateException(
                                    String.format("Schema with ID %d not found", sid));
                        }
                        // Compute target position BitSet by mapping column IDs to positions
                        BitSet targetPosBitSet = new BitSet();
                        for (int pos = 0; pos < schema.getColumns().size(); pos++) {
                            int columnId = schema.getColumns().get(pos).getColumnId();
                            if (targetColumnIdBitSet.get(columnId)) {
                                targetPosBitSet.set(pos);
                            }
                        }
                        return targetPosBitSet;
                    });
        }
    }
}
