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

package org.apache.fluss.record;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.record.statistics.LogRecordBatchStatistics;

/**
 * Schema-aware filter that supports statistics-based filtering with tightly bound schema
 * statistics. This filter only provides batch-level filtering based on statistical information and
 * does not support row-level predicate evaluation.
 *
 * <p>The filter applies the underlying predicate to batch statistics to determine whether entire
 * batches can be filtered out based on their statistical properties.
 */
@Internal
public class Filter {

    private final Predicate predicate;
    private final int schemaId;

    /**
     * Creates a schema-aware filter.
     *
     * @param predicate the underlying predicate to apply
     * @param schemaId the expected schema ID
     */
    public Filter(Predicate predicate, int schemaId) {
        this.predicate = predicate;
        this.schemaId = schemaId;
    }

    /**
     * Test method that accepts LogRecordBatchStatistics directly. This method provides the main
     * schema-aware filtering logic.
     *
     * <p>Current implementation only supports basic schema awareness by exact schema ID matching.
     * When statistics schema ID doesn't match the filter's expected schema ID, the batch is
     * included without statistics-based filtering.
     *
     * <p>TODO: Support schema evolution by implementing compatible schema mapping and column
     * projection/transformation between different schema versions.
     *
     * @param rowCount the number of rows in the batch
     * @param statistics the schema-aware statistics
     * @return true to include the batch, false to filter it out
     */
    public boolean test(long rowCount, LogRecordBatchStatistics statistics) {
        if (statistics == null) {
            // No statistics available, cannot filter
            return true;
        }

        // Check schema compatibility
        int statisticsSchemaId = statistics.getSchemaId();
        if (statisticsSchemaId != schemaId) {
            // Schema mismatch, skip statistics-based filtering
            return true;
        }

        // Schema matches, apply predicate using statistics
        return predicate.test(
                rowCount,
                statistics.getMinValues(),
                statistics.getMaxValues(),
                statistics.getNullCounts());
    }

    /**
     * Get the underlying predicate.
     *
     * @return the underlying predicate
     */
    public Predicate getPredicate() {
        return predicate;
    }

    /**
     * Get the expected schema ID.
     *
     * @return the expected schema ID
     */
    public int getSchemaId() {
        return schemaId;
    }

    /**
     * Create a schema-aware wrapper for an existing predicate.
     *
     * @param predicate the predicate to wrap
     * @param schemaId the expected schema ID
     * @return schema-aware predicate wrapper
     */
    public static Filter wrap(Predicate predicate, int schemaId) {
        if (predicate instanceof Filter) {
            // Already schema-aware, return as-is
            return (Filter) predicate;
        }
        return new Filter(predicate, schemaId);
    }

    public String toString() {
        return String.format("Filter{schemaId=%d, predicate=%s}", schemaId, predicate);
    }
}
