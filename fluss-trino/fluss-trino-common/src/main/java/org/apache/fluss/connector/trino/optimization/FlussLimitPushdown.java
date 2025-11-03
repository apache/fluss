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

package org.apache.fluss.connector.trino.optimization;

import org.apache.fluss.connector.trino.config.FlussConnectorConfig;
import org.apache.fluss.connector.trino.handle.FlussTableHandle;
import org.apache.fluss.metadata.TableInfo;

import io.airlift.log.Logger;

import javax.inject.Inject;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Handles limit pushdown optimization for Fluss connector.
 * 
 * <p>Limit pushdown reduces data transfer by limiting the number of rows
 * read from storage.
 */
public class FlussLimitPushdown {

    private static final Logger log = Logger.get(FlussLimitPushdown.class);

    private final FlussConnectorConfig config;

    @Inject
    public FlussLimitPushdown(FlussConnectorConfig config) {
        this.config = requireNonNull(config, "config is null");
    }

    /**
     * Apply limit pushdown to the table handle.
     * 
     * <p>This method applies the limit constraint to reduce the amount of data
     * read from storage.
     */
    public FlussTableHandle applyLimitPushdown(
            FlussTableHandle tableHandle,
            long limit) {
        
        if (!config.isLimitPushdownEnabled()) {
            log.debug("Limit pushdown is disabled");
            return tableHandle;
        }

        if (limit <= 0) {
            log.debug("Invalid limit value: %d", limit);
            return tableHandle;
        }

        log.debug("Applying limit pushdown for table: %s with limit: %d",
                tableHandle.getTableName(), limit);

        // Analyze limit to determine pushdown effectiveness
        long optimizedLimit = analyzeLimitForPushdown(tableHandle, limit);
        
        return tableHandle.withLimit(optimizedLimit);
    }
    
    /**
     * Analyze limit to optimize pushdown.
     * 
     * <p>This method performs intelligent analysis of the limit value to determine
     * the optimal limit for pushdown based on table statistics, distribution, and
     * query characteristics.
     */
    private long analyzeLimitForPushdown(FlussTableHandle tableHandle, long limit) {
        // Validate input
        if (limit <= 0) {
            log.debug("Invalid limit %d for table %s, returning as-is", limit, tableHandle.getTableName());
            return limit;
        }
        
        log.debug("Analyzing limit %d for table: %s", limit, tableHandle.getTableName());
        
        // Get table statistics for better analysis
        TableStatistics tableStats = getTableStatistics(tableHandle);
        
        // 1. Estimate total row count
        long estimatedRowCount = estimateTotalRows(tableHandle, tableStats);
        log.debug("Estimated row count for table %s: %d", tableHandle.getTableName(), estimatedRowCount);
        
        // 2. Check if limit is beneficial
        if (!isBeneficial(limit, estimatedRowCount)) {
            log.debug("Limit pushdown not beneficial for table %s (limit: %d, estimated rows: %d)", 
                    tableHandle.getTableName(), limit, estimatedRowCount);
            return limit;
        }
        
        // 3. Calculate data reduction potential
        double reduction = estimateDataReduction(limit, estimatedRowCount);
        log.debug("Estimated data reduction for table %s: %.2f%%", 
                tableHandle.getTableName(), reduction * 100);
        
        // 4. Adjust limit based on distribution and safety margins
        long adjustedLimit = adjustLimitForDistribution(tableHandle, limit, tableStats);
        
        // 5. Apply safety factors for over-provisioning
        long safeLimit = applySafetyMargin(adjustedLimit, tableStats);
        
        // 6. Consider other optimizations like predicate pushdown
        long finalLimit = considerOtherOptimizations(tableHandle, safeLimit);
        
        log.debug("Limit analysis for table %s: original=%d, estimated=%d, adjusted=%d, safe=%d, final=%d",
                tableHandle.getTableName(), limit, estimatedRowCount, adjustedLimit, safeLimit, finalLimit);
        
        return finalLimit;
    }
    
    /**
     * Get table statistics for analysis.
     */
    private TableStatistics getTableStatistics(FlussTableHandle tableHandle) {
        try {
            // In a production implementation, this would query Fluss metadata
            // to get actual table statistics including row count, size, distribution, etc.
            
            TableInfo tableInfo = tableHandle.getTableInfo();
            
            // Estimate based on table descriptor
            Optional<Integer> bucketCount = tableInfo.getTableDescriptor()
                    .getDistribution()
                    .getBucketCount();
            
            return new TableStatistics(
                    bucketCount.orElse(1),  // Default to 1 bucket
                    0,  // Row count unknown
                    0   // Size unknown
            );
        } catch (Exception e) {
            log.warn(e, "Error getting table statistics for table: %s", tableHandle.getTableName());
            return new TableStatistics(1, 0, 0); // Default statistics
        }
    }
    
    /**
     * Estimate total row count for the table.
     */
    private long estimateTotalRows(FlussTableHandle tableHandle, TableStatistics stats) {
        // If we have actual row count from statistics, use it
        if (stats.getRowCount() > 0) {
            return stats.getRowCount();
        }
        
        // Otherwise, make an educated guess based on bucket count and typical data distribution
        // This is a very rough estimate - in production, this would use actual metadata
        long baseEstimate = stats.getBucketCount() * 10000L; // Assume 10K rows per bucket as baseline
        
        // Adjust based on table name patterns or other heuristics
        String tableName = tableHandle.getTableName().toLowerCase();
        if (tableName.contains("log") || tableName.contains("event")) {
            return baseEstimate * 10; // Log/event tables are typically larger
        } else if (tableName.contains("config") || tableName.contains("lookup")) {
            return Math.max(100, baseEstimate / 10); // Config/lookup tables are typically smaller
        }
        
        return baseEstimate;
    }
    
    /**
     * Adjust limit based on data distribution across buckets.
     */
    private long adjustLimitForDistribution(FlussTableHandle tableHandle, long limit, TableStatistics stats) {
        int bucketCount = stats.getBucketCount();
        
        // For single bucket tables, use limit as-is
        if (bucketCount <= 1) {
            return limit;
        }
        
        // For multi-bucket tables, we need to account for data distribution
        // Since we don't know the exact distribution, we'll add a safety margin
        // assuming worst-case uneven distribution
        
        // Calculate limit per bucket with safety margin
        // We assume data might be unevenly distributed, so we over-provision
        double safetyFactor = 1.5; // 50% safety margin
        
        // If we have many buckets, increase safety factor
        if (bucketCount > 100) {
            safetyFactor = 2.0; // Double for very high bucket counts
        } else if (bucketCount > 10) {
            safetyFactor = 1.75;
        }
        
        long adjustedLimit = (long) (limit * safetyFactor);
        
        // Ensure we don't exceed reasonable bounds
        long maxReasonableLimit = Long.MAX_VALUE / 2;
        if (adjustedLimit > maxReasonableLimit || adjustedLimit < 0) {
            adjustedLimit = maxReasonableLimit;
        }
        
        log.debug("Adjusted limit for distribution - buckets: %d, original: %d, adjusted: %d, factor: %.2f",
                bucketCount, limit, adjustedLimit, safetyFactor);
        
        return adjustedLimit;
    }
    
    /**
     * Apply safety margin to limit to account for estimation errors.
     */
    private long applySafetyMargin(long limit, TableStatistics stats) {
        // Apply additional safety margin based on confidence in estimates
        double confidence = estimateConfidence(stats);
        double safetyMargin = 1.0 + (1.0 - confidence); // More margin for lower confidence
        
        long safeLimit = (long) (limit * safetyMargin);
        
        // Ensure we don't exceed reasonable bounds
        long maxReasonableLimit = Long.MAX_VALUE / 2;
        if (safeLimit > maxReasonableLimit || safeLimit < 0) {
            safeLimit = maxReasonableLimit;
        }
        
        log.debug("Applied safety margin - original: %d, confidence: %.2f, margin: %.2f, safe: %d",
                limit, confidence, safetyMargin, safeLimit);
        
        return safeLimit;
    }
    
    /**
     * Estimate confidence in our statistics.
     */
    private double estimateConfidence(TableStatistics stats) {
        // Confidence is based on how much we actually know about the table
        if (stats.getRowCount() > 0) {
            return 0.9; // High confidence if we have actual row count
        }
        
        if (stats.getBucketCount() > 1) {
            return 0.7; // Medium confidence if we know bucket count
        }
        
        return 0.5; // Low confidence with minimal information
    }
    
    /**
     * Consider other optimizations that might affect limit pushdown.
     */
    private long considerOtherOptimizations(FlussTableHandle tableHandle, long limit) {
        long finalLimit = limit;
        
        // If we have predicate pushdown, we might need to adjust limit
        if (tableHandle.getConstraint().isPresent() && 
            !tableHandle.getConstraint().get().isAll()) {
            // Predicates might significantly reduce result set, so we could be more aggressive
            finalLimit = (long) (finalLimit * 1.2); // 20% increase to account for predicate filtering
            log.debug("Adjusted limit for predicate pushdown: %d", finalLimit);
        }
        
        // If we have column pruning, data transfer will be reduced
        if (tableHandle.getProjectedColumns().isPresent()) {
            // Column pruning reduces I/O, so limit pushdown is more beneficial
            // No adjustment needed, but worth noting in logs
            log.debug("Column pruning enabled, limit pushdown more effective");
        }
        
        // If we have partition pruning, we might scan fewer partitions
        if (tableHandle.getPartitionFilters().isPresent() && 
            !tableHandle.getPartitionFilters().get().isEmpty()) {
            // Partition pruning reduces data scanned, so limit pushdown is more beneficial
            finalLimit = (long) (finalLimit * 0.9); // 10% reduction as we'll scan less data
            log.debug("Adjusted limit for partition pruning: %d", finalLimit);
        }
        
        return finalLimit;
    }
    
    /**
     * Statistics about a table for limit analysis.
     */
    private static class TableStatistics {
        private final int bucketCount;
        private final long rowCount;
        private final long sizeInBytes;
        
        public TableStatistics(int bucketCount, long rowCount, long sizeInBytes) {
            this.bucketCount = bucketCount;
            this.rowCount = rowCount;
            this.sizeInBytes = sizeInBytes;
        }
        
        public int getBucketCount() {
            return bucketCount;
        }
        
        public long getRowCount() {
            return rowCount;
        }
        
        public long getSizeInBytes() {
            return sizeInBytes;
        }
    }

    /**
     * Check if limit pushdown is beneficial for this query.
     * 
     * <p>This method determines whether pushing down the limit will provide
     * significant performance benefits based on the limit value and estimated
     * data size.
     */
    public boolean isBeneficial(long limit, long estimatedRowCount) {
        // Limit pushdown is beneficial when limit is much smaller than total rows
        if (limit <= 0) {
            return false;
        }
        
        if (estimatedRowCount <= 0) {
            // If we don't know the row count, make a conservative estimate
            // Limit pushdown is generally beneficial for small limits
            return limit <= 10000; // Conservative threshold
        }
        
        // Calculate the ratio of limit to total rows
        double ratio = (double) limit / estimatedRowCount;
        
        // Limit pushdown is beneficial when we're reading a small fraction of the data
        // The threshold can be adjusted based on empirical data
        boolean beneficial = ratio < 0.1; // Beneficial if we're reading less than 10%
        
        log.debug("Limit benefit analysis - limit: %d, estimated rows: %d, ratio: %.4f, beneficial: %s",
                limit, estimatedRowCount, ratio, beneficial);
        
        return beneficial;
    }

    /**
     * Estimate data reduction from limit pushdown.
     * 
     * <p>This method calculates the expected reduction in data transfer and
     * processing when limit pushdown is applied.
     */
    public double estimateDataReduction(long limit, long totalRows) {
        if (totalRows <= 0 || limit <= 0) {
            return 0.0;
        }
        
        if (limit >= totalRows) {
            return 0.0; // No reduction if limit is larger than total rows
        }
        
        // Calculate basic reduction ratio
        double basicReduction = 1.0 - ((double) limit / totalRows);
        
        // Apply diminishing returns for very small limits
        // Very small limits might not provide proportional benefits due to overhead
        if (limit < 10) {
            basicReduction *= 0.8; // Reduce estimated benefit for very small limits
        } else if (limit < 100) {
            basicReduction *= 0.9; // Slight reduction for small limits
        }
        
        // Ensure result is within valid range
        double finalReduction = Math.max(0.0, Math.min(1.0, basicReduction));
        
        log.debug("Data reduction estimate - limit: %d, total rows: %d, basic: %.4f, final: %.4f",
                limit, totalRows, basicReduction, finalReduction);
        
        return finalReduction;
    }
}
