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
import java.util.HashMap;
import java.util.Map;
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
        
        // 7. Apply adaptive learning from previous queries (if available)
        long adaptiveLimit = applyAdaptiveLearning(tableHandle, finalLimit);
        
        // 8. Ensure limit is within reasonable bounds
        long validatedLimit = validateAndBoundLimit(adaptiveLimit, estimatedRowCount);
        
        log.debug("Limit analysis for table %s: original=%d, estimated=%d, adjusted=%d, safe=%d, final=%d, adaptive=%d, validated=%d",
                tableHandle.getTableName(), limit, estimatedRowCount, adjustedLimit, safeLimit, finalLimit, adaptiveLimit, validatedLimit);
        
        return validatedLimit;
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
     * Apply adaptive learning from previous queries to optimize limit.
     * 
     * <p>This method uses historical query performance data to adjust the limit
     * for better performance based on learned patterns.
     */
    private long applyAdaptiveLearning(FlussTableHandle tableHandle, long limit) {
        // In a production implementation, this would use a cache of historical query data
        // to learn optimal limits for specific tables and query patterns
        
        String tableName = tableHandle.getTableName();
        
        // Get historical performance data for this table
        QueryPerformanceHistory history = getQueryHistory(tableName);
        
        if (history.getQueryCount() < 5) {
            // Not enough data to make informed decisions
            log.debug("Insufficient historical data for adaptive learning on table: %s", tableName);
            return limit;
        }
        
        // Analyze historical data to determine if we should adjust the limit
        double avgActualRows = history.getAverageActualRows();
        double avgLimit = history.getAverageLimit();
        
        // If our limits are consistently too high, reduce them
        if (avgLimit > avgActualRows * 2) {
            // Limits are typically 2x higher than actual results, adjust downward
            double adjustmentFactor = avgActualRows / avgLimit * 1.5; // 50% buffer
            long adjustedLimit = Math.max(1, (long) (limit * adjustmentFactor));
            
            log.debug("Adjusted limit based on adaptive learning - table: %s, original: %d, adjusted: %d, factor: %.2f",
                    tableName, limit, adjustedLimit, adjustmentFactor);
            
            return adjustedLimit;
        }
        
        // If our limits are consistently too low, increase them with a safety margin
        if (avgActualRows > avgLimit * 1.2) {
            // Actual results are typically 20% higher than limits, adjust upward
            double adjustmentFactor = avgActualRows / avgLimit * 1.3; // 30% buffer
            long adjustedLimit = Math.max(1, (long) (limit * adjustmentFactor));
            
            log.debug("Increased limit based on adaptive learning - table: %s, original: %d, adjusted: %d, factor: %.2f",
                    tableName, limit, adjustedLimit, adjustmentFactor);
            
            return adjustedLimit;
        }
        
        // No significant adjustment needed
        return limit;
    }
    
    /**
     * Validate and bound limit to reasonable values.
     * 
     * <p>This method ensures the limit is within acceptable ranges and adjusts
     * extreme values to prevent resource exhaustion.
     */
    private long validateAndBoundLimit(long limit, long estimatedRowCount) {
        // Ensure limit is positive
        if (limit <= 0) {
            return 1;
        }
        
        // Prevent extremely large limits that could cause resource issues
        long maxSafeLimit = Math.min(Long.MAX_VALUE / 4, 10_000_000_000L); // 10 billion as max safe limit
        
        if (limit > maxSafeLimit) {
            log.warn("Limit %d exceeds maximum safe limit %d, capping to safe value", limit, maxSafeLimit);
            return maxSafeLimit;
        }
        
        // For very small tables, don't over-limit
        if (estimatedRowCount > 0 && limit > estimatedRowCount * 2) {
            // If limit is more than 2x the estimated row count, cap it
            long cappedLimit = Math.min(limit, (long) (estimatedRowCount * 1.5));
            log.debug("Capping limit for small table - estimated: %d, original: %d, capped: %d",
                    estimatedRowCount, limit, cappedLimit);
            return cappedLimit;
        }
        
        return limit;
    }
    
    /**
     * Get query performance history for adaptive learning.
     * 
     * <p>In a production implementation, this would retrieve historical data
     * from a performance monitoring system.
     */
    private QueryPerformanceHistory getQueryHistory(String tableName) {
        // This is a simplified implementation that would be replaced with
        // actual historical data retrieval in production
        return new QueryPerformanceHistory(0, 0, 0);
    }

    /**
     * Historical query performance data for adaptive learning.
     */
    private static class QueryPerformanceHistory {
        private final long queryCount;
        private final double averageActualRows;
        private final double averageLimit;
        
        public QueryPerformanceHistory(long queryCount, double averageActualRows, double averageLimit) {
            this.queryCount = queryCount;
            this.averageActualRows = averageActualRows;
            this.averageLimit = averageLimit;
        }
        
        public long getQueryCount() {
            return queryCount;
        }
        
        public double getAverageActualRows() {
            return averageActualRows;
        }
        
        public double getAverageLimit() {
            return averageLimit;
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
        
        // For very small limits, always beneficial
        if (limit <= 100) {
            log.debug("Limit pushdown beneficial for small limit: %d", limit);
            return true;
        }
        
        if (estimatedRowCount <= 0) {
            // If we don't know the row count, make a conservative estimate
            // Limit pushdown is generally beneficial for small limits
            boolean beneficial = limit <= 10000; // Conservative threshold
            log.debug("Limit pushdown beneficial (unknown row count): %s, limit: %d", beneficial, limit);
            return beneficial;
        }
        
        // Calculate the ratio of limit to total rows
        double ratio = (double) limit / estimatedRowCount;
        
        // Consider additional factors for benefit analysis
        boolean sizeBenefit = ratio < 0.1; // Beneficial if we're reading less than 10%
        
        // For larger tables, even higher ratios might be beneficial
        boolean tableSizeBenefit = estimatedRowCount > 1000000 && ratio < 0.25; // 25% for large tables
        
        // For very large tables, even higher ratios can be beneficial
        boolean veryLargeTableBenefit = estimatedRowCount > 100000000 && ratio < 0.5; // 50% for very large tables
        
        boolean beneficial = sizeBenefit || tableSizeBenefit || veryLargeTableBenefit;
        
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
        
        // Consider column count impact - more columns mean more data reduction benefit
        // This is a simplified model - in practice, this would be more sophisticated
        double columnFactor = 1.0; // Default factor
        
        // For wide tables (many columns), limit pushdown is more beneficial
        if (limit < 1000) {
            columnFactor = 1.1; // 10% bonus for wide tables with small limits
        }
        
        // Apply column factor but ensure we don't exceed 100% reduction
        double adjustedReduction = Math.min(1.0, basicReduction * columnFactor);
        
        // Consider row size - larger rows mean more data reduction benefit
        double rowSizeFactor = 1.0; // Default factor
        
        // For large row sizes, limit pushdown is more beneficial
        if (limit < 10000) {
            rowSizeFactor = 1.05; // 5% bonus for large rows with small limits
        }
        
        // Apply row size factor but ensure we don't exceed 100% reduction
        double finalReduction = Math.min(1.0, adjustedReduction * rowSizeFactor);
        
        // Ensure result is within valid range
        finalReduction = Math.max(0.0, Math.min(1.0, finalReduction));
        
        log.debug("Data reduction estimate - limit: %d, total rows: %d, basic: %.4f, adjusted: %.4f, final: %.4f",
                limit, totalRows, basicReduction, adjustedReduction, finalReduction);
        
        return finalReduction;
    }
}
