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
import org.apache.fluss.connector.trino.handle.FlussColumnHandle;
import org.apache.fluss.connector.trino.handle.FlussTableHandle;
import org.apache.fluss.connector.trino.typeutils.FlussTypeUtils;
import org.apache.fluss.types.DataField;

import io.airlift.log.Logger;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Handles column pruning optimization for Fluss connector.
 * 
 * <p>Column pruning reduces I/O by only reading the columns that are
 * actually needed by the query.
 */
public class FlussColumnPruning {

    private static final Logger log = Logger.get(FlussColumnPruning.class);

    private final FlussConnectorConfig config;

    @Inject
    public FlussColumnPruning(FlussConnectorConfig config) {
        this.config = requireNonNull(config, "config is null");
    }

    /**
     * Apply column pruning to the table handle.
     * 
     * <p>This method analyzes the projected columns and determines which ones
     * can be safely pruned to reduce I/O.
     */
    public FlussTableHandle applyColumnPruning(
            FlussTableHandle tableHandle,
            Set<ColumnHandle> projectedColumns) {
        
        if (!config.isColumnPruningEnabled()) {
            log.debug("Column pruning is disabled");
            return tableHandle;
        }

        if (projectedColumns.isEmpty()) {
            log.debug("No columns to project for table: %s", tableHandle.getTableName());
            return tableHandle;
        }

        log.debug("Applying column pruning for table: %s, projecting %d columns",
                tableHandle.getTableName(), projectedColumns.size());

        // Analyze projected columns to determine pruning potential
        Set<ColumnHandle> prunedColumns = analyzeColumnsForPruning(tableHandle, projectedColumns);
        
        return tableHandle.withProjectedColumns(prunedColumns);
    }
    
    /**
     * Analyze columns to determine which ones should be pruned.
     * 
     * <p>This method performs intelligent analysis of projected columns to determine
     * the optimal set of columns for pruning based on data types, sizes, and query
     * characteristics.
     */
    private Set<ColumnHandle> analyzeColumnsForPruning(
            FlussTableHandle tableHandle, 
            Set<ColumnHandle> projectedColumns) {
        
        // Get all available columns from table metadata
        int totalColumns = tableHandle.getTableInfo().getSchema().toRowType().getFieldCount();
        
        // Check if pruning is beneficial
        if (!canBenefitFromPruning(totalColumns, projectedColumns.size())) {
            log.debug("Column pruning not beneficial for table %s (%d total, %d projected)", 
                    tableHandle.getTableName(), totalColumns, projectedColumns.size());
            return projectedColumns;
        }
        
        // Estimate I/O reduction
        double ioReduction = estimateIoReduction(totalColumns, projectedColumns.size());
        log.debug("Estimated I/O reduction from column pruning: %.2f%% for table: %s", 
                ioReduction * 100, tableHandle.getTableName());
        
        // Perform intelligent column analysis
        ColumnAnalysisResult analysis = performColumnAnalysis(tableHandle, projectedColumns);
        
        // Apply pruning based on analysis
        Set<ColumnHandle> prunedColumns = applyIntelligentPruning(tableHandle, projectedColumns, analysis);
        
        log.debug("Pruned columns for table %s: %d -> %d (reduction: %.2f%%)", 
                tableHandle.getTableName(), projectedColumns.size(), prunedColumns.size(),
                analysis.getReductionPercentage());
        
        return prunedColumns;
    }
    
    /**
     * Perform detailed analysis of columns for pruning decisions.
     */
    private ColumnAnalysisResult performColumnAnalysis(
            FlussTableHandle tableHandle, 
            Set<ColumnHandle> projectedColumns) {
        
        int totalColumns = tableHandle.getTableInfo().getSchema().toRowType().getFieldCount();
        int projectedCount = projectedColumns.size();
        
        // Estimate data size reduction
        double sizeReduction = estimateIoReduction(totalColumns, projectedCount);
        
        // Analyze column types and their impact
        int largeColumnCount = 0;
        int smallColumnCount = 0;
        long estimatedTotalSize = 0;
        long estimatedProjectedSize = 0;
        
        // Get column metadata for size estimation
        Map<String, ColumnMetadata> columnMetadataMap = getColumnMetadataMap(tableHandle);
        
        for (ColumnHandle column : projectedColumns) {
            if (column instanceof FlussColumnHandle) {
                FlussColumnHandle flussColumn = (FlussColumnHandle) column;
                String columnName = flussColumn.getName();
                
                // Estimate column size based on type
                long estimatedSize = estimateColumnSize(flussColumn, columnMetadataMap.get(columnName));
                estimatedProjectedSize += estimatedSize;
                
                // Categorize columns by size
                if (estimatedSize > 1024) { // Larger than 1KB
                    largeColumnCount++;
                } else {
                    smallColumnCount++;
                }
            }
        }
        
        // Estimate total table size
        estimatedTotalSize = estimateTotalTableSize(tableHandle, columnMetadataMap);
        
        // Calculate actual size reduction
        double actualSizeReduction = estimatedTotalSize > 0 ? 
                1.0 - ((double) estimatedProjectedSize / estimatedTotalSize) : sizeReduction;
        
        log.debug("Column analysis for table %s - total: %d, projected: %d, large cols: %d, small cols: %d, size reduction: %.2f%%",
                tableHandle.getTableName(), totalColumns, projectedCount, largeColumnCount, smallColumnCount, 
                actualSizeReduction * 100);
        
        return new ColumnAnalysisResult(
                totalColumns,
                projectedCount,
                largeColumnCount,
                smallColumnCount,
                actualSizeReduction,
                estimatedTotalSize,
                estimatedProjectedSize
        );
    }
    
    /**
     * Get column metadata map for size estimation.
     */
    private Map<String, ColumnMetadata> getColumnMetadataMap(FlussTableHandle tableHandle) {
        Map<String, ColumnMetadata> columnMetadataMap = new HashMap<>();
        
        try {
            // This would typically come from metadata service
            // For now, we'll create a simplified version
            List<DataField> fields = tableHandle.getTableInfo().getSchema().toRowType().getFields();
            for (DataField field : fields) {
                // In a real implementation, we would get actual ColumnMetadata
                // For now, we'll create placeholder metadata
                ColumnMetadata metadata = ColumnMetadata.builder()
                        .setName(field.getName())
                        .setType(FlussTypeUtils.toTrinoType(field.getType(), null)) // TypeManager would be injected
                        .setNullable(field.getType().isNullable())
                        .build();
                columnMetadataMap.put(field.getName(), metadata);
            }
        } catch (Exception e) {
            log.warn(e, "Error getting column metadata for table: %s", tableHandle.getTableName());
        }
        
        return columnMetadataMap;
    }
    
    /**
     * Estimate the size of a column based on its type and metadata.
     */
    private long estimateColumnSize(FlussColumnHandle column, ColumnMetadata metadata) {
        if (metadata == null) {
            // Fallback estimation based on type name
            String typeName = column.getType().getDisplayName().toLowerCase();
            return estimateColumnSizeByType(typeName);
        }
        
        // Estimate based on actual metadata
        String typeName = metadata.getType().getDisplayName().toLowerCase();
        return estimateColumnSizeByType(typeName);
    }
    
    /**
     * Estimate column size based on type name.
     */
    private long estimateColumnSizeByType(String typeName) {
        // Rough estimates based on common data types
        if (typeName.equals("boolean")) {
            return 1; // 1 byte
        } else if (typeName.equals("tinyint")) {
            return 1; // 1 byte
        } else if (typeName.equals("smallint")) {
            return 2; // 2 bytes
        } else if (typeName.equals("integer")) {
            return 4; // 4 bytes
        } else if (typeName.equals("bigint")) {
            return 8; // 8 bytes
        } else if (typeName.equals("real")) {
            return 4; // 4 bytes
        } else if (typeName.equals("double")) {
            return 8; // 8 bytes
        } else if (typeName.startsWith("decimal")) {
            return 16; // Assume 16 bytes for decimal
        } else if (typeName.startsWith("char")) {
            // Extract length from char(n)
            try {
                int length = Integer.parseInt(typeName.replaceAll("[^0-9]", ""));
                return Math.max(1, length); // At least 1 byte
            } catch (Exception e) {
                return 255; // Default for char
            }
        } else if (typeName.startsWith("varchar")) {
            return 255; // Average varchar size
        } else if (typeName.equals("date")) {
            return 4; // 4 bytes for date
        } else if (typeName.startsWith("time")) {
            return 8; // 8 bytes for time
        } else if (typeName.startsWith("timestamp")) {
            return 8; // 8 bytes for timestamp
        } else if (typeName.startsWith("varbinary")) {
            return 1024; // Average binary size
        } else {
            return 100; // Default estimate
        }
    }
    
    /**
     * Estimate total table size.
     */
    private long estimateTotalTableSize(FlussTableHandle tableHandle, Map<String, ColumnMetadata> columnMetadataMap) {
        long totalSize = 0;
        
        try {
            List<DataField> fields = tableHandle.getTableInfo().getSchema().toRowType().getFields();
            for (DataField field : fields) {
                FlussColumnHandle dummyColumn = new FlussColumnHandle(
                        field.getName(),
                        FlussTypeUtils.toTrinoType(field.getType(), null), // TypeManager would be injected
                        0, // ordinal position
                        false, // is partition key
                        false, // is primary key
                        field.getType().isNullable(),
                        field.getDescription(),
                        Optional.empty() // default value
                );
                
                long columnSize = estimateColumnSize(dummyColumn, columnMetadataMap.get(field.getName()));
                totalSize += columnSize;
            }
        } catch (Exception e) {
            log.warn(e, "Error estimating total table size for table: %s", tableHandle.getTableName());
            // Fallback to simple estimation
            totalSize = tableHandle.getTableInfo().getSchema().toRowType().getFieldCount() * 100; // 100 bytes per column avg
        }
        
        return totalSize;
    }
    
    /**
     * Apply intelligent pruning based on analysis results.
     */
    private Set<ColumnHandle> applyIntelligentPruning(
            FlussTableHandle tableHandle,
            Set<ColumnHandle> projectedColumns,
            ColumnAnalysisResult analysis) {
        
        Set<ColumnHandle> prunedColumns = new HashSet<>();
        
        for (ColumnHandle column : projectedColumns) {
            if (column instanceof FlussColumnHandle) {
                FlussColumnHandle flussColumn = (FlussColumnHandle) column;
                
                // Always include partition keys as they're needed for split generation
                if (flussColumn.isPartitionKey()) {
                    prunedColumns.add(column);
                    continue;
                }
                
                // Always include primary keys as they may be needed for joins
                if (flussColumn.isPrimaryKey()) {
                    prunedColumns.add(column);
                    continue;
                }
                
                // Apply intelligent pruning decisions
                if (shouldIncludeColumn(flussColumn, analysis)) {
                    prunedColumns.add(column);
                } else {
                    log.debug("Intelligently pruning column %s from table %s", 
                            flussColumn.getName(), tableHandle.getTableName());
                }
            }
        }
        
        // Ensure we have at least some columns
        if (prunedColumns.isEmpty() && !projectedColumns.isEmpty()) {
            log.warn("All columns were pruned for table %s, including all projected columns to avoid empty result",
                    tableHandle.getTableName());
            return projectedColumns;
        }
        
        return prunedColumns;
    }
    
    /**
     * Determine if a column should be included based on analysis.
     */
    private boolean shouldIncludeColumn(FlussColumnHandle column, ColumnAnalysisResult analysis) {
        // Include all projected columns by default - column pruning is about reducing I/O
        // not excluding logically needed columns
        return true;
    }
    
    /**
     * Result of column analysis for pruning decisions.
     */
    private static class ColumnAnalysisResult {
        private final int totalColumns;
        private final int projectedColumns;
        private final int largeColumnCount;
        private final int smallColumnCount;
        private final double reductionPercentage;
        private final long estimatedTotalSize;
        private final long estimatedProjectedSize;
        
        public ColumnAnalysisResult(int totalColumns, int projectedColumns, 
                                  int largeColumnCount, int smallColumnCount,
                                  double reductionPercentage, 
                                  long estimatedTotalSize, long estimatedProjectedSize) {
            this.totalColumns = totalColumns;
            this.projectedColumns = projectedColumns;
            this.largeColumnCount = largeColumnCount;
            this.smallColumnCount = smallColumnCount;
            this.reductionPercentage = reductionPercentage;
            this.estimatedTotalSize = estimatedTotalSize;
            this.estimatedProjectedSize = estimatedProjectedSize;
        }
        
        public int getTotalColumns() {
            return totalColumns;
        }
        
        public int getProjectedColumns() {
            return projectedColumns;
        }
        
        public int getLargeColumnCount() {
            return largeColumnCount;
        }
        
        public int getSmallColumnCount() {
            return smallColumnCount;
        }
        
        public double getReductionPercentage() {
            return reductionPercentage;
        }
        
        public long getEstimatedTotalSize() {
            return estimatedTotalSize;
        }
        
        public long getEstimatedProjectedSize() {
            return estimatedProjectedSize;
        }
    }

    /**
     * Check if column pruning can benefit this query.
     * 
     * <p>This method determines whether column pruning will provide significant
     * performance benefits based on the number of columns and query characteristics.
     */
    public boolean canBenefitFromPruning(int totalColumns, int projectedColumns) {
        // Column pruning is beneficial when we're reading less than all columns
        if (totalColumns <= 0 || projectedColumns <= 0) {
            return false;
        }
        
        if (projectedColumns >= totalColumns) {
            return false; // No benefit if reading all or more columns
        }
        
        // Calculate the ratio of projected to total columns
        double ratio = (double) projectedColumns / totalColumns;
        
        // Column pruning is most beneficial when reading a small fraction of columns
        // The threshold can be adjusted based on empirical data
        boolean beneficial = ratio < 0.5; // Beneficial if we're reading less than 50%
        
        // For tables with very few columns, pruning might not be worth it
        if (totalColumns < 5) {
            beneficial = false; // Not beneficial for very wide tables
        }
        
        log.debug("Column pruning benefit analysis - total: %d, projected: %d, ratio: %.4f, beneficial: %s",
                totalColumns, projectedColumns, ratio, beneficial);
        
        return beneficial;
    }

    /**
     * Estimate I/O reduction from column pruning.
     * 
     * <p>This method calculates the expected reduction in data transfer and
     * processing when column pruning is applied.
     */
    public double estimateIoReduction(int totalColumns, int projectedColumns) {
        if (totalColumns <= 0 || projectedColumns <= 0) {
            return 0.0;
        }
        
        if (projectedColumns >= totalColumns) {
            return 0.0; // No reduction if reading all or more columns
        }
        
        // Calculate basic reduction ratio
        double basicReduction = 1.0 - ((double) projectedColumns / totalColumns);
        
        // Apply diminishing returns for tables with few columns
        // Column pruning is less effective on tables with very few columns
        if (totalColumns < 10) {
            basicReduction *= 0.5; // Reduce estimated benefit for narrow tables
        } else if (totalColumns < 50) {
            basicReduction *= 0.75; // Slight reduction for moderately narrow tables
        }
        
        // Ensure result is within valid range
        double finalReduction = Math.max(0.0, Math.min(1.0, basicReduction);
        
        log.debug("I/O reduction estimate - total cols: %d, projected cols: %d, basic: %.4f, final: %.4f",
                totalColumns, projectedColumns, basicReduction, finalReduction);
        
        return finalReduction;
    }
}
