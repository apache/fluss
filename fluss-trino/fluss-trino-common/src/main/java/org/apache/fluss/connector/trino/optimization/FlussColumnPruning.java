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

import io.airlift.log.Logger;
import io.trino.spi.connector.ColumnHandle;

import javax.inject.Inject;

import java.util.HashSet;
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
        
        // Filter out columns that cannot be pruned
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
                
                // Include all other projected columns
                prunedColumns.add(column);
            }
        }
        
        log.debug("Pruned columns for table %s: %d -> %d", 
                tableHandle.getTableName(), projectedColumns.size(), prunedColumns.size());
        
        return prunedColumns;
    }

    /**
     * Check if column pruning can benefit this query.
     */
    public boolean canBenefitFromPruning(int totalColumns, int projectedColumns) {
        // Column pruning is beneficial when we're reading less than all columns
        return projectedColumns < totalColumns && projectedColumns > 0;
    }

    /**
     * Estimate I/O reduction from column pruning.
     */
    public double estimateIoReduction(int totalColumns, int projectedColumns) {
        if (totalColumns == 0 || projectedColumns >= totalColumns) {
            return 0.0;
        }
        return 1.0 - ((double) projectedColumns / totalColumns);
    }
}
