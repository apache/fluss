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

package org.apache.fluss.connector.trino.lakehouse;

import org.apache.fluss.connector.trino.config.FlussConnectorConfig;
import org.apache.fluss.connector.trino.connection.FlussClientManager;
import org.apache.fluss.connector.trino.handle.FlussTableHandle;
import org.apache.fluss.metadata.TableInfo;

import io.airlift.log.Logger;

import javax.inject.Inject;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Manager for Union Read functionality.
 * 
 * <p>Union Read enables querying both real-time data from Fluss and historical
 * data from Lakehouse storage (e.g., Paimon) in a single query.
 */
public class FlussUnionReadManager {

    private static final Logger log = Logger.get(FlussUnionReadManager.class);

    private final FlussClientManager clientManager;
    private final FlussConnectorConfig config;
    private final FlussLakehouseReader lakehouseReader;

    @Inject
    public FlussUnionReadManager(
            FlussClientManager clientManager,
            FlussConnectorConfig config,
            FlussLakehouseReader lakehouseReader) {
        this.clientManager = requireNonNull(clientManager, "clientManager is null");
        this.config = requireNonNull(config, "config is null");
        this.lakehouseReader = requireNonNull(lakehouseReader, "lakehouseReader is null");
    }

    /**
     * Check if Union Read is enabled and applicable for this table.
     */
    public boolean isUnionReadApplicable(FlussTableHandle tableHandle) {
        if (!config.isUnionReadEnabled()) {
            log.debug("Union Read is disabled");
            return false;
        }

        TableInfo tableInfo = tableHandle.getTableInfo();
        
        // Check if table has lakehouse configuration
        Optional<String> lakehouseFormat = tableInfo.getTableDescriptor()
                .getCustomProperties()
                .map(props -> props.get("datalake.format"));
        
        if (lakehouseFormat.isEmpty()) {
            log.debug("Table %s has no lakehouse configuration", tableHandle.getTableName());
            return false;
        }

        log.debug("Union Read is applicable for table: %s with format: %s",
                tableHandle.getTableName(), lakehouseFormat.get());
        return true;
    }

    /**
     * Get the lakehouse reader for historical data.
     */
    public FlussLakehouseReader getLakehouseReader() {
        return lakehouseReader;
    }

    /**
     * Determine the data split strategy for Union Read.
     * 
     * <p>This method analyzes the query characteristics and table properties
     * to determine the optimal read strategy.
     */
    public UnionReadStrategy determineStrategy(FlussTableHandle tableHandle) {
        if (!isUnionReadApplicable(tableHandle)) {
            return UnionReadStrategy.REAL_TIME_ONLY;
        }

        // Check if predicates indicate a historical-only query
        if (isHistoricalOnlyQuery(tableHandle)) {
            log.debug("Using HISTORICAL_ONLY strategy for table: %s", 
                    tableHandle.getTableName());
            return UnionReadStrategy.HISTORICAL_ONLY;
        }
        
        // Check if predicates indicate a real-time-only query
        if (isRealTimeOnlyQuery(tableHandle)) {
            log.debug("Using REAL_TIME_ONLY strategy for table: %s", 
                    tableHandle.getTableName());
            return UnionReadStrategy.REAL_TIME_ONLY;
        }
        
        // Default to union read for best coverage
        log.debug("Using UNION strategy for table: %s", tableHandle.getTableName());
        return UnionReadStrategy.UNION;
    }
    
    /**
     * Check if the query should only read historical data.
     */
    private boolean isHistoricalOnlyQuery(FlussTableHandle tableHandle) {
        // Analyze time-based predicates to determine if query is historical
        // For example, if there's a predicate like "date < '2024-01-01'" and
        // current lakehouse boundary is '2024-01-01', then it's historical only
        
        Optional<Long> timeBoundary = getTimeBoundary(tableHandle);
        if (timeBoundary.isEmpty()) {
            return false;
        }
        
        // Check if all predicates indicate data before the boundary
        // This would require analyzing the constraint domains
        // For now, return false to be safe
        return false;
    }
    
    /**
     * Check if the query should only read real-time data.
     */
    private boolean isRealTimeOnlyQuery(FlussTableHandle tableHandle) {
        // If there's a small limit and no complex predicates,
        // reading from real-time storage might be more efficient
        
        if (tableHandle.getLimit().isPresent()) {
            long limit = tableHandle.getLimit().get();
            // For very small limits, real-time only might be faster
            if (limit <= 100) {
                log.debug("Small limit (%d), preferring real-time read", limit);
                return true;
            }
        }
        
        // If predicates indicate recent data, use real-time only
        Optional<Long> timeBoundary = getTimeBoundary(tableHandle);
        if (timeBoundary.isPresent()) {
            // Check if predicates filter for data after boundary
            // Implementation would analyze constraint domains
        }
        
        return false;
    }

    /**
     * Union Read strategies.
     */
    public enum UnionReadStrategy {
        /** Only read from real-time storage (Fluss) */
        REAL_TIME_ONLY,
        
        /** Only read from historical storage (Lakehouse) */
        HISTORICAL_ONLY,
        
        /** Read from both and union the results */
        UNION
    }

    /**
     * Get the time boundary between real-time and historical data.
     * 
     * <p>This boundary represents the cutoff point where data older than this
     * timestamp is stored in the lakehouse, and newer data is in real-time storage.
     */
    public Optional<Long> getTimeBoundary(FlussTableHandle tableHandle) {
        try {
            // In a production implementation, this would:
            // 1. Query Fluss metadata to get the lakehouse sync timestamp
            // 2. Get the earliest timestamp in real-time LogStore
            // 3. Return the boundary timestamp
            
            TableInfo tableInfo = tableHandle.getTableInfo();
            
            // Check if table has time-based partition or timestamp column
            // that can be used to determine the boundary
            Optional<String> timestampColumn = findTimestampColumn(tableInfo);
            
            if (timestampColumn.isEmpty()) {
                log.debug("No timestamp column found for table: %s", 
                        tableHandle.getTableName());
                return Optional.empty();
            }
            
            // Get the lakehouse sync configuration
            Optional<String> syncTimestamp = tableInfo.getTableDescriptor()
                    .getCustomProperties()
                    .flatMap(props -> Optional.ofNullable(props.get("lakehouse.sync.timestamp")));
            
            if (syncTimestamp.isPresent()) {
                try {
                    long boundary = Long.parseLong(syncTimestamp.get());
                    log.debug("Time boundary for table %s: %d", 
                            tableHandle.getTableName(), boundary);
                    return Optional.of(boundary);
                } catch (NumberFormatException e) {
                    log.warn(e, "Invalid lakehouse sync timestamp: %s", syncTimestamp.get());
                }
            }
            
            return Optional.empty();
        } catch (Exception e) {
            log.warn(e, "Error getting time boundary for table: %s", 
                    tableHandle.getTableName());
            return Optional.empty();
        }
    }
    
    /**
     * Find timestamp column in table schema.
     */
    private Optional<String> findTimestampColumn(TableInfo tableInfo) {
        List<org.apache.fluss.types.DataField> fields = 
                tableInfo.getSchema().toRowType().getFields();
        
        for (org.apache.fluss.types.DataField field : fields) {
            org.apache.fluss.types.DataType type = field.getType();
            // Check if it's a timestamp type
            if (type.is(org.apache.fluss.types.DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE) ||
                type.is(org.apache.fluss.types.DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)) {
                return Optional.of(field.getName());
            }
        }
        
        // Also check for common timestamp column names
        for (org.apache.fluss.types.DataField field : fields) {
            String name = field.getName().toLowerCase();
            if (name.contains("time") || name.contains("date") || 
                name.equals("ts") || name.equals("timestamp")) {
                return Optional.of(field.getName());
            }
        }
        
        return Optional.empty();
    }
}
