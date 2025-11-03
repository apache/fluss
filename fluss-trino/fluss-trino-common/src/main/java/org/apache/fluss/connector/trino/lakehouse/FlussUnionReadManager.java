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
     */
    public UnionReadStrategy determineStrategy(FlussTableHandle tableHandle) {
        if (!isUnionReadApplicable(tableHandle)) {
            return UnionReadStrategy.REAL_TIME_ONLY;
        }

        // In a full implementation, we would analyze query predicates,
        // time ranges, and data distribution to determine the optimal strategy
        
        return UnionReadStrategy.UNION;
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
     */
    public Optional<Long> getTimeBoundary(FlussTableHandle tableHandle) {
        // In a full implementation, this would query Fluss metadata
        // to get the earliest timestamp of real-time data
        return Optional.empty();
    }
}
