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

package org.apache.fluss.connector.trino.connection;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.connector.trino.config.FlussConnectorConfig;
import org.apache.fluss.metadata.TablePath;

import io.airlift.log.Logger;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.Arrays;

import static org.apache.fluss.config.ConfigOptions.BOOTSTRAP_SERVERS;
import static org.apache.fluss.config.ConfigOptions.CLIENT_REQUEST_TIMEOUT;

/**
 * Manager for Fluss client connections.
 * 
 * <p>This class manages the lifecycle of Fluss connections and provides access to
 * Admin and Table clients.
 */
public class FlussClientManager {

    private static final Logger log = Logger.get(FlussClientManager.class);

    private final Connection connection;
    private final Configuration flussConfig;
    private final FlussConnectorConfig connectorConfig;

    @Inject
    public FlussClientManager(FlussConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
        this.flussConfig = createFlussConfiguration(connectorConfig);
        
        try {
            log.info("Creating Fluss connection with bootstrap servers: %s", 
                    connectorConfig.getBootstrapServers());
            this.connection = ConnectionFactory.createConnection(flussConfig);
            log.info("Fluss connection created successfully");
        } catch (Exception e) {
            log.error(e, "Failed to create Fluss connection to servers: %s", 
                    connectorConfig.getBootstrapServers());
            throw new RuntimeException("Failed to create Fluss connection", e);
        }
    }

    /**
     * Get an Admin client for metadata operations.
     * 
     * <p>Note: The caller is responsible for closing the returned Admin client.
     */
    public Admin getAdmin() {
        if (connection == null) {
            throw new IllegalStateException("Fluss connection is not available");
        }
        return connection.getAdmin();
    }

    /**
     * Get a Table client for data operations.
     * 
     * <p>Note: The caller is responsible for closing the returned Table client.
     */
    public Table getTable(TablePath tablePath) {
        if (connection == null) {
            throw new IllegalStateException("Fluss connection is not available");
        }
        return connection.getTable(tablePath);
    }

    /**
     * Get the Fluss configuration.
     */
    public Configuration getConfiguration() {
        return flussConfig;
    }

    /**
     * Get the connector configuration.
     */
    public FlussConnectorConfig getConnectorConfig() {
        return connectorConfig;
    }

    /**
     * Close the connection and release all resources.
     */
    @PreDestroy
    public void close() {
        try {
            if (connection != null) {
                log.info("Closing Fluss connection");
                connection.close();
                log.info("Fluss connection closed successfully");
            }
        } catch (Exception e) {
            log.error(e, "Error closing Fluss connection");
        }
    }

    /**
     * Create Fluss configuration from connector config.
     */
    private static Configuration createFlussConfiguration(FlussConnectorConfig config) {
        Configuration flussConfig = new Configuration();
        
        // Bootstrap servers (required)
        String bootstrapServers = config.getBootstrapServers();
        if (bootstrapServers == null || bootstrapServers.trim().isEmpty()) {
            throw new IllegalArgumentException("bootstrap.servers is required and cannot be empty");
        }
        flussConfig.set(BOOTSTRAP_SERVERS, Arrays.asList(bootstrapServers.split(",")));
        
        // Request timeout
        flussConfig.setString(
                CLIENT_REQUEST_TIMEOUT.key(),
                config.getRequestTimeout().toString());
        
        // Connection max idle time
        flussConfig.setString(
                "client.connection.max-idle-time",
                config.getConnectionMaxIdleTime().toString());
        
        // Scanner configurations
        flussConfig.setString(
                "client.scanner.fetch.max-wait-time",
                config.getScannerFetchMaxWaitTime().toString());
        
        flussConfig.setString(
                "client.scanner.fetch.min-bytes",
                config.getScannerFetchMinBytes());
        
        // Union Read configuration
        flussConfig.setString(
                "client.union.read.enabled",
                String.valueOf(config.isUnionReadEnabled()));
        
        // Column pruning configuration
        flussConfig.setString(
                "client.column.pruning.enabled",
                String.valueOf(config.isColumnPruningEnabled()));
        
        // Predicate pushdown configuration
        flussConfig.setString(
                "client.predicate.pushdown.enabled",
                String.valueOf(config.isPredicatePushdownEnabled()));
        
        // Limit pushdown configuration
        flussConfig.setString(
                "client.limit.pushdown.enabled",
                String.valueOf(config.isLimitPushdownEnabled()));
        
        // Aggregate pushdown configuration
        flussConfig.setString(
                "client.aggregate.pushdown.enabled",
                String.valueOf(config.isAggregatePushdownEnabled()));
        
        // Performance tuning configurations
        flussConfig.setString(
                "client.max.splits.per.second",
                String.valueOf(config.getMaxSplitsPerSecond()));
        
        flussConfig.setString(
                "client.max.splits.per.request",
                String.valueOf(config.getMaxSplitsPerRequest()));
        
        return flussConfig;
    }
}
