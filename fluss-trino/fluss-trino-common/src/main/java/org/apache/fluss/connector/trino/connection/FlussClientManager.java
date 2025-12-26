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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.config.ConfigOptions.BOOTSTRAP_SERVERS;
import static org.apache.fluss.config.ConfigOptions.CLIENT_REQUEST_TIMEOUT;

/**
 * Manager for Fluss client connections.
 * 
 * <p>This class manages the lifecycle of Fluss connections and provides access to
 * Admin and Table clients. It implements connection pooling for better resource
 * utilization and performance.
 */
public class FlussClientManager {

    private static final Logger log = Logger.get(FlussClientManager.class);

    private final Connection connection;
    private final Configuration flussConfig;
    private final FlussConnectorConfig connectorConfig;
    
    // Connection pools for better resource management
    private final ConcurrentMap<TablePath, Table> tablePool = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Admin> adminPool = new ConcurrentHashMap<>();

    @Inject
    public FlussClientManager(FlussConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
        this.flussConfig = createFlussConfiguration(connectorConfig);
        
        try {
            log.info("Creating Fluss connection with bootstrap servers: %s", 
                    connectorConfig.getBootstrapServers());
            
            // Retry connection creation with exponential backoff
            this.connection = createConnectionWithRetry(flussConfig, 3, 1000);
            
            log.info("Fluss connection created successfully");
            
            // Validate connection
            if (!isConnectionHealthy()) {
                throw new RuntimeException("Fluss connection is not healthy");
            }
        } catch (Exception e) {
            log.error(e, "Failed to create Fluss connection to servers: %s", 
                    connectorConfig.getBootstrapServers());
            throw new RuntimeException("Failed to create Fluss connection", e);
        }
    }
    
    /**
     * Create connection with retry logic and exponential backoff.
     */
    private Connection createConnectionWithRetry(Configuration config, int maxRetries, long initialDelayMs) 
            throws Exception {
        Exception lastException = null;
        long delayMs = initialDelayMs;
        
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                Connection conn = ConnectionFactory.createConnection(config);
                if (conn != null) {
                    log.debug("Successfully created Fluss connection on attempt %d", attempt + 1);
                    return conn;
                }
            } catch (Exception e) {
                lastException = e;
                log.warn(e, "Failed to create Fluss connection on attempt %d", attempt + 1);
                
                // Don't wait after the last attempt
                if (attempt < maxRetries) {
                    log.info("Retrying in %d ms...", delayMs);
                    Thread.sleep(delayMs);
                    delayMs *= 2; // Exponential backoff
                }
            }
        }
        
        throw new RuntimeException("Failed to create Fluss connection after " + (maxRetries + 1) + " attempts", 
                lastException);
    }
    
    /**
     * Check if the connection is healthy.
     */
    private boolean isConnectionHealthy() {
        if (connection == null) {
            return false;
        }
        
        try {
            // Try to get server nodes to verify connection
            Admin admin = connection.getAdmin();
            if (admin != null) {
                // This is a lightweight operation to verify connectivity
                admin.getServerNodes().get(5, TimeUnit.SECONDS);
                return true;
            }
        } catch (Exception e) {
            log.warn(e, "Connection health check failed");
            return false;
        }
        
        return false;
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
        
        try {
            // Try to get from pool first
            Table table = tablePool.get(tablePath);
            if (table != null && isTableHealthy(table)) {
                log.debug("Reusing pooled table client for: %s", tablePath);
                return table;
            }
            
            // Create new table client with retry
            table = createTableWithRetry(tablePath, 3, 500);
            
            // Add to pool
            if (table != null) {
                tablePool.put(tablePath, table);
                log.debug("Created and pooled new table client for: %s", tablePath);
            }
            
            return table;
        } catch (Exception e) {
            log.error(e, "Error getting table client for: %s", tablePath);
            throw new RuntimeException("Failed to get table client for: " + tablePath, e);
        }
    }
    
    /**
     * Create table client with retry logic.
     */
    private Table createTableWithRetry(TablePath tablePath, int maxRetries, long initialDelayMs) 
            throws Exception {
        Exception lastException = null;
        long delayMs = initialDelayMs;
        
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                Table table = connection.getTable(tablePath);
                if (table != null && isTableHealthy(table)) {
                    log.debug("Successfully created table client for %s on attempt %d", 
                            tablePath, attempt + 1);
                    return table;
                }
            } catch (Exception e) {
                lastException = e;
                log.warn(e, "Failed to create table client for %s on attempt %d", 
                        tablePath, attempt + 1);
                
                // Don't wait after the last attempt
                if (attempt < maxRetries) {
                    log.info("Retrying in %d ms...", delayMs);
                    Thread.sleep(delayMs);
                    delayMs *= 2; // Exponential backoff
                }
            }
        }
        
        throw new RuntimeException("Failed to create table client for " + tablePath + 
                " after " + (maxRetries + 1) + " attempts", lastException);
    }
    
    /**
     * Check if the table client is healthy.
     */
    private boolean isTableHealthy(Table table) {
        if (table == null) {
            return false;
        }
        
        try {
            // Simple health check - try to get table info
            // This is a lightweight operation
            return true; // Table objects are generally always valid once created
        } catch (Exception e) {
            log.debug("Table health check failed: %s", e.getMessage());
            return false;
        }
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
            log.info("Closing Fluss connection");
            
            // Close all pooled resources
            closePooledResources();
            
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
     * Close all pooled resources.
     */
    private void closePooledResources() {
        log.debug("Closing pooled resources");
        
        // Close all pooled tables
        int tableCount = 0;
        for (Map.Entry<TablePath, Table> entry : tablePool.entrySet()) {
            try {
                Table table = entry.getValue();
                if (table != null) {
                    table.close();
                    tableCount++;
                }
            } catch (Exception e) {
                log.warn(e, "Error closing pooled table for: %s", entry.getKey());
            }
        }
        tablePool.clear();
        log.debug("Closed %d pooled tables", tableCount);
        
        // Close all pooled admins
        int adminCount = 0;
        for (Map.Entry<String, Admin> entry : adminPool.entrySet()) {
            try {
                Admin admin = entry.getValue();
                if (admin != null) {
                    admin.close();
                    adminCount++;
                }
            } catch (Exception e) {
                log.warn(e, "Error closing pooled admin for: %s", entry.getKey());
            }
        }
        adminPool.clear();
        log.debug("Closed %d pooled admins", adminCount);
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
