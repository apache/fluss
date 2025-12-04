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

package org.apache.fluss.connector.trino.config;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

/**
 * Configuration for Fluss Trino connector.
 */
public class FlussConnectorConfig {

    private String bootstrapServers;
    private Duration connectionMaxIdleTime = new Duration(10, TimeUnit.MINUTES);
    private Duration requestTimeout = new Duration(60, TimeUnit.SECONDS);
    private Duration scannerFetchMaxWaitTime = new Duration(500, TimeUnit.MILLISECONDS);
    private String scannerFetchMinBytes = "1MB";
    private boolean unionReadEnabled = true;
    private boolean columnPruningEnabled = true;
    private boolean predicatePushdownEnabled = true;
    private boolean limitPushdownEnabled = true;
    private boolean aggregatePushdownEnabled = false;
    private int maxSplitsPerSecond = 1000;
    private int maxSplitsPerRequest = 100;

    @NotNull
    public String getBootstrapServers() {
        return bootstrapServers;
    }

    @Config("bootstrap.servers")
    @ConfigDescription("Comma-separated list of Fluss server addresses (host:port)")
    public FlussConnectorConfig setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        return this;
    }

    @NotNull
    @MinDuration("1s")
    public Duration getConnectionMaxIdleTime() {
        return connectionMaxIdleTime;
    }

    @Config("connection.max-idle-time")
    @ConfigDescription("Maximum idle time for connections")
    public FlussConnectorConfig setConnectionMaxIdleTime(Duration connectionMaxIdleTime) {
        this.connectionMaxIdleTime = connectionMaxIdleTime;
        return this;
    }

    @NotNull
    @MinDuration("1s")
    public Duration getRequestTimeout() {
        return requestTimeout;
    }

    @Config("request.timeout")
    @ConfigDescription("Request timeout duration")
    public FlussConnectorConfig setRequestTimeout(Duration requestTimeout) {
        this.requestTimeout = requestTimeout;
        return this;
    }

    @NotNull
    public Duration getScannerFetchMaxWaitTime() {
        return scannerFetchMaxWaitTime;
    }

    @Config("scanner.fetch.max-wait-time")
    @ConfigDescription("Maximum wait time for scanner fetch operations")
    public FlussConnectorConfig setScannerFetchMaxWaitTime(Duration scannerFetchMaxWaitTime) {
        this.scannerFetchMaxWaitTime = scannerFetchMaxWaitTime;
        return this;
    }

    @NotNull
    public String getScannerFetchMinBytes() {
        return scannerFetchMinBytes;
    }

    @Config("scanner.fetch.min-bytes")
    @ConfigDescription("Minimum bytes to fetch in scanner operations")
    public FlussConnectorConfig setScannerFetchMinBytes(String scannerFetchMinBytes) {
        this.scannerFetchMinBytes = scannerFetchMinBytes;
        return this;
    }

    public boolean isUnionReadEnabled() {
        return unionReadEnabled;
    }

    @Config("union-read.enabled")
    @ConfigDescription("Enable Union Read feature for querying both real-time and historical data")
    public FlussConnectorConfig setUnionReadEnabled(boolean unionReadEnabled) {
        this.unionReadEnabled = unionReadEnabled;
        return this;
    }

    public boolean isColumnPruningEnabled() {
        return columnPruningEnabled;
    }

    @Config("column-pruning.enabled")
    @ConfigDescription("Enable column pruning optimization")
    public FlussConnectorConfig setColumnPruningEnabled(boolean columnPruningEnabled) {
        this.columnPruningEnabled = columnPruningEnabled;
        return this;
    }

    public boolean isPredicatePushdownEnabled() {
        return predicatePushdownEnabled;
    }

    @Config("predicate-pushdown.enabled")
    @ConfigDescription("Enable predicate pushdown optimization")
    public FlussConnectorConfig setPredicatePushdownEnabled(boolean predicatePushdownEnabled) {
        this.predicatePushdownEnabled = predicatePushdownEnabled;
        return this;
    }

    public boolean isLimitPushdownEnabled() {
        return limitPushdownEnabled;
    }

    @Config("limit-pushdown.enabled")
    @ConfigDescription("Enable limit pushdown optimization")
    public FlussConnectorConfig setLimitPushdownEnabled(boolean limitPushdownEnabled) {
        this.limitPushdownEnabled = limitPushdownEnabled;
        return this;
    }

    public boolean isAggregatePushdownEnabled() {
        return aggregatePushdownEnabled;
    }

    @Config("aggregate-pushdown.enabled")
    @ConfigDescription("Enable aggregate pushdown optimization (experimental)")
    public FlussConnectorConfig setAggregatePushdownEnabled(boolean aggregatePushdownEnabled) {
        this.aggregatePushdownEnabled = aggregatePushdownEnabled;
        return this;
    }

    public int getMaxSplitsPerSecond() {
        return maxSplitsPerSecond;
    }

    @Config("max-splits-per-second")
    @ConfigDescription("Maximum number of splits per second")
    public FlussConnectorConfig setMaxSplitsPerSecond(int maxSplitsPerSecond) {
        this.maxSplitsPerSecond = maxSplitsPerSecond;
        return this;
    }

    public int getMaxSplitsPerRequest() {
        return maxSplitsPerRequest;
    }

    @Config("max-splits-per-request")
    @ConfigDescription("Maximum number of splits per request")
    public FlussConnectorConfig setMaxSplitsPerRequest(int maxSplitsPerRequest) {
        this.maxSplitsPerRequest = maxSplitsPerRequest;
        return this;
    }
}
