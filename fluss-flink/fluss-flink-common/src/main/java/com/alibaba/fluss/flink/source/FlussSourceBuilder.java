/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.source;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.flink.FlinkConnectorOptions;
import com.alibaba.fluss.flink.source.deserializer.FlussDeserializationSchema;
import com.alibaba.fluss.flink.source.enumerator.initializer.OffsetsInitializer;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.types.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Builder class for creating {@link FlussSource} instances.
 *
 * <p>The builder llows for step-by-step configuration of a Fluss source connector. It handles the
 * setup of connection parameters, table metadata retrieval, and source configuration.
 *
 * <p>Sample usage:
 *
 * <pre>{@code
 * FlussSource<Order> source = FlussSource.<Order>builder()
 *     .setBootstrapServers("localhost:9092")
 *     .setDatabase("mydb")
 *     .setTable("orders")
 *     .setScanPartitionDiscoveryIntervalMs(1000L)
 *     .setStartingOffsets(OffsetsInitializer.earliest())
 *     .setDeserializationSchema(new OrderDeserializationSchema())
 *     .build();
 * }</pre>
 *
 * @param <OUT> The type of records produced by the source being built
 */
public class FlussSourceBuilder<OUT> {
    private static final Logger LOG = LoggerFactory.getLogger(FlussSourceBuilder.class);

    private Configuration flussConf;

    private int[] projectedFields;
    private Long scanPartitionDiscoveryIntervalMs;
    private OffsetsInitializer offsetsInitializer;
    private FlussDeserializationSchema<OUT> deserializationSchema;

    private String bootstrapServers;

    private String database;
    private String tableName;

    public FlussSourceBuilder<OUT> setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        return this;
    }

    public FlussSourceBuilder<OUT> setDatabase(String database) {
        this.database = database;
        return this;
    }

    public FlussSourceBuilder<OUT> setTable(String table) {
        this.tableName = table;
        return this;
    }

    public FlussSourceBuilder<OUT> setScanPartitionDiscoveryIntervalMs(
            long scanPartitionDiscoveryIntervalMs) {
        this.scanPartitionDiscoveryIntervalMs = scanPartitionDiscoveryIntervalMs;
        return this;
    }

    public FlussSourceBuilder<OUT> setStartingOffsets(OffsetsInitializer offsetsInitializer) {
        this.offsetsInitializer = offsetsInitializer;
        return this;
    }

    public FlussSourceBuilder<OUT> setDeserializationSchema(
            FlussDeserializationSchema<OUT> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
        return this;
    }

    // TODO: refactor this method to use field names instead of indexes
    //  see: https://github.com/alibaba/fluss/issues/804
    public FlussSourceBuilder<OUT> setProjectedFields(int[] projectedFields) {
        this.projectedFields = projectedFields;
        return this;
    }

    public FlussSourceBuilder<OUT> setFlussConfig(Configuration flussConf) {
        this.flussConf = flussConf;
        return this;
    }

    public FlussSource<OUT> build() {
        checkNotNull(bootstrapServers, "BootstrapServers is required but not provided.");
        checkNotNull(database, "Database is required but not provided.");
        if (database.isEmpty()) {
            throw new IllegalArgumentException("Database must not be empty.");
        }
        checkNotNull(tableName, "TableName is required but not provided.");
        if (tableName.isEmpty()) {
            throw new IllegalArgumentException("TableName must not be empty.");
        }
        checkNotNull(deserializationSchema, "Deserialization schema is required but not provided.");

        // if null use the default value:
        if (offsetsInitializer == null) {
            offsetsInitializer = OffsetsInitializer.initial();
        }

        // if null use the default value:
        if (scanPartitionDiscoveryIntervalMs == null) {
            scanPartitionDiscoveryIntervalMs =
                    FlinkConnectorOptions.SCAN_PARTITION_DISCOVERY_INTERVAL
                            .defaultValue()
                            .toMillis();
        }

        if (this.flussConf == null) {
            this.flussConf = new Configuration();
        }

        TablePath tablePath = new TablePath(this.database, this.tableName);
        this.flussConf.setString(ConfigOptions.BOOTSTRAP_SERVERS.key(), bootstrapServers);

        TableInfo tableInfo;
        try (Connection connection = ConnectionFactory.createConnection(flussConf);
                Admin admin = connection.getAdmin()) {
            try {
                tableInfo = admin.getTableInfo(tablePath).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while getting table info", e);
            } catch (ExecutionException e) {
                throw new RuntimeException("Failed to get table info", e);
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to initialize FlussSource admin connection: " + e.getMessage(), e);
        }

        flussConf.addAll(tableInfo.getCustomProperties());
        flussConf.addAll(tableInfo.getProperties());

        boolean isPartitioned = !tableInfo.getPartitionKeys().isEmpty();
        boolean hasPrimaryKey = !tableInfo.getPrimaryKeys().isEmpty();

        RowType sourceOutputType =
                projectedFields != null
                        ? tableInfo.getRowType().project(projectedFields)
                        : tableInfo.getRowType();

        LOG.info("Creating Fluss Source with Configuration: {}", flussConf);

        return new FlussSource<>(
                flussConf,
                tablePath,
                hasPrimaryKey,
                isPartitioned,
                sourceOutputType,
                projectedFields,
                offsetsInitializer,
                scanPartitionDiscoveryIntervalMs,
                deserializationSchema,
                true);
    }
}
