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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Builder class for creating {@link FlussSource} instances.
 *
 * <p>The builder allows for step-by-step configuration of a Fluss source connector. It handles the
 * setup of connection parameters, table metadata retrieval, and source configuration.
 *
 * <p>Sample usage:
 *
 * <pre>{@code
 * FlussSource<Order> source = FlussSource.<Order>builder()
 *     .setBootstrapServers("localhost:9092")
 *     .setDatabase("mydb")
 *     .setTable("orders")
 *     .setProjectedFields("orderId", "amount")
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
    private String[] projectedFieldNames;
    private Long scanPartitionDiscoveryIntervalMs;
    private OffsetsInitializer offsetsInitializer;
    private FlussDeserializationSchema<OUT> deserializationSchema;

    private String bootstrapServers;

    private String database;
    private String tableName;

    /**
     * Sets the bootstrap servers for the Fluss source connection.
     *
     * <p>This is a required parameter.
     *
     * @param bootstrapServers bootstrap server addresses
     * @return this builder
     */
    public FlussSourceBuilder<OUT> setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        return this;
    }

    /**
     * Sets the database name for the Fluss source.
     *
     * <p>This is a required parameter.
     *
     * @param database name of the database
     * @return this builder
     */
    public FlussSourceBuilder<OUT> setDatabase(String database) {
        this.database = database;
        return this;
    }

    /**
     * Sets the table name for the Fluss source.
     *
     * <p>This is a required parameter.
     *
     * @param table name of the table
     * @return this builder
     */
    public FlussSourceBuilder<OUT> setTable(String table) {
        this.tableName = table;
        return this;
    }

    /**
     * Sets the scan partition discovery interval in milliseconds.
     *
     * <p>If not specified, the default value from {@link
     * FlinkConnectorOptions#SCAN_PARTITION_DISCOVERY_INTERVAL} is used.
     *
     * @param scanPartitionDiscoveryIntervalMs interval in milliseconds
     * @return this builder
     */
    public FlussSourceBuilder<OUT> setScanPartitionDiscoveryIntervalMs(
            long scanPartitionDiscoveryIntervalMs) {
        this.scanPartitionDiscoveryIntervalMs = scanPartitionDiscoveryIntervalMs;
        return this;
    }

    /**
     * Sets the starting offsets strategy for the Fluss source.
     *
     * <p>If not specified, {@link OffsetsInitializer#full()} is used by default.
     *
     * @param offsetsInitializer the strategy for determining starting offsets
     * @return this builder
     */
    public FlussSourceBuilder<OUT> setStartingOffsets(OffsetsInitializer offsetsInitializer) {
        this.offsetsInitializer = offsetsInitializer;
        return this;
    }

    /**
     * Sets the deserialization schema for converting Fluss records to output records.
     *
     * <p>This is a required parameter.
     *
     * @param deserializationSchema the deserialization schema to use
     * @return this builder
     */
    public FlussSourceBuilder<OUT> setDeserializationSchema(
            FlussDeserializationSchema<OUT> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
        return this;
    }

    /**
     * Sets the projected fields for this source using field names.
     *
     * <p>Projection allows selecting a subset of fields from the table. Without projection, all
     * fields from the table are included.
     *
     * @param projectedFieldNames names of the fields to project
     * @return this builder
     * @throws NullPointerException if projectedFieldNames is null
     */
    public FlussSourceBuilder<OUT> setProjectedFields(String... projectedFieldNames) {
        checkNotNull(projectedFieldNames, "Field names must not be null");
        this.projectedFieldNames = projectedFieldNames;
        return this;
    }

    /**
     * Sets custom Fluss configuration properties for the source connector.
     *
     * <p>If not specified, an empty configuration will be created and populated with required
     * properties. Any configuration set through this method will be merged with table-specific
     * properties retrieved from the Fluss system.
     *
     * @param flussConf the configuration to use
     * @return this builder
     */
    public FlussSourceBuilder<OUT> setFlussConfig(Configuration flussConf) {
        this.flussConf = flussConf;
        return this;
    }

    /**
     * Builds and returns a new {@link FlussSource} instance with the configured properties.
     *
     * <p>This method validates all required parameters, connects to the Fluss system to retrieve
     * table metadata, and constructs a configured source.
     *
     * @return a new {@link FlussSource} instance
     * @throws NullPointerException if any required parameter is missing
     * @throws IllegalArgumentException if any parameter is invalid
     * @throws RuntimeException if connection to Fluss fails or the table cannot be found
     */
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
            offsetsInitializer = OffsetsInitializer.full();
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

        if (this.projectedFieldNames != null && this.projectedFieldNames.length > 0) {
            RowType rowType = tableInfo.getRowType();
            List<String> allFieldNames = rowType.getFieldNames();

            // Create a map of field name to index
            Map<String, Integer> fieldNameToIndex = new HashMap<>();
            for (int i = 0; i < allFieldNames.size(); i++) {
                fieldNameToIndex.put(allFieldNames.get(i), i);
            }

            int[] indices = new int[projectedFieldNames.length];
            for (int i = 0; i < projectedFieldNames.length; i++) {
                String fieldName = projectedFieldNames[i];
                Integer index = fieldNameToIndex.get(fieldName);

                if (index == null) {
                    throw new IllegalArgumentException(
                            "Field name '"
                                    + fieldName
                                    + "' not found in table schema. "
                                    + "Available fields: "
                                    + String.join(", ", allFieldNames));
                }

                indices[i] = index;
            }

            this.projectedFields = indices;
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
