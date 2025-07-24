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

package com.alibaba.fluss.flink.sink;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.flink.sink.serializer.FlussSerializationSchema;
import com.alibaba.fluss.flink.sink.writer.FlinkSinkWriter;
import com.alibaba.fluss.metadata.DataLakeFormat;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;

import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.alibaba.fluss.flink.utils.FlinkConversions.toFlinkRowType;
import static com.alibaba.fluss.utils.Preconditions.checkArgument;
import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/**
 * Builder for creating and configuring Fluss sink connectors for Apache Flink.
 *
 * <p>The builder supports automatic schema inference from POJO classes using reflection and
 * provides options for customizing data conversion logic through custom converters.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * FlinkSink<Order> sink = new FlussSinkBuilder<Order>()
 *          .setBootstrapServers(bootstrapServers)
 *          .setTable(tableName)
 *          .setDatabase(databaseName)
 *          .setRowType(orderRowType)
 *          .setSerializationSchema(new OrderSerializationSchema())
 *          .setPartialUpdateColumns("amount", "address");
 *          .build())
 * }</pre>
 *
 * @param <InputT>> The input type of records to be written to Fluss
 * @since 0.7
 */
@PublicEvolving
public class FlussSinkBuilder<InputT> {
    private static final Logger LOG = LoggerFactory.getLogger(FlussSinkBuilder.class);

    private String bootstrapServers;
    private String database;
    private String tableName;
    private final Map<String, String> configOptions = new HashMap<>();
    private FlussSerializationSchema<InputT> serializationSchema;
    private boolean shuffleByBucketId = true;
    private String[] partialUpdateColumnNames;

    /** Set the bootstrap server for the sink. */
    public FlussSinkBuilder<InputT> setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        return this;
    }

    /** Set the database for the sink. */
    public FlussSinkBuilder<InputT> setDatabase(String database) {
        this.database = database;
        return this;
    }

    /** Set the table name for the sink. */
    public FlussSinkBuilder<InputT> setTable(String table) {
        this.tableName = table;
        return this;
    }

    /** Set shuffle by bucket id. */
    public FlussSinkBuilder<InputT> setShuffleByBucketId(boolean shuffleByBucketId) {
        this.shuffleByBucketId = shuffleByBucketId;
        return this;
    }

    /** Set a configuration option. */
    public FlussSinkBuilder<InputT> setOption(String key, String value) {
        configOptions.put(key, value);
        return this;
    }

    /** Set multiple configuration options. */
    public FlussSinkBuilder<InputT> setOptions(Map<String, String> options) {
        configOptions.putAll(options);
        return this;
    }

    /** Set a FlussSerializationSchema. */
    public FlussSinkBuilder<InputT> setSerializationSchema(
            FlussSerializationSchema<InputT> serializationSchema) {
        this.serializationSchema = serializationSchema;
        return this;
    }

    /**
     * Set partial update column names for upsert operations.
     *
     * <p>This method allows specifying which columns should be updated during upsert operations.
     * Only the specified columns will be updated, while other columns will remain unchanged. This
     * is particularly useful for scenarios where you only want to update specific fields like
     * status, timestamps, or computed values without affecting the entire record.
     *
     * <p><strong>Note:</strong> Partial updates are only supported for tables with primary keys.
     * For append-only tables, this setting will be ignored with a warning message.
     */
    public FlussSinkBuilder<InputT> setPartialUpdateColumns(String... columnNames) {
        checkNotNull(columnNames, "Column names cannot be null");
        checkArgument(columnNames.length > 0, "Column names cannot be empty");
        this.partialUpdateColumnNames = Arrays.copyOf(columnNames, columnNames.length);
        return this;
    }

    /** Build the FlussSink. */
    public FlussSink<InputT> build() {
        validateConfiguration();

        Configuration flussConfig = Configuration.fromMap(configOptions);

        TablePath tablePath = new TablePath(database, tableName);
        flussConfig.setString(ConfigOptions.BOOTSTRAP_SERVERS.key(), bootstrapServers);

        TableInfo tableInfo = getTableInfo(flussConfig, tablePath);

        int numBucket = tableInfo.getNumBuckets();
        List<String> bucketKeys = tableInfo.getBucketKeys();
        List<String> partitionKeys = tableInfo.getPartitionKeys();
        RowType tableRowType = toFlinkRowType(tableInfo.getRowType());
        DataLakeFormat lakeFormat = tableInfo.getTableConfig().getDataLakeFormat().orElse(null);

        boolean isUpsert = tableInfo.hasPrimaryKey();

        // Resolve column names to indexes if specified
        int[] targetColumnIndexes = null;
        if (partialUpdateColumnNames != null) {
            if (isUpsert) {
                targetColumnIndexes =
                        resolveColumnNamesToIndexes(
                                Arrays.asList(partialUpdateColumnNames), tableRowType);
                LOG.info(
                        "Partial update enabled for columns: {} -> indexes: {}",
                        partialUpdateColumnNames,
                        Arrays.toString(targetColumnIndexes));
            } else {
                LOG.warn(
                        "Partial update columns specified for append-only table '{}', ignoring configuration. "
                                + "Partial updates are only supported for tables with primary keys.",
                        tablePath);
            }
        }

        FlinkSink.SinkWriterBuilder<? extends FlinkSinkWriter<InputT>, InputT> writerBuilder;

        if (isUpsert) {
            LOG.info("Initializing Fluss upsert sink writer ...");
            writerBuilder =
                    new FlinkSink.UpsertSinkWriterBuilder<>(
                            tablePath,
                            flussConfig,
                            tableRowType,
                            null, // not support partialUpdateColumns yet
                            numBucket,
                            bucketKeys,
                            partitionKeys,
                            lakeFormat,
                            shuffleByBucketId,
                            serializationSchema);
        } else {
            LOG.info("Initializing Fluss append sink writer ...");
            writerBuilder =
                    new FlinkSink.AppendSinkWriterBuilder<>(
                            tablePath,
                            flussConfig,
                            tableRowType,
                            numBucket,
                            bucketKeys,
                            partitionKeys,
                            lakeFormat,
                            shuffleByBucketId,
                            serializationSchema);
        }

        return new FlussSink<>(writerBuilder);
    }

    /** Get table information from Fluss cluster. */
    private TableInfo getTableInfo(Configuration flussConfig, TablePath tablePath) {
        try (Connection connection = ConnectionFactory.createConnection(flussConfig);
                Admin admin = connection.getAdmin()) {
            try {
                return admin.getTableInfo(tablePath).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while getting table info", e);
            } catch (ExecutionException e) {
                throw new RuntimeException("Failed to get table info for table: " + tablePath, e);
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to initialize Fluss admin connection: " + e.getMessage(), e);
        }
    }

    /** Resolve column names to their corresponding indexes in the table schema. */
    private int[] resolveColumnNamesToIndexes(List<String> columnNames, RowType tableRowType) {
        List<String> fieldNames = tableRowType.getFieldNames();

        // Create name to index mapping for efficient lookup
        Map<String, Integer> nameToIndexMap = new HashMap<>();
        for (int i = 0; i < fieldNames.size(); i++) {
            nameToIndexMap.put(fieldNames.get(i), i);
        }

        // Resolve each column name to its index
        int[] indexes = new int[columnNames.size()];
        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = columnNames.get(i);
            Integer index = nameToIndexMap.get(columnName);

            if (index == null) {
                throw new IllegalArgumentException(
                        String.format(
                                "Column '%s' not found in table schema. Available columns: %s",
                                columnName, fieldNames));
            }

            indexes[i] = index;
        }

        return indexes;
    }

    private void validateConfiguration() {
        checkNotNull(bootstrapServers, "BootstrapServers is required but not provided.");
        checkNotNull(serializationSchema, "SerializationSchema is required but not provided.");

        checkNotNull(database, "Database is required but not provided.");
        checkArgument(!database.isEmpty(), "Database cannot be empty.");

        checkNotNull(tableName, "Table name is required but not provided.");
        checkArgument(!tableName.isEmpty(), "Table name cannot be empty.");
    }
}
