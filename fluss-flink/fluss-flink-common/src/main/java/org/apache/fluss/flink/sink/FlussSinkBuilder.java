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

package org.apache.fluss.flink.sink;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.sink.serializer.FlussSerializationSchema;
import org.apache.fluss.flink.sink.writer.FlinkSinkWriter;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.apache.fluss.flink.utils.FlinkConversions.toFlinkRowType;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

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
    // Optional list of columns for partial update. When set, upsert will only update these columns.
    // The primary key columns must be fully specified in this list.
    private List<String> partialUpdateColumns;

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

    /**
     * Enable partial update by specifying the column names to update for upsert tables. Primary key
     * columns must be included in this list.
     */
    public FlussSinkBuilder<InputT> setPartialUpdateColumns(List<String> columns) {
        this.partialUpdateColumns = columns;
        return this;
    }

    /**
     * Enable partial update by specifying the column names to update for upsert tables. Convenience
     * varargs overload.
     */
    public FlussSinkBuilder<InputT> setPartialUpdateColumns(String... columns) {
        this.partialUpdateColumns = Arrays.asList(columns);
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

    /** Build the FlussSink. */
    public FlussSink<InputT> build() {
        validateConfiguration();

        Configuration flussConfig = Configuration.fromMap(configOptions);

        FlinkSink.SinkWriterBuilder<? extends FlinkSinkWriter<InputT>, InputT> writerBuilder;

        TablePath tablePath = new TablePath(database, tableName);
        flussConfig.setString(ConfigOptions.BOOTSTRAP_SERVERS.key(), bootstrapServers);

        TableInfo tableInfo;
        try (Connection connection = ConnectionFactory.createConnection(flussConfig);
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
        int numBucket = tableInfo.getNumBuckets();
        List<String> bucketKeys = tableInfo.getBucketKeys();
        List<String> partitionKeys = tableInfo.getPartitionKeys();
        RowType tableRowType = toFlinkRowType(tableInfo.getRowType());
        DataLakeFormat lakeFormat = tableInfo.getTableConfig().getDataLakeFormat().orElse(null);

        boolean isUpsert = tableInfo.hasPrimaryKey();

        if (isUpsert) {
            LOG.info("Initializing Fluss upsert sink writer ...");
            int[] targetColumnIndexes =
                    computeTargetColumnIndexes(
                            tableRowType.getFieldNames(),
                            tableInfo.getPrimaryKeys(),
                            partialUpdateColumns);
            writerBuilder =
                    new FlinkSink.UpsertSinkWriterBuilder<>(
                            tablePath,
                            flussConfig,
                            tableRowType,
                            targetColumnIndexes,
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

    private void validateConfiguration() {
        checkNotNull(bootstrapServers, "BootstrapServers is required but not provided.");
        checkNotNull(serializationSchema, "SerializationSchema is required but not provided.");

        checkNotNull(database, "Database is required but not provided.");
        checkArgument(!database.isEmpty(), "Database cannot be empty.");

        checkNotNull(tableName, "Table name is required but not provided.");
        checkArgument(!tableName.isEmpty(), "Table name cannot be empty.");
    }

    // -------------- Test-visible helper methods --------------
    /**
     * Computes target column indexes for partial updates. If {@code specifiedColumns} is null or
     * empty, returns null indicating full update. Validates that all primary key columns are
     * included in the specified columns.
     *
     * @param allFieldNames the list of all field names in table row type order
     * @param primaryKeyNames the list of primary key column names
     * @param specifiedColumns the optional list of columns specified for partial update
     * @return the indexes into {@code allFieldNames} corresponding to {@code specifiedColumns}, or
     *     null for full update
     * @throws IllegalArgumentException if a specified column does not exist or primary key coverage
     *     is incomplete
     */
    static int[] computeTargetColumnIndexes(
            List<String> allFieldNames,
            List<String> primaryKeyNames,
            List<String> specifiedColumns) {
        if (specifiedColumns == null || specifiedColumns.isEmpty()) {
            return null; // full update
        }

        // Map specified column names to indexes
        int[] indexes = new int[specifiedColumns.size()];
        for (int i = 0; i < specifiedColumns.size(); i++) {
            String col = specifiedColumns.get(i);
            int idx = allFieldNames.indexOf(col);
            checkArgument(
                    idx >= 0, "Column '%s' not found in table schema: %s", col, allFieldNames);
            indexes[i] = idx;
        }

        // Validate that all primary key columns are covered
        for (String pk : primaryKeyNames) {
            checkArgument(
                    specifiedColumns.contains(pk),
                    "Partial updates must include all primary key columns. Missing primary key column: %s. Provided columns: %s",
                    pk,
                    specifiedColumns);
        }

        return indexes;
    }
}
