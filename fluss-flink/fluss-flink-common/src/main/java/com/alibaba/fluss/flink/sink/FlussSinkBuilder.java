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

    private void validateConfiguration() {
        checkNotNull(bootstrapServers, "BootstrapServers is required but not provided.");
        checkNotNull(serializationSchema, "SerializationSchema is required but not provided.");

        checkNotNull(database, "Database is required but not provided.");
        checkArgument(!database.isEmpty(), "Database cannot be empty.");

        checkNotNull(tableName, "Table name is required but not provided.");
        checkArgument(!tableName.isEmpty(), "Table name cannot be empty.");
    }
}
