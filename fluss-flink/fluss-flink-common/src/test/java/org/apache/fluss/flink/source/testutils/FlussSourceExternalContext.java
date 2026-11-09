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

package org.apache.fluss.flink.source.testutils;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.flink.source.FlussSource;
import org.apache.fluss.flink.source.FlussSourceBuilder;
import org.apache.fluss.flink.source.deserializer.FlussDeserializationSchema;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.testframe.external.ExternalSystemSplitDataWriter;
import org.apache.flink.connector.testframe.external.source.DataStreamSourceExternalContext;
import org.apache.flink.connector.testframe.external.source.TestingSourceSettings;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * A {@link DataStreamSourceExternalContext} for testing {@link FlussSource} with Flink's connector
 * testing framework.
 *
 * <p>Each context creates its own partitioned log table with one bucket per partition. Every call
 * to {@link #createSourceSplitDataWriter(TestingSourceSettings)} creates a new partition, so one
 * split of the testing framework maps to exactly one partition, one bucket, and one Fluss split.
 */
public class FlussSourceExternalContext implements DataStreamSourceExternalContext<String> {

    private static final String DATABASE = "fluss";
    private static final String DATA_COLUMN = "data";
    private static final String PARTITION_COLUMN = "p";
    private static final int NUM_RECORDS_UPPER_BOUND = 100;
    private static final int NUM_RECORDS_LOWER_BOUND = 10;

    private final FlussClusterExtension flussCluster;
    private final TablePath tablePath;
    private final Connection connection;
    private final Admin admin;
    private final List<FlussSplitDataWriter> writers = new ArrayList<>();

    /**
     * Creates the context and a dedicated partitioned log table in the given Fluss cluster.
     *
     * @param flussCluster the running Fluss cluster to create the table in
     */
    public FlussSourceExternalContext(FlussClusterExtension flussCluster) throws Exception {
        this.flussCluster = flussCluster;
        this.tablePath =
                TablePath.of(
                        DATABASE, "source_test_" + UUID.randomUUID().toString().replace("-", "_"));
        this.connection = ConnectionFactory.createConnection(flussCluster.getClientConfig());
        this.admin = connection.getAdmin();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column(DATA_COLUMN, DataTypes.STRING())
                                        .column(PARTITION_COLUMN, DataTypes.STRING())
                                        .build())
                        .partitionedBy(PARTITION_COLUMN)
                        .distributedBy(1)
                        .build();
        admin.createTable(tablePath, tableDescriptor, false).get();
    }

    @Override
    public Source<String, ?, ?> createSource(TestingSourceSettings sourceSettings) {
        FlussSourceBuilder<String> builder =
                FlussSource.<String>builder()
                        .setBootstrapServers(flussCluster.getBootstrapServers())
                        .setDatabase(tablePath.getDatabaseName())
                        .setTable(tablePath.getTableName())
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setDeserializationSchema(new DataColumnDeserializationSchema());
        if (sourceSettings.getBoundedness() == Boundedness.BOUNDED) {
            builder.setStoppingOffsets(OffsetsInitializer.latest());
        }
        return builder.build();
    }

    @Override
    public ExternalSystemSplitDataWriter<String> createSourceSplitDataWriter(
            TestingSourceSettings sourceSettings) {
        String partitionName = "p" + writers.size();
        try {
            admin.createPartition(
                            tablePath,
                            new PartitionSpec(
                                    Collections.singletonMap(PARTITION_COLUMN, partitionName)),
                            false)
                    .get();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create partition " + partitionName, e);
        }
        flussCluster.waitUntilPartitionAllReady(tablePath, writers.size() + 1);

        FlussSplitDataWriter writer =
                new FlussSplitDataWriter(connection.getTable(tablePath), partitionName);
        writers.add(writer);
        return writer;
    }

    @Override
    public List<String> generateTestData(
            TestingSourceSettings sourceSettings, int splitIndex, long seed) {
        Random random = new Random(seed);
        int numRecords =
                random.nextInt(NUM_RECORDS_UPPER_BOUND - NUM_RECORDS_LOWER_BOUND)
                        + NUM_RECORDS_LOWER_BOUND;
        List<String> records = new ArrayList<>(numRecords);
        for (int i = 0; i < numRecords; i++) {
            // prefix with the split index so that records from different splits never equal
            records.add("split-" + splitIndex + "-" + i + "-" + random.nextLong());
        }
        return records;
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return Types.STRING;
    }

    @Override
    public List<URL> getConnectorJarPaths() {
        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {
        for (FlussSplitDataWriter writer : writers) {
            writer.close();
        }
        writers.clear();
        admin.dropTable(tablePath, true).get();
        connection.close();
    }

    @Override
    public String toString() {
        return "Fluss partitioned log table";
    }

    /** Writes records of one testing framework split into a dedicated Fluss partition. */
    private static class FlussSplitDataWriter implements ExternalSystemSplitDataWriter<String> {

        private final Table table;
        private final AppendWriter appendWriter;
        private final BinaryString partitionName;

        private FlussSplitDataWriter(Table table, String partitionName) {
            this.table = table;
            this.appendWriter = table.newAppend().createWriter();
            this.partitionName = BinaryString.fromString(partitionName);
        }

        @Override
        public void writeRecords(List<String> records) {
            for (String record : records) {
                appendWriter.append(GenericRow.of(BinaryString.fromString(record), partitionName));
            }
            appendWriter.flush();
        }

        @Override
        public void close() throws Exception {
            table.close();
        }
    }

    /** Deserializes the data column of a Fluss record into a {@link String}. */
    private static class DataColumnDeserializationSchema
            implements FlussDeserializationSchema<String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void open(InitializationContext context) {}

        @Override
        public String deserialize(LogRecord record) {
            return record.getRow().getString(0).toString();
        }

        @Override
        public TypeInformation<String> getProducedType(RowType scanRowType) {
            return Types.STRING;
        }
    }
}
