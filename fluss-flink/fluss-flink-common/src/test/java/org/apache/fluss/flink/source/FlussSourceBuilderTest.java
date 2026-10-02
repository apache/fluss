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

package org.apache.fluss.flink.source;

import org.apache.fluss.client.initializer.LatestOffsetsInitializer;
import org.apache.fluss.client.initializer.NoStoppingOffsetsInitializer;
import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.client.initializer.TimestampOffsetsInitializer;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.flink.source.deserializer.FlussDeserializationSchema;
import org.apache.fluss.flink.utils.FlinkTestBase;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.RowType;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/** Tests for the {@link FlussSourceBuilder} class. */
public class FlussSourceBuilderTest extends FlinkTestBase {

    private static String bootstrapServers;

    @BeforeEach
    public void setup() throws Exception {
        bootstrapServers = conn.getConfiguration().get(ConfigOptions.BOOTSTRAP_SERVERS).get(0);

        createTable(DEFAULT_TABLE_PATH, DEFAULT_PK_TABLE_DESCRIPTOR);
    }

    @Test
    public void testBuildWithValidConfiguration() {
        // Given
        FlussSource<TestRecord> source =
                FlussSource.<TestRecord>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(DEFAULT_DB)
                        .setTable(DEFAULT_TABLE_PATH.getTableName())
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setScanPartitionDiscoveryIntervalMs(1000L)
                        .setDeserializationSchema(new TestDeserializationSchema())
                        .build();

        // Then
        assertThat(source).isNotNull();
        assertThat(source.getBoundedness()).isEqualTo(Boundedness.CONTINUOUS_UNBOUNDED);
        assertThat(source.isStreaming()).isTrue();
        assertThat(source.getStoppingOffsetsInitializer())
                .isInstanceOf(NoStoppingOffsetsInitializer.class);
    }

    @Test
    public void testRejectUnsupportedStoppingOffsetsInitializer() {
        FlussSourceBuilder<TestRecord> builder = FlussSource.builder();

        assertThatThrownBy(() -> builder.setStoppingOffsets(OffsetsInitializer.earliest()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Only OffsetsInitializer.latest() and "
                                + "OffsetsInitializer.timestamp(...) are supported");
        assertThatThrownBy(() -> builder.setStoppingOffsets(OffsetsInitializer.full()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Only OffsetsInitializer.latest() and "
                                + "OffsetsInitializer.timestamp(...) are supported");

        assertThat(builder.setStoppingOffsets(OffsetsInitializer.timestamp(1L))).isSameAs(builder);
    }

    @Test
    public void testBuildStreamingSourceWithStoppingOffsets() {
        FlussSource<TestRecord> source =
                FlussSource.<TestRecord>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(DEFAULT_DB)
                        .setTable(DEFAULT_TABLE_PATH.getTableName())
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setStoppingOffsets(OffsetsInitializer.latest())
                        .setDeserializationSchema(new TestDeserializationSchema())
                        .build();

        assertThat(source.getBoundedness()).isEqualTo(Boundedness.BOUNDED);
        assertThat(source.isStreaming()).isTrue();
        assertThat(source.getStoppingOffsetsInitializer())
                .isInstanceOf(LatestOffsetsInitializer.class);
    }

    @Test
    public void testBuildBatchSource() {
        FlussSource<TestRecord> source =
                FlussSource.<TestRecord>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(DEFAULT_DB)
                        .setTable(DEFAULT_TABLE_PATH.getTableName())
                        .setBatch()
                        .setDeserializationSchema(new TestDeserializationSchema())
                        .build();

        assertThat(source.getBoundedness()).isEqualTo(Boundedness.BOUNDED);
        assertThat(source.isStreaming()).isFalse();
        assertThat(source.getStoppingOffsetsInitializer())
                .isInstanceOf(LatestOffsetsInitializer.class);
    }

    @Test
    public void testBatchAndStoppingOffsetsAreOrderIndependent() throws Exception {
        TablePath logTablePath = TablePath.of(DEFAULT_DB, "batch-with-stopping-offsets");
        createTable(logTablePath, DEFAULT_LOG_TABLE_DESCRIPTOR);

        FlussSource<TestRecord> batchThenStopping =
                FlussSource.<TestRecord>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(DEFAULT_DB)
                        .setTable(logTablePath.getTableName())
                        .setBatch()
                        .setStoppingOffsets(OffsetsInitializer.timestamp(1L))
                        .setDeserializationSchema(new TestDeserializationSchema())
                        .build();
        FlussSource<TestRecord> stoppingThenBatch =
                FlussSource.<TestRecord>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(DEFAULT_DB)
                        .setTable(logTablePath.getTableName())
                        .setStoppingOffsets(OffsetsInitializer.timestamp(1L))
                        .setBatch()
                        .setDeserializationSchema(new TestDeserializationSchema())
                        .build();

        assertThat(batchThenStopping.getBoundedness()).isEqualTo(Boundedness.BOUNDED);
        assertThat(batchThenStopping.isStreaming()).isFalse();
        assertThat(batchThenStopping.getStoppingOffsetsInitializer())
                .isInstanceOf(TimestampOffsetsInitializer.class);
        assertThat(stoppingThenBatch.getBoundedness()).isEqualTo(Boundedness.BOUNDED);
        assertThat(stoppingThenBatch.isStreaming()).isFalse();
        assertThat(stoppingThenBatch.getStoppingOffsetsInitializer())
                .isInstanceOf(TimestampOffsetsInitializer.class);
    }

    @Test
    public void testRejectStoppingOffsetsForPrimaryKeyBatchRead() {
        assertThatThrownBy(
                        () ->
                                FlussSource.<TestRecord>builder()
                                        .setBootstrapServers(bootstrapServers)
                                        .setDatabase(DEFAULT_DB)
                                        .setTable(DEFAULT_TABLE_PATH.getTableName())
                                        .setBatch()
                                        .setStoppingOffsets(OffsetsInitializer.latest())
                                        .setDeserializationSchema(new TestDeserializationSchema())
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Batch read on primary-key table")
                .hasMessageContaining("does not support explicit stopping offsets");

        assertThatThrownBy(
                        () ->
                                FlussSource.<TestRecord>builder()
                                        .setBootstrapServers(bootstrapServers)
                                        .setDatabase(DEFAULT_DB)
                                        .setTable(DEFAULT_TABLE_PATH.getTableName())
                                        .setBatch()
                                        .setStartingOffsets(OffsetsInitializer.earliest())
                                        .setStoppingOffsets(OffsetsInitializer.latest())
                                        .setDeserializationSchema(new TestDeserializationSchema())
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Batch read on primary-key table")
                .hasMessageContaining("does not support explicit stopping offsets");
    }

    @Test
    public void testBuildLegacyBoundedSource() {
        FlussSource<TestRecord> source =
                FlussSource.<TestRecord>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(DEFAULT_DB)
                        .setTable(DEFAULT_TABLE_PATH.getTableName())
                        .setBounded()
                        .setDeserializationSchema(new TestDeserializationSchema())
                        .build();

        assertThat(source.getBoundedness()).isEqualTo(Boundedness.BOUNDED);
        assertThat(source.isStreaming()).isFalse();
        assertThat(source.getStoppingOffsetsInitializer())
                .isInstanceOf(LatestOffsetsInitializer.class);
    }

    @Test
    public void testMissingBootstrapServers() {
        // Given
        Executable executable =
                () ->
                        FlussSource.<TestRecord>builder()
                                .setDatabase(DEFAULT_DB)
                                .setTable(DEFAULT_TABLE_PATH.getTableName())
                                .setStartingOffsets(OffsetsInitializer.earliest())
                                .setScanPartitionDiscoveryIntervalMs(1000L)
                                .setDeserializationSchema(new TestDeserializationSchema())
                                .build();

        // Then
        assertThatThrownBy(executable::execute)
                .isInstanceOf(NullPointerException.class)
                .hasMessage("BootstrapServers is required but not provided.");
    }

    @Test
    public void testEmptyBootstrapServers() {
        // Given
        Executable executable =
                () ->
                        FlussSource.<TestRecord>builder()
                                .setBootstrapServers("")
                                .setDatabase(DEFAULT_DB)
                                .setTable(DEFAULT_TABLE_PATH.getTableName())
                                .setStartingOffsets(OffsetsInitializer.earliest())
                                .setScanPartitionDiscoveryIntervalMs(1000L)
                                .setDeserializationSchema(new TestDeserializationSchema())
                                .build();

        assertThatThrownBy(executable::execute)
                .isInstanceOf(RuntimeException.class)
                .hasMessage(
                        "Failed to initialize FlussSource admin connection: No resolvable bootstrap urls given in bootstrap.servers");
    }

    @Test
    public void testMissingDatabase() {
        // Given
        Executable executable =
                () ->
                        FlussSource.<TestRecord>builder()
                                .setBootstrapServers(bootstrapServers)
                                .setTable(DEFAULT_TABLE_PATH.getTableName())
                                .setStartingOffsets(OffsetsInitializer.earliest())
                                .setScanPartitionDiscoveryIntervalMs(1000L)
                                .setDeserializationSchema(new TestDeserializationSchema())
                                .build();

        // Then
        assertThatThrownBy(executable::execute)
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Database is required but not provided.");
    }

    @Test
    public void testEmptyDatabase() {
        // Given
        Executable executable =
                () ->
                        FlussSource.<TestRecord>builder()
                                .setBootstrapServers(bootstrapServers)
                                .setDatabase("")
                                .setTable(DEFAULT_TABLE_PATH.getTableName())
                                .setStartingOffsets(OffsetsInitializer.earliest())
                                .setScanPartitionDiscoveryIntervalMs(1000L)
                                .setDeserializationSchema(new TestDeserializationSchema())
                                .build();

        // Then
        assertThatThrownBy(executable::execute)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Database must not be empty.");
    }

    @Test
    public void testMissingTable() {
        // Given
        Executable executable =
                () ->
                        FlussSource.<TestRecord>builder()
                                .setBootstrapServers(bootstrapServers)
                                .setDatabase(DEFAULT_DB)
                                .setStartingOffsets(OffsetsInitializer.earliest())
                                .setScanPartitionDiscoveryIntervalMs(1000L)
                                .setDeserializationSchema(new TestDeserializationSchema())
                                .build();

        // Then
        assertThatThrownBy(executable::execute)
                .isInstanceOf(NullPointerException.class)
                .hasMessage("TableName is required but not provided.");
    }

    @Test
    public void testEmptyTable() {
        // Given
        Executable executable =
                () ->
                        FlussSource.<TestRecord>builder()
                                .setBootstrapServers(bootstrapServers)
                                .setDatabase(DEFAULT_DB)
                                .setTable("")
                                .setStartingOffsets(OffsetsInitializer.earliest())
                                .setScanPartitionDiscoveryIntervalMs(1000L)
                                .setDeserializationSchema(new TestDeserializationSchema())
                                .build();

        // Then
        assertThatThrownBy(executable::execute)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("TableName must not be empty.");
    }

    @Test
    public void testMissingScanPartitionDiscoveryInterval() {
        // Given
        FlussSource<TestRecord> source =
                FlussSource.<TestRecord>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(DEFAULT_DB)
                        .setTable(DEFAULT_TABLE_PATH.getTableName())
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setDeserializationSchema(new TestDeserializationSchema())
                        .build();

        // Then
        assertThat(source.scanPartitionDiscoveryIntervalMs).isEqualTo(60000L);
    }

    @Test
    public void testMissingOffsetsInitializer() {
        // Given
        FlussSource<TestRecord> source =
                FlussSource.<TestRecord>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(DEFAULT_DB)
                        .setTable(DEFAULT_TABLE_PATH.getTableName())
                        .setScanPartitionDiscoveryIntervalMs(1000L)
                        .setDeserializationSchema(new TestDeserializationSchema())
                        .build();

        assertThat(source.getOffsetsInitializer().getClass())
                .isEqualTo(OffsetsInitializer.full().getClass());
    }

    @Test
    public void testMissingDeserializationSchema() {
        // Given
        Executable executable =
                () ->
                        FlussSource.<TestRecord>builder()
                                .setBootstrapServers(bootstrapServers)
                                .setDatabase(DEFAULT_DB)
                                .setTable(DEFAULT_TABLE_PATH.getTableName())
                                .setStartingOffsets(OffsetsInitializer.earliest())
                                .setScanPartitionDiscoveryIntervalMs(10000L)
                                .build();

        // Then
        assertThatThrownBy(executable::execute)
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Deserialization schema is required but not provided.");
    }

    @Test
    public void testSetProjectedFields() {
        // Given
        FlussSource<TestRecord> source =
                FlussSource.<TestRecord>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(DEFAULT_DB)
                        .setTable(DEFAULT_TABLE_PATH.getTableName())
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setScanPartitionDiscoveryIntervalMs(1000L)
                        .setDeserializationSchema(new TestDeserializationSchema())
                        .setProjectedFields("id", "name")
                        .build();

        // Then
        assertThat(source).isNotNull();
    }

    @Test
    public void testProjectedFields() {
        // When
        FlussSource<TestRecord> source =
                FlussSource.<TestRecord>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(DEFAULT_DB)
                        .setTable(DEFAULT_TABLE_PATH.getTableName())
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setScanPartitionDiscoveryIntervalMs(1000L)
                        .setDeserializationSchema(new TestDeserializationSchema())
                        .setProjectedFields("id", "name")
                        .build();

        // Then
        assertThat(source).isNotNull();
    }

    // Test record class for tests
    private static class TestRecord {
        private int id;
        private String name;

        public TestRecord(int id, String name) {
            this.id = id;
            this.name = name;
        }

        public int getId() {
            return id;
        }

        public String getName() {
            return name;
        }
    }

    // Test deserialization schema for tests
    private static class TestDeserializationSchema
            implements FlussDeserializationSchema<TestRecord> {

        @Override
        public void open(InitializationContext context) throws Exception {}

        @Override
        public TestRecord deserialize(LogRecord record) throws Exception {
            InternalRow row = record.getRow();
            return new TestRecord(row.getInt(0), row.getString(1).toString());
        }

        @Override
        public TypeInformation<TestRecord> getProducedType(RowType rowSchema) {
            return TypeInformation.of(TestRecord.class);
        }
    }
}
