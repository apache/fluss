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

package org.apache.fluss.lake.delta.tiering;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidAlterTableException;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.exception.TableAlreadyExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.lake.committer.CommittedLakeSnapshot;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.committer.LakeCommitter.CommitContext;
import org.apache.fluss.lake.delta.DeltaConfig;
import org.apache.fluss.lake.delta.DeltaLakeCatalog;
import org.apache.fluss.lake.delta.testutils.DeltaTestUtils;
import org.apache.fluss.lake.delta.utils.FlussDataTypeToDeltaDataType;
import org.apache.fluss.lake.lakestorage.TestingLakeCatalogContext;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.GenericRecord;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.PartitionUtils;

import io.delta.kernel.Operation;
import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.Transaction;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.utils.CloseableIterable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.apache.fluss.lake.committer.LakeCommitter.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY;
import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The UT for tiering to Delta Lake via {@link DeltaLakeTieringFactory}. */
class DeltaTieringTest {

    private static final int BUCKET_NUM = 3;
    private static final List<String> PARTITIONS = Arrays.asList("p1", "p2");
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private @TempDir File tempWarehouseDir;
    private Configuration configuration;
    private DeltaLakeTieringFactory tieringFactory;
    private Engine engine;

    @BeforeEach
    void beforeEach() {
        configuration = new Configuration();
        configuration.setString(DeltaConfig.WAREHOUSE, tempWarehouseDir.toString());
        tieringFactory = new DeltaLakeTieringFactory(configuration);
        engine = DeltaConfig.createEngine(configuration);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testTieringWriteTable(boolean isPartitioned) throws Exception {
        TablePath tablePath = TablePath.of("fluss", isPartitioned ? "partitioned" : "log");
        TableInfo tableInfo = createTable(tablePath, isPartitioned);
        String deltaPath = DeltaConfig.tablePath(configuration, tablePath);

        try (LakeCommitter<DeltaWriteResult, DeltaCommittable> committer =
                createLakeCommitter(tablePath, tableInfo)) {
            // the table is just created, nothing is tiered yet
            assertThat(committer.getMissingLakeSnapshot(null)).isNull();
        }

        SimpleVersionedSerializer<DeltaWriteResult> writeResultSerializer =
                tieringFactory.getWriteResultSerializer();
        List<DeltaWriteResult> writeResults = new ArrayList<>();
        List<String> expectedRows = new ArrayList<>();
        List<String> partitions = isPartitioned ? PARTITIONS : Collections.singletonList(null);
        for (int bucket = 0; bucket < BUCKET_NUM; bucket++) {
            for (String partition : partitions) {
                try (LakeWriter<DeltaWriteResult> writer =
                        createLakeWriter(tablePath, bucket, partition, tableInfo)) {
                    for (int i = 0; i < 10; i++) {
                        GenericRow row = genRow(bucket, partition, i);
                        writer.write(new GenericRecord(i, i * 1000L, ChangeType.APPEND_ONLY, row));
                        expectedRows.add(expectedRow(bucket, partition, i));
                    }
                    byte[] serialized = writeResultSerializer.serialize(writer.complete());
                    writeResults.add(
                            writeResultSerializer.deserialize(
                                    writeResultSerializer.getVersion(), serialized));
                }
            }
        }

        long committedVersion = commit(tablePath, tableInfo, writeResults, "offsets-1");
        // version 0 is the table creation
        assertThat(committedVersion).isEqualTo(1L);

        assertThat(DeltaTestUtils.readRows(engine, deltaPath))
                .containsExactlyInAnyOrderElementsOf(expectedRows);
        if (isPartitioned) {
            for (String partition : PARTITIONS) {
                assertThat(Paths.get(deltaPath, "c=" + partition)).isDirectory();
            }
        }

        try (LakeCommitter<DeltaWriteResult, DeltaCommittable> committer =
                createLakeCommitter(tablePath, tableInfo)) {
            CommittedLakeSnapshot missing = committer.getMissingLakeSnapshot(null);
            assertThat(missing).isNotNull();
            assertThat(missing.getLakeSnapshotId()).isEqualTo(1L);
            assertThat(missing.getSnapshotProperties())
                    .containsEntry(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY, "offsets-1");
            assertThat(committer.getMissingLakeSnapshot(0L)).isEqualTo(missing);
            assertThat(committer.getMissingLakeSnapshot(1L)).isNull();
        }
    }

    @Test
    void testEmptyCommitRecordsOffsets() throws Exception {
        TablePath tablePath = TablePath.of("fluss", "empty_commit");
        TableInfo tableInfo = createTable(tablePath, false);

        assertThat(commit(tablePath, tableInfo, Collections.emptyList(), "offsets-1"))
                .isEqualTo(1L);
        assertThat(commit(tablePath, tableInfo, Collections.emptyList(), "offsets-2"))
                .isEqualTo(2L);

        try (LakeCommitter<DeltaWriteResult, DeltaCommittable> committer =
                createLakeCommitter(tablePath, tableInfo)) {
            CommittedLakeSnapshot missing = committer.getMissingLakeSnapshot(1L);
            assertThat(missing).isNotNull();
            assertThat(missing.getLakeSnapshotId()).isEqualTo(2L);
            assertThat(missing.getSnapshotProperties())
                    .containsEntry(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY, "offsets-2");
        }
    }

    @Test
    void testMissingLakeSnapshotSkipsCommitsOfOtherWriters() throws Exception {
        TablePath tablePath = TablePath.of("fluss", "other_writer");
        TableInfo tableInfo = createTable(tablePath, false);
        String deltaPath = DeltaConfig.tablePath(configuration, tablePath);

        assertThat(commit(tablePath, tableInfo, Collections.emptyList(), "offsets-1"))
                .isEqualTo(1L);
        commitAsOtherWriter(deltaPath);
        commitAsOtherWriter(deltaPath);

        try (LakeCommitter<DeltaWriteResult, DeltaCommittable> committer =
                createLakeCommitter(tablePath, tableInfo)) {
            CommittedLakeSnapshot missing = committer.getMissingLakeSnapshot(null);
            assertThat(missing).isNotNull();
            assertThat(missing.getLakeSnapshotId()).isEqualTo(1L);
            assertThat(committer.getMissingLakeSnapshot(1L)).isNull();
        }
    }

    @Test
    void testMissingLakeSnapshotOnTruncatedLog() throws Exception {
        TablePath tablePath = TablePath.of("fluss", "truncated_log");
        TableInfo tableInfo = createTable(tablePath, false);
        String deltaPath = DeltaConfig.tablePath(configuration, tablePath);

        assertThat(commit(tablePath, tableInfo, Collections.emptyList(), "offsets-1"))
                .isEqualTo(1L);
        commitAsOtherWriter(deltaPath);
        commitAsOtherWriter(deltaPath);
        // log cleanup keeps the checkpoint of version 3 and drops the commits before it
        TableManager.loadSnapshot(deltaPath).build(engine).writeCheckpoint(engine);
        DeltaTestUtils.truncateLogBefore(deltaPath, 3L);
        commitAsOtherWriter(deltaPath);

        try (LakeCommitter<DeltaWriteResult, DeltaCommittable> committer =
                createLakeCommitter(tablePath, tableInfo)) {
            // version 1 set the offsets but is gone, so the oldest version left stands in for it
            CommittedLakeSnapshot missing = committer.getMissingLakeSnapshot(null);
            assertThat(missing).isNotNull();
            assertThat(missing.getLakeSnapshotId()).isEqualTo(3L);
            assertThat(missing.getSnapshotProperties())
                    .containsEntry(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY, "offsets-1");
            // the version known to Fluss is gone too, so the same offsets are reported again
            assertThat(committer.getMissingLakeSnapshot(1L)).isEqualTo(missing);
            assertThat(committer.getMissingLakeSnapshot(3L)).isNull();
        }

        assertThat(commit(tablePath, tableInfo, Collections.emptyList(), "offsets-2"))
                .isEqualTo(5L);
        commitAsOtherWriter(deltaPath);
        try (LakeCommitter<DeltaWriteResult, DeltaCommittable> committer =
                createLakeCommitter(tablePath, tableInfo)) {
            CommittedLakeSnapshot missing = committer.getMissingLakeSnapshot(3L);
            assertThat(missing).isNotNull();
            assertThat(missing.getLakeSnapshotId()).isEqualTo(5L);
            assertThat(missing.getSnapshotProperties())
                    .containsEntry(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY, "offsets-2");
            assertThat(committer.getMissingLakeSnapshot(5L)).isNull();
        }
    }

    @Test
    void testCommitFailsWhenOtherEngineChangesSchema() throws Exception {
        TablePath tablePath = TablePath.of("fluss", "schema_changed");
        TableInfo tableInfo = createTable(tablePath, false);
        String deltaPath = DeltaConfig.tablePath(configuration, tablePath);
        assertThat(commit(tablePath, tableInfo, Collections.emptyList(), "offsets-1"))
                .isEqualTo(1L);

        // like ALTER TABLE ... ADD COLUMNS (extra STRING) of Spark, and b becomes an integer
        commitMetadataChange(
                deltaPath,
                metadata -> {
                    ObjectNode schema =
                            (ObjectNode) readTree(metadata.get("schemaString").asText());
                    ArrayNode fields = (ArrayNode) schema.get("fields");
                    ((ObjectNode) fields.get(1)).put("type", "integer");
                    fields.addObject()
                            .put("name", "extra")
                            .put("type", "string")
                            .put("nullable", true)
                            .putObject("metadata");
                    metadata.put("schemaString", schema.toString());
                });

        assertThatThrownBy(() -> commit(tablePath, tableInfo, Collections.emptyList(), "offsets-2"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no longer matches the Fluss table")
                .hasMessageContaining("column 'b' is integer, expected string")
                .hasMessageContaining("column 'extra' is unexpected");
    }

    @Test
    void testCommitFailsWhenOtherEngineChangesPartitionColumns() throws Exception {
        TablePath tablePath = TablePath.of("fluss", "partitioning_changed");
        TableInfo tableInfo = createTable(tablePath, false);
        String deltaPath = DeltaConfig.tablePath(configuration, tablePath);
        assertThat(commit(tablePath, tableInfo, Collections.emptyList(), "offsets-1"))
                .isEqualTo(1L);

        commitMetadataChange(deltaPath, metadata -> metadata.putArray("partitionColumns").add("c"));

        assertThatThrownBy(() -> commit(tablePath, tableInfo, Collections.emptyList(), "offsets-2"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "is partitioned by [c], but the Fluss table is partitioned by []");
    }

    @ParameterizedTest
    @MethodSource("partitionKeyTypes")
    void testTieringTypedPartitionKey(DataType type, Object value, String expected)
            throws Exception {
        TablePath tablePath =
                TablePath.of("fluss", "partition_" + type.getTypeRoot().name().toLowerCase());
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .column("p", type)
                                        .build())
                        .partitionedBy("p")
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .build();
        TableInfo tableInfo = createTable(tablePath, descriptor);
        // the writer gets the partition as a Fluss partition name
        String partition = PartitionUtils.convertValueOfType(value, type.getTypeRoot());

        List<DeltaWriteResult> writeResults = new ArrayList<>();
        try (LakeWriter<DeltaWriteResult> writer =
                createLakeWriter(tablePath, 0, partition, tableInfo)) {
            for (int i = 0; i < 2; i++) {
                writer.write(
                        new GenericRecord(i, i, ChangeType.APPEND_ONLY, GenericRow.of(i, value)));
            }
            writeResults.add(writer.complete());
        }
        assertThat(commit(tablePath, tableInfo, writeResults, "offsets-1")).isEqualTo(1L);

        // Kernel drops the partition column from the data files and reads it back from the
        // partition value of the add action
        assertThat(DeltaTestUtils.readRows(engine, DeltaConfig.tablePath(configuration, tablePath)))
                .containsExactlyInAnyOrder("+I[0, " + expected + "]", "+I[1, " + expected + "]");
    }

    private static Stream<Arguments> partitionKeyTypes() {
        return Stream.of(
                Arguments.of(DataTypes.CHAR(3), BinaryString.fromString("abc"), "abc"),
                Arguments.of(DataTypes.STRING(), BinaryString.fromString("a b/c"), "a b/c"),
                Arguments.of(DataTypes.BOOLEAN(), true, "true"),
                Arguments.of(DataTypes.TINYINT(), (byte) 7, "7"),
                Arguments.of(DataTypes.SMALLINT(), (short) -3, "-3"),
                Arguments.of(DataTypes.INT(), 42, "42"),
                Arguments.of(DataTypes.BIGINT(), 1234567890123L, "1234567890123"),
                Arguments.of(DataTypes.FLOAT(), 1.5f, "1.5"),
                Arguments.of(DataTypes.DOUBLE(), -2.25d, "-2.25"),
                // days since epoch, 2024-01-15
                Arguments.of(DataTypes.DATE(), 19737, "19737"),
                // microseconds since epoch
                Arguments.of(
                        DataTypes.TIMESTAMP(6),
                        TimestampNtz.fromMicros(1700000000123456L),
                        "1700000000123456"),
                Arguments.of(
                        DataTypes.TIMESTAMP_LTZ(6),
                        TimestampLtz.fromEpochMicros(1700000000123456L),
                        "1700000000123456"));
    }

    @Test
    void testWriteBatchSizeSplitsFiles() throws Exception {
        TablePath tablePath = TablePath.of("fluss", "small_batches");
        TableInfo tableInfo = createTable(tablePath, false);
        configuration.setString(DeltaConfig.WRITE_BATCH_SIZE, "4");
        tieringFactory = new DeltaLakeTieringFactory(configuration);

        DeltaWriteResult writeResult;
        try (LakeWriter<DeltaWriteResult> writer =
                createLakeWriter(tablePath, 0, null, tableInfo)) {
            for (int i = 0; i < 10; i++) {
                writer.write(new GenericRecord(i, i, ChangeType.APPEND_ONLY, genRow(0, null, i)));
            }
            writeResult = writer.complete();
        }
        // 4 + 4 + 2 rows
        assertThat(writeResult.dataFiles()).hasSize(3);

        commit(tablePath, tableInfo, Collections.singletonList(writeResult), "offsets");
        assertThat(DeltaTestUtils.readRows(engine, DeltaConfig.tablePath(configuration, tablePath)))
                .hasSize(10);
    }

    @Test
    void testAbortDeletesWrittenFiles() throws Exception {
        TablePath tablePath = TablePath.of("fluss", "abort");
        TableInfo tableInfo = createTable(tablePath, false);

        DeltaWriteResult writeResult;
        try (LakeWriter<DeltaWriteResult> writer =
                createLakeWriter(tablePath, 0, null, tableInfo)) {
            writer.write(new GenericRecord(0, 0, ChangeType.APPEND_ONLY, genRow(0, null, 0)));
            writeResult = writer.complete();
        }
        assertThat(writeResult.dataFiles()).hasSize(1);
        Path dataFile = Paths.get(java.net.URI.create(writeResult.dataFiles().get(0).path()));
        assertThat(dataFile).exists();

        try (LakeCommitter<DeltaWriteResult, DeltaCommittable> committer =
                createLakeCommitter(tablePath, tableInfo)) {
            committer.abort(committer.toCommittable(Collections.singletonList(writeResult)));
        }
        assertThat(dataFile).doesNotExist();
        assertThat(DeltaTestUtils.readRows(engine, DeltaConfig.tablePath(configuration, tablePath)))
                .isEmpty();
    }

    @Test
    void testCreateTable() throws Exception {
        DeltaLakeCatalog catalog = new DeltaLakeCatalog(configuration);
        TablePath tablePath = TablePath.of("fluss", "create");
        TableDescriptor base = logTableDescriptor(false);
        // a custom property without the delta. prefix is mirrored like the Fluss options
        TableDescriptor descriptor =
                base.withProperties(
                        base.getProperties(), Collections.singletonMap("owner", "fluss-team"));

        catalog.createTable(tablePath, descriptor, new TestingLakeCatalogContext());
        Snapshot snapshot =
                TableManager.loadSnapshot(DeltaConfig.tablePath(configuration, tablePath))
                        .build(engine);
        assertThat(snapshot.getVersion()).isEqualTo(0L);
        assertThat(snapshot.getSchema().fieldNames()).containsExactly("a", "b", "c", "tags", "ts");
        assertThat(snapshot.getProtocol().getWriterFeatures()).contains("domainMetadata");
        // Fluss properties are mirrored with the fluss. prefix, like Paimon and Iceberg do
        assertThat(snapshot.getTableProperties())
                .containsEntry("fluss." + ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                .containsEntry("fluss.owner", "fluss-team")
                .doesNotContainKeys(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "owner");

        // creating the same table again is a no-op
        catalog.createTable(tablePath, descriptor, new TestingLakeCatalogContext());
        assertThat(
                        TableManager.loadSnapshot(DeltaConfig.tablePath(configuration, tablePath))
                                .build(engine)
                                .getVersion())
                .isEqualTo(0L);

        // but a different schema at the same path fails
        TableDescriptor otherSchema =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("x", DataTypes.INT()).build())
                        .distributedBy(1)
                        .build();
        assertThatThrownBy(
                        () ->
                                catalog.createTable(
                                        tablePath, otherSchema, new TestingLakeCatalogContext()))
                .isInstanceOf(TableAlreadyExistException.class);
    }

    @Test
    void testCreateUnsupportedTables() {
        DeltaLakeCatalog catalog = new DeltaLakeCatalog(configuration);

        TableDescriptor pkTable =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("a", DataTypes.INT())
                                        .primaryKey("a")
                                        .build())
                        .distributedBy(1)
                        .build();
        assertThatThrownBy(
                        () ->
                                catalog.createTable(
                                        TablePath.of("fluss", "pk"),
                                        pkTable,
                                        new TestingLakeCatalogContext()))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("primary key");

        TableDescriptor bytesPartition =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("a", DataTypes.INT())
                                        .column("p", DataTypes.BYTES())
                                        .build())
                        .partitionedBy("p")
                        .distributedBy(1)
                        .build();
        assertThatThrownBy(
                        () ->
                                catalog.createTable(
                                        TablePath.of("fluss", "bytes_partition"),
                                        bytesPartition,
                                        new TestingLakeCatalogContext()))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("doesn't support partition key 'p' of type BYTES");

        TableDescriptor timeColumn =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("t", DataTypes.TIME()).build())
                        .distributedBy(1)
                        .build();
        assertThatThrownBy(
                        () ->
                                catalog.createTable(
                                        TablePath.of("fluss", "time"),
                                        timeColumn,
                                        new TestingLakeCatalogContext()))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("TIME");

        TableDescriptor autoCompaction =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("a", DataTypes.INT()).build())
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_DATALAKE_AUTO_COMPACTION, true)
                        .build();
        assertThatThrownBy(
                        () ->
                                catalog.createTable(
                                        TablePath.of("fluss", "auto_compaction"),
                                        autoCompaction,
                                        new TestingLakeCatalogContext()))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining(ConfigOptions.TABLE_DATALAKE_AUTO_COMPACTION.key());
    }

    @Test
    void testCreateTableOnExistingTableWithoutDomainMetadata() {
        DeltaLakeCatalog catalog = new DeltaLakeCatalog(configuration);
        TablePath tablePath = TablePath.of("fluss", "no_domain_metadata");
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("a", DataTypes.INT()).build())
                        .distributedBy(1)
                        .build();
        TableManager.buildCreateTableTransaction(
                        DeltaConfig.tablePath(configuration, tablePath),
                        FlussDataTypeToDeltaDataType.toDeltaSchema(
                                descriptor.getSchema().getRowType()),
                        "external")
                .build(engine)
                .commit(engine, CloseableIterable.emptyIterable());

        assertThatThrownBy(
                        () ->
                                catalog.createTable(
                                        tablePath, descriptor, new TestingLakeCatalogContext()))
                .isInstanceOf(TableAlreadyExistException.class)
                .hasMessageContaining("domainMetadata");
    }

    @Test
    void testCreateFlussTableReusesEmptyTable() throws Exception {
        DeltaLakeCatalog catalog = new DeltaLakeCatalog(configuration);
        TablePath tablePath = TablePath.of("fluss", "create_retry");
        TableDescriptor descriptor = logTableDescriptor(false);

        catalog.createTable(tablePath, descriptor, creatingFlussTable());
        // a retry finds the empty table left by the first attempt
        catalog.createTable(tablePath, descriptor, creatingFlussTable());
        assertThat(
                        TableManager.loadSnapshot(DeltaConfig.tablePath(configuration, tablePath))
                                .build(engine)
                                .getVersion())
                .isEqualTo(0L);
    }

    @Test
    void testCreateFlussTableRejectsTieredTable() throws Exception {
        TablePath tablePath = TablePath.of("fluss", "create_retry");
        TableInfo tableInfo = createTable(tablePath, false);
        commit(
                tablePath,
                tableInfo,
                Collections.singletonList(writeOneRow(tablePath, tableInfo)),
                "offsets-1");

        assertCreateFlussTableRejected(tablePath);
        new DeltaLakeCatalog(configuration)
                .createTable(tablePath, logTableDescriptor(false), new TestingLakeCatalogContext());
    }

    @Test
    void testCreateFlussTableRejectsTableWithOnlyFlussOffsets() throws Exception {
        TablePath tablePath = TablePath.of("fluss", "only_offsets");
        TableInfo tableInfo = createTable(tablePath, false);
        // an empty round has no data files but still sets the fluss.tiering domain metadata
        commit(tablePath, tableInfo, Collections.emptyList(), "offsets-1");

        assertCreateFlussTableRejected(tablePath);
    }

    @Test
    void testCreateFlussTableRejectsTableWithOnlyDataFiles() throws Exception {
        TablePath tablePath = TablePath.of("fluss", "only_data_files");
        String deltaPath = DeltaConfig.tablePath(configuration, tablePath);
        TableInfo tableInfo = createTable(tablePath, false);
        commit(
                tablePath,
                tableInfo,
                Collections.singletonList(writeOneRow(tablePath, tableInfo)),
                "offsets-1");
        // like a table that another engine filled, without the fluss.tiering domain metadata
        Transaction transaction =
                TableManager.loadSnapshot(deltaPath)
                        .build(engine)
                        .buildUpdateTableTransaction("other-engine", Operation.WRITE)
                        .build(engine);
        transaction.removeDomainMetadata(DeltaLakeCommitter.FLUSS_DOMAIN);
        transaction.commit(engine, CloseableIterable.emptyIterable());
        assertThat(
                        TableManager.loadSnapshot(deltaPath)
                                .build(engine)
                                .getDomainMetadata(DeltaLakeCommitter.FLUSS_DOMAIN))
                .isEmpty();

        assertCreateFlussTableRejected(tablePath);
    }

    @ParameterizedTest
    @CsvSource({
        "delta.columnMapping.mode, name",
        "delta.columnMapping.mode, id",
        "delta.enableIcebergCompatV2, true"
    })
    void testCreateTableRejectsColumnMapping(String key, String value) {
        DeltaLakeCatalog catalog = new DeltaLakeCatalog(configuration);
        TablePath tablePath = TablePath.of("fluss", "column_mapping");
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("a", DataTypes.INT()).build())
                        .distributedBy(1)
                        .customProperty(key, value)
                        .build();

        assertThatThrownBy(
                        () ->
                                catalog.createTable(
                                        tablePath, descriptor, new TestingLakeCatalogContext()))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("column mapping");
        assertThat(new File(DeltaConfig.tablePath(configuration, tablePath))).doesNotExist();
    }

    @Test
    void testAlterTableProperties() throws Exception {
        DeltaLakeCatalog catalog = new DeltaLakeCatalog(configuration);
        TablePath tablePath = TablePath.of("fluss", "alter_properties");
        createTable(tablePath, false);

        String freshnessKey = "fluss." + ConfigOptions.TABLE_DATALAKE_FRESHNESS.key();
        String enabledKey = "fluss." + ConfigOptions.TABLE_DATALAKE_ENABLED.key();

        // Fluss-side options are mirrored as fluss.* in one metadata-only commit, a key set and
        // reset in the same request is not written
        catalog.alterTable(
                tablePath,
                Arrays.asList(
                        TableChange.set(ConfigOptions.TABLE_DATALAKE_FRESHNESS.key(), "1min"),
                        TableChange.reset(ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()),
                        TableChange.set(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "false"),
                        TableChange.set("owner", "fluss-team")),
                new TestingLakeCatalogContext());
        Snapshot snapshot =
                TableManager.loadSnapshot(DeltaConfig.tablePath(configuration, tablePath))
                        .build(engine);
        assertThat(snapshot.getVersion()).isEqualTo(1L);
        assertThat(snapshot.getTableProperties())
                .containsEntry(enabledKey, "false")
                .containsEntry("fluss.owner", "fluss-team")
                .doesNotContainKey(freshnessKey);
        // the metadata-only commit keeps the table feature the committer relies on
        assertThat(snapshot.getProtocol().getWriterFeatures()).contains("domainMetadata");

        // reset removes the mirrored property, a set after reset keeps the last value
        catalog.alterTable(
                tablePath,
                Arrays.asList(
                        TableChange.reset("owner"),
                        TableChange.reset(ConfigOptions.TABLE_DATALAKE_FRESHNESS.key()),
                        TableChange.set(ConfigOptions.TABLE_DATALAKE_FRESHNESS.key(), "3min")),
                new TestingLakeCatalogContext());
        snapshot =
                TableManager.loadSnapshot(DeltaConfig.tablePath(configuration, tablePath))
                        .build(engine);
        assertThat(snapshot.getVersion()).isEqualTo(2L);
        assertThat(snapshot.getTableProperties())
                .containsEntry(freshnessKey, "3min")
                .containsEntry(enabledKey, "false")
                .doesNotContainKey("fluss.owner");

        assertThatThrownBy(
                        () ->
                                catalog.alterTable(
                                        TablePath.of("fluss", "missing"),
                                        Collections.singletonList(
                                                TableChange.set(
                                                        ConfigOptions.TABLE_DATALAKE_FRESHNESS
                                                                .key(),
                                                        "1min")),
                                        new TestingLakeCatalogContext()))
                .isInstanceOf(TableNotExistException.class);
    }

    @Test
    void testAlterTableDeltaProperties() throws Exception {
        DeltaLakeCatalog catalog = new DeltaLakeCatalog(configuration);
        TablePath tablePath = TablePath.of("fluss", "alter_delta_properties");
        String retentionKey = "delta.logRetentionDuration";
        String retention = "interval 7 days";
        catalog.createTable(
                tablePath,
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("a", DataTypes.INT()).build())
                        .distributedBy(1)
                        .customProperty(retentionKey, retention)
                        .build(),
                new TestingLakeCatalogContext());

        // replayed when datalake is enabled together with a property createTable already applied,
        // nothing to mirror so no commit is made
        catalog.alterTable(
                tablePath,
                Arrays.asList(
                        TableChange.set(retentionKey, retention),
                        TableChange.reset("delta.checkpointInterval")),
                new TestingLakeCatalogContext());
        assertThat(
                        TableManager.loadSnapshot(DeltaConfig.tablePath(configuration, tablePath))
                                .build(engine)
                                .getVersion())
                .isEqualTo(0L);

        for (TableChange change :
                Arrays.asList(
                        TableChange.set(retentionKey, "interval 8 days"),
                        TableChange.reset(retentionKey),
                        TableChange.set("delta.checkpointInterval", "5"))) {
            assertThatThrownBy(
                            () ->
                                    catalog.alterTable(
                                            tablePath,
                                            Collections.singletonList(change),
                                            new TestingLakeCatalogContext()))
                    .isInstanceOf(InvalidAlterTableException.class)
                    .hasMessageContaining("delta.");
        }
    }

    @Test
    void testAlterTableRejectsUnsupportedChanges() throws Exception {
        DeltaLakeCatalog catalog = new DeltaLakeCatalog(configuration);
        TablePath tablePath = TablePath.of("fluss", "alter_unsupported");
        createTable(tablePath, false);

        for (TableChange change :
                Arrays.asList(
                        TableChange.addColumn(
                                "d", DataTypes.INT(), null, TableChange.ColumnPosition.last()),
                        TableChange.dropColumn("a"),
                        TableChange.set(ConfigOptions.TABLE_DATALAKE_TABLE_NAME.key(), "other"),
                        TableChange.reset(ConfigOptions.TABLE_DATALAKE_DATABASE_NAME.key()),
                        TableChange.set(
                                ConfigOptions.TABLE_DATALAKE_AUTO_COMPACTION.key(), "TRUE"))) {
            assertThatThrownBy(
                            () ->
                                    catalog.alterTable(
                                            tablePath,
                                            Collections.singletonList(change),
                                            new TestingLakeCatalogContext()))
                    .isInstanceOf(InvalidAlterTableException.class);
        }
        assertThatThrownBy(
                        () ->
                                catalog.alterTable(
                                        tablePath,
                                        Collections.singletonList(
                                                TableChange.modifyBucketCount(BUCKET_NUM + 1)),
                                        new TestingLakeCatalogContext()))
                .isInstanceOf(UnsupportedOperationException.class);

        // disabling auto-compaction is accepted and mirrored like any other Fluss option
        catalog.alterTable(
                tablePath,
                Collections.singletonList(
                        TableChange.set(
                                ConfigOptions.TABLE_DATALAKE_AUTO_COMPACTION.key(), "false")),
                new TestingLakeCatalogContext());
        assertThat(
                        TableManager.loadSnapshot(DeltaConfig.tablePath(configuration, tablePath))
                                .build(engine)
                                .getTableProperties())
                .containsEntry(
                        "fluss." + ConfigOptions.TABLE_DATALAKE_AUTO_COMPACTION.key(), "false");
    }

    private long commit(
            TablePath tablePath,
            TableInfo tableInfo,
            List<DeltaWriteResult> writeResults,
            String offsets)
            throws Exception {
        SimpleVersionedSerializer<DeltaCommittable> committableSerializer =
                tieringFactory.getCommittableSerializer();
        try (LakeCommitter<DeltaWriteResult, DeltaCommittable> committer =
                createLakeCommitter(tablePath, tableInfo)) {
            DeltaCommittable committable = committer.toCommittable(writeResults);
            committable =
                    committableSerializer.deserialize(
                            committableSerializer.getVersion(),
                            committableSerializer.serialize(committable));
            return committer
                    .commit(
                            committable,
                            new CommitContext(
                                    Collections.singletonMap(
                                            FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY, offsets),
                                    Collections.emptyMap()))
                    .getCommittedSnapshotId();
        }
    }

    /** Commits to the table like another engine, without touching the Fluss domain metadata. */
    private void commitAsOtherWriter(String deltaPath) {
        TableManager.loadSnapshot(deltaPath)
                .build(engine)
                .buildUpdateTableTransaction("other-engine", Operation.WRITE)
                .build(engine)
                .commit(engine, CloseableIterable.emptyIterable());
    }

    /**
     * Commits a new metaData action built from the latest one, the way Spark alters a table. Delta
     * Kernel can't change the schema of a table without column mapping.
     */
    private void commitMetadataChange(String deltaPath, Consumer<ObjectNode> change)
            throws IOException {
        long version = TableManager.loadSnapshot(deltaPath).build(engine).getVersion();
        Path logDir = Paths.get(deltaPath, "_delta_log");
        ObjectNode metadata = null;
        for (long v = version; v >= 0 && metadata == null; v--) {
            for (String line : Files.readAllLines(logDir.resolve(String.format("%020d.json", v)))) {
                JsonNode action = readTree(line);
                if (action.has("metaData")) {
                    metadata = (ObjectNode) action.get("metaData");
                }
            }
        }
        assertThat(metadata).isNotNull();
        change.accept(metadata);
        ObjectNode action = OBJECT_MAPPER.createObjectNode();
        action.set("metaData", metadata);
        Files.write(
                logDir.resolve(String.format("%020d.json", version + 1)),
                Collections.singletonList(action.toString()),
                StandardOpenOption.CREATE_NEW);
    }

    private void assertCreateFlussTableRejected(TablePath tablePath) {
        assertThatThrownBy(
                        () ->
                                new DeltaLakeCatalog(configuration)
                                        .createTable(
                                                tablePath,
                                                logTableDescriptor(false),
                                                creatingFlussTable()))
                .isInstanceOf(TableAlreadyExistException.class)
                .hasMessageContaining("not empty");
    }

    private DeltaWriteResult writeOneRow(TablePath tablePath, TableInfo tableInfo)
            throws IOException {
        try (LakeWriter<DeltaWriteResult> writer =
                createLakeWriter(tablePath, 0, null, tableInfo)) {
            writer.write(new GenericRecord(0, 0, ChangeType.APPEND_ONLY, genRow(0, null, 0)));
            return writer.complete();
        }
    }

    /** The context of CREATE TABLE. The default one is turning on tiering for an existing table. */
    private static TestingLakeCatalogContext creatingFlussTable() {
        return new TestingLakeCatalogContext() {
            @Override
            public boolean isCreatingFlussTable() {
                return true;
            }
        };
    }

    private static JsonNode readTree(String json) {
        try {
            return OBJECT_MAPPER.readTree(json);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private TableInfo createTable(TablePath tablePath, boolean isPartitioned) throws Exception {
        return createTable(tablePath, logTableDescriptor(isPartitioned));
    }

    private TableInfo createTable(TablePath tablePath, TableDescriptor descriptor)
            throws Exception {
        new DeltaLakeCatalog(configuration)
                .createTable(tablePath, descriptor, new TestingLakeCatalogContext());
        return TableInfo.of(tablePath, 0, 1, descriptor, DEFAULT_REMOTE_DATA_DIR, 1L, 1L);
    }

    private static TableDescriptor logTableDescriptor(boolean isPartitioned) {
        TableDescriptor.Builder builder =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("a", DataTypes.INT())
                                        .column("b", DataTypes.STRING())
                                        .column("c", DataTypes.STRING())
                                        .column("tags", DataTypes.ARRAY(DataTypes.STRING()))
                                        .column("ts", DataTypes.TIMESTAMP_LTZ(3))
                                        .build())
                        .distributedBy(BUCKET_NUM)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true);
        if (isPartitioned) {
            builder.partitionedBy("c");
        }
        return builder.build();
    }

    private static GenericRow genRow(int bucket, @Nullable String partition, int i) {
        return GenericRow.of(
                i,
                BinaryString.fromString("bucket" + bucket + "_" + i),
                BinaryString.fromString(partition == null ? "bucket" + bucket : partition),
                new GenericArray(new Object[] {BinaryString.fromString("t" + i), null}),
                TimestampLtz.fromEpochMillis(i * 1000L));
    }

    private static String expectedRow(int bucket, @Nullable String partition, int i) {
        return String.format(
                "+I[%d, bucket%d_%d, %s, [t%d, null], %d]",
                i, bucket, i, partition == null ? "bucket" + bucket : partition, i, i * 1000_000L);
    }

    private LakeCommitter<DeltaWriteResult, DeltaCommittable> createLakeCommitter(
            TablePath tablePath, TableInfo tableInfo) throws IOException {
        return tieringFactory.createLakeCommitter(
                DeltaTestUtils.committerContext(tablePath, tableInfo));
    }

    private LakeWriter<DeltaWriteResult> createLakeWriter(
            TablePath tablePath, int bucket, @Nullable String partition, TableInfo tableInfo)
            throws IOException {
        return tieringFactory.createLakeWriter(
                DeltaTestUtils.writerContext(tablePath, bucket, partition, tableInfo));
    }
}
