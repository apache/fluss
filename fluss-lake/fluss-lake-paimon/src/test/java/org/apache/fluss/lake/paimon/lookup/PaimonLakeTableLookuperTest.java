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

package org.apache.fluss.lake.paimon.lookup;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.lakestorage.LakeTableLookuper;
import org.apache.fluss.lake.lakestorage.TestingLakeCatalogContext;
import org.apache.fluss.lake.paimon.PaimonLakeCatalog;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.row.encode.ValueDecoder;
import org.apache.fluss.row.encode.paimon.PaimonKeyEncoder;
import org.apache.fluss.types.DataTypes;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimon;
import static org.apache.fluss.lake.paimon.utils.PaimonTestUtils.CompactHelper;
import static org.apache.fluss.lake.paimon.utils.PaimonTestUtils.writeAndCommitData;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PaimonLakeTableLookuper}. */
class PaimonLakeTableLookuperTest {

    private static final String DB = "lookup_db";
    private static final short SCHEMA_ID = 1;
    private static final short EVOLVED_SCHEMA_ID = 2;

    @TempDir private File tempWarehouseDir;

    private Configuration paimonConfig;
    private PaimonLakeCatalog lakeCatalog;
    private Catalog paimonCatalog;

    @BeforeEach
    void setUp() {
        paimonConfig = new Configuration();
        paimonConfig.setString("warehouse", tempWarehouseDir.toURI().toString());
        lakeCatalog = new PaimonLakeCatalog(paimonConfig);
        paimonCatalog =
                CatalogFactory.createCatalog(
                        CatalogContext.create(Options.fromMap(paimonConfig.toMap())));
    }

    @AfterEach
    void tearDown() throws Exception {
        if (paimonCatalog != null) {
            paimonCatalog.close();
        }
        if (lakeCatalog != null) {
            lakeCatalog.close();
        }
    }

    @Test
    void testLookupPartitionedPrimaryKeyTable() throws Exception {
        TablePath tablePath = TablePath.of(DB, "partitioned_pk");
        Schema schema = pkSchema();
        TableDescriptor tableDescriptor = partitionedPkDescriptor(schema);
        FileStoreTable table = createPaimonTable(tablePath, tableDescriptor);
        writeAndCommitData(
                table,
                Collections.singletonMap(
                        0, Collections.singletonList(paimonRow(1, "20240101", "Alice"))));

        try (LakeTableLookuper lookuper =
                new PaimonLakeTableLookuper(paimonConfig, tablePath, KvFormat.COMPACTED)) {
            LakeTableLookuper.LookupContext context =
                    lookupContext(schema, "20240101", 0, SCHEMA_ID);

            byte[] value = lookuper.lookup(paimonKey(schema, 1, "20240101"), context);
            BinaryValue decodedValue = decodeValue(value, SCHEMA_ID, schema);

            assertThat(decodedValue.schemaId).isEqualTo(SCHEMA_ID);
            assertRow(decodedValue.row, 1, "20240101", "Alice");
            assertThat(lookuper.lookup(paimonKey(schema, 2, "20240101"), context)).isNull();
            assertThat(
                            lookuper.lookup(
                                    paimonKey(schema, 1, "20240101"),
                                    lookupContext(schema, "20240101", 1, SCHEMA_ID)))
                    .isNull();
            assertThat(lookuper.lookup(compactedKey(schema, 1, "20240101"), context)).isNull();
        }
    }

    @Test
    void testLookupWithIndexedKvFormat() throws Exception {
        TablePath tablePath = TablePath.of(DB, "indexed_kv_format");
        Schema schema = pkSchema();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder(partitionedPkDescriptor(schema))
                        .kvFormat(KvFormat.INDEXED)
                        .build();
        FileStoreTable table = createPaimonTable(tablePath, tableDescriptor);
        writeAndCommitData(
                table,
                Collections.singletonMap(
                        0, Collections.singletonList(paimonRow(1, "20240101", "Alice"))));

        try (LakeTableLookuper lookuper =
                new PaimonLakeTableLookuper(paimonConfig, tablePath, KvFormat.INDEXED)) {
            LakeTableLookuper.LookupContext context =
                    lookupContext(schema, "20240101", 0, SCHEMA_ID);

            BinaryValue decodedValue =
                    decodeValue(
                            lookuper.lookup(paimonKey(schema, 1, "20240101"), context),
                            SCHEMA_ID,
                            schema,
                            KvFormat.INDEXED);

            assertThat(decodedValue.schemaId).isEqualTo(SCHEMA_ID);
            assertRow(decodedValue.row, 1, "20240101", "Alice");
        }
    }

    @Test
    void testRefreshFilesAfterCompactionAndSnapshotExpiration() throws Exception {
        TablePath tablePath = TablePath.of(DB, "compacted_pk");
        Schema schema = pkSchema();
        FileStoreTable table = createPaimonTable(tablePath, partitionedPkDescriptor(schema));
        for (int id = 1; id <= 5; id++) {
            writeAndCommitData(
                    table,
                    Collections.singletonMap(
                            0, Collections.singletonList(paimonRow(id, "20240101", "name-" + id))));
        }

        BinaryRow partition = BinaryRow.singleColumn(BinaryString.fromString("20240101"));
        List<DataFileMeta> filesBeforeCompaction = dataFiles(table, partition, 0);
        assertThat(filesBeforeCompaction).hasSize(5);

        try (LakeTableLookuper lookuper =
                new PaimonLakeTableLookuper(paimonConfig, tablePath, KvFormat.COMPACTED)) {
            LakeTableLookuper.LookupContext context =
                    lookupContext(schema, "20240101", 0, SCHEMA_ID);
            assertThat(lookuper.lookup(paimonKey(schema, 5, "20240101"), context)).isNotNull();

            new CompactHelper(table, new File(tempWarehouseDir, "compact"))
                    .compactBucket(partition, 0)
                    .commit();
            assertThat(dataFiles(table, partition, 0)).hasSize(1);

            DataFilePathFactory pathFactory =
                    table.store().pathFactory().createDataFilePathFactory(partition, 0);
            List<Path> filesBeforeCompactionPaths = new ArrayList<>();
            for (DataFileMeta file : filesBeforeCompaction) {
                Path path = pathFactory.toPath(file);
                assertThat(table.store().snapshotManager().fileIO().exists(path)).isTrue();
                filesBeforeCompactionPaths.add(path);
            }

            Options expireOptions = new Options();
            expireOptions.set(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN, 1);
            expireOptions.set(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX, 1);
            // Compaction only marks the replaced files as deleted. Snapshot expiration performs
            // the physical cleanup that makes the stale lookuper fail to open an old data file.
            try (TableCommitImpl commit = table.copy(expireOptions.toMap()).newCommit("")) {
                commit.expireSnapshots();
            }
            for (Path path : filesBeforeCompactionPaths) {
                assertThat(table.store().snapshotManager().fileIO().exists(path)).isFalse();
            }

            BinaryValue decodedValue =
                    decodeValue(
                            lookuper.lookup(paimonKey(schema, 1, "20240101"), context),
                            SCHEMA_ID,
                            schema);
            assertRow(decodedValue.row, 1, "20240101", "name-1");
        }
    }

    @Test
    void testLookupWithNonStringPartitionKey() throws Exception {
        TablePath tablePath = TablePath.of(DB, "int_partition_pk");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("pt", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id", "pt")
                        .build();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .partitionedBy("pt")
                        .distributedBy(2, "id")
                        .build();
        FileStoreTable table = createPaimonTable(tablePath, tableDescriptor);
        writeAndCommitData(
                table,
                Collections.singletonMap(0, Collections.singletonList(paimonRow(1, 7, "Alice"))));

        try (LakeTableLookuper lookuper =
                new PaimonLakeTableLookuper(paimonConfig, tablePath, KvFormat.COMPACTED)) {
            LakeTableLookuper.LookupContext context =
                    new LakeTableLookuper.LookupContext(
                            ResolvedPartitionSpec.fromPartitionName(
                                    Collections.singletonList("pt"), "7"),
                            0,
                            SCHEMA_ID,
                            schema.getRowType());

            BinaryValue decodedValue =
                    decodeValue(
                            lookuper.lookup(paimonKey(schema, 1, 7), context), SCHEMA_ID, schema);

            assertThat(decodedValue.row.getInt(0)).isEqualTo(1);
            assertThat(decodedValue.row.getInt(1)).isEqualTo(7);
            assertThat(decodedValue.row.getString(2).toString()).isEqualTo("Alice");
        }
    }

    @Test
    void testRejectAppendOnlyTable() throws Exception {
        TablePath tablePath = TablePath.of(DB, "append_only");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .build();
        TableDescriptor tableDescriptor = TableDescriptor.builder().schema(schema).build();
        createPaimonTable(tablePath, tableDescriptor);

        try (LakeTableLookuper lookuper =
                new PaimonLakeTableLookuper(paimonConfig, tablePath, KvFormat.COMPACTED)) {
            LakeTableLookuper.LookupContext context =
                    new LakeTableLookuper.LookupContext(
                            new ResolvedPartitionSpec(
                                    Collections.emptyList(), Collections.emptyList()),
                            0,
                            SCHEMA_ID,
                            schema.getRowType());

            assertThatThrownBy(() -> lookuper.lookup(new byte[0], context))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("primary-key Paimon tables");
        }
    }

    @Test
    void testLookupAfterSchemaEvolutionPadsNewColumnsWithNull() throws Exception {
        TablePath tablePath = TablePath.of(DB, "schema_evolution_pk");
        Schema oldSchema = pkSchema();
        TableDescriptor oldDescriptor = partitionedPkDescriptor(oldSchema);
        FileStoreTable oldTable = createPaimonTable(tablePath, oldDescriptor);
        writeAndCommitData(
                oldTable,
                Collections.singletonMap(
                        0, Collections.singletonList(paimonRow(1, "20240101", "Alice"))));

        Schema newSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("dt", DataTypes.STRING())
                        .column("name", DataTypes.STRING())
                        .column("extra", DataTypes.STRING())
                        .primaryKey("id", "dt")
                        .build();
        TableDescriptor newDescriptor = partitionedPkDescriptor(newSchema);
        lakeCatalog.alterTable(
                tablePath,
                Collections.singletonList(
                        TableChange.addColumn(
                                "extra",
                                DataTypes.STRING(),
                                "extra column",
                                TableChange.ColumnPosition.last())),
                new TestingLakeCatalogContext(oldDescriptor, newDescriptor));
        FileStoreTable newTable = getPaimonTable(tablePath);
        writeAndCommitData(
                newTable,
                Collections.singletonMap(
                        0,
                        Collections.singletonList(paimonRow(2, "20240101", "Bob", "new-value"))));

        try (LakeTableLookuper lookuper =
                new PaimonLakeTableLookuper(paimonConfig, tablePath, KvFormat.COMPACTED)) {
            BinaryValue oldSchemaValue =
                    decodeValue(
                            lookuper.lookup(
                                    paimonKey(oldSchema, 1, "20240101"),
                                    lookupContext(oldSchema, "20240101", 0, SCHEMA_ID)),
                            SCHEMA_ID,
                            oldSchema);
            LakeTableLookuper.LookupContext context =
                    lookupContext(newSchema, "20240101", 0, EVOLVED_SCHEMA_ID);

            assertThat(oldSchemaValue.schemaId).isEqualTo(SCHEMA_ID);
            assertRow(oldSchemaValue.row, 1, "20240101", "Alice");

            BinaryValue oldValue =
                    decodeValue(
                            lookuper.lookup(paimonKey(newSchema, 1, "20240101"), context),
                            EVOLVED_SCHEMA_ID,
                            newSchema);
            assertThat(oldValue.schemaId).isEqualTo(EVOLVED_SCHEMA_ID);
            assertThat(oldValue.row.getInt(0)).isEqualTo(1);
            assertThat(oldValue.row.getString(2).toString()).isEqualTo("Alice");
            assertThat(oldValue.row.isNullAt(3)).isTrue();

            BinaryValue newValue =
                    decodeValue(
                            lookuper.lookup(paimonKey(newSchema, 2, "20240101"), context),
                            EVOLVED_SCHEMA_ID,
                            newSchema);
            assertThat(newValue.schemaId).isEqualTo(EVOLVED_SCHEMA_ID);
            assertThat(newValue.row.getInt(0)).isEqualTo(2);
            assertThat(newValue.row.getString(2).toString()).isEqualTo("Bob");
            assertThat(newValue.row.getString(3).toString()).isEqualTo("new-value");
        }
    }

    private FileStoreTable createPaimonTable(TablePath tablePath, TableDescriptor tableDescriptor)
            throws Exception {
        lakeCatalog.createTable(
                tablePath, tableDescriptor, new TestingLakeCatalogContext(tableDescriptor));
        return getPaimonTable(tablePath);
    }

    private FileStoreTable getPaimonTable(TablePath tablePath) throws Exception {
        refreshPaimonCatalog();
        return (FileStoreTable) paimonCatalog.getTable(toPaimon(tablePath));
    }

    private void refreshPaimonCatalog() throws Exception {
        if (paimonCatalog != null) {
            paimonCatalog.close();
        }
        paimonCatalog =
                CatalogFactory.createCatalog(
                        CatalogContext.create(Options.fromMap(paimonConfig.toMap())));
    }

    private static Schema pkSchema() {
        return Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("dt", DataTypes.STRING())
                .column("name", DataTypes.STRING())
                .primaryKey("id", "dt")
                .build();
    }

    private static TableDescriptor partitionedPkDescriptor(Schema schema) {
        return TableDescriptor.builder()
                .schema(schema)
                .partitionedBy("dt")
                .distributedBy(2, "id")
                .build();
    }

    private static LakeTableLookuper.LookupContext lookupContext(
            Schema schema, String partitionName, int bucket, short schemaId) {
        return new LakeTableLookuper.LookupContext(
                ResolvedPartitionSpec.fromPartitionName(
                        Collections.singletonList("dt"), partitionName),
                bucket,
                schemaId,
                schema.getRowType());
    }

    private static List<DataFileMeta> dataFiles(
            FileStoreTable table, BinaryRow partition, int bucket) {
        List<DataFileMeta> files = new ArrayList<>();
        for (Split split :
                table.newScan()
                        .withPartitionFilter(Collections.singletonList(partition))
                        .withBucket(bucket)
                        .plan()
                        .splits()) {
            if (split instanceof DataSplit) {
                files.addAll(((DataSplit) split).dataFiles());
            }
        }
        return files;
    }

    private static org.apache.paimon.data.GenericRow paimonRow(Object... fields) {
        Object[] rowFields = Arrays.copyOf(fields, fields.length + 3);
        for (int i = 0; i < fields.length; i++) {
            if (rowFields[i] instanceof String) {
                rowFields[i] =
                        org.apache.paimon.data.BinaryString.fromString((String) rowFields[i]);
            }
        }
        rowFields[fields.length] = 0;
        rowFields[fields.length + 1] = 0L;
        rowFields[fields.length + 2] = Timestamp.fromEpochMillis(0);
        return org.apache.paimon.data.GenericRow.of(rowFields);
    }

    private static byte[] paimonKey(Schema schema, int id, String dt) {
        return paimonKey(schema, Arrays.asList("id", "dt"), id, dt, "");
    }

    private static byte[] paimonKey(Schema schema, int id, int pt) {
        return paimonKey(schema, Arrays.asList("id", "pt"), id, pt, "");
    }

    private static byte[] paimonKey(Schema schema, List<String> keys, Object... fields) {
        return new PaimonKeyEncoder(schema.getRowType(), keys).encodeKey(row(fields));
    }

    private static byte[] compactedKey(Schema schema, int id, String dt) {
        return CompactedKeyEncoder.createKeyEncoder(schema.getRowType(), Arrays.asList("id", "dt"))
                .encodeKey(row(id, dt, ""));
    }

    private static BinaryValue decodeValue(byte[] value, short schemaId, Schema schema) {
        return decodeValue(value, schemaId, schema, KvFormat.COMPACTED);
    }

    private static BinaryValue decodeValue(
            byte[] value, short schemaId, Schema schema, KvFormat kvFormat) {
        return new ValueDecoder(new TestingSchemaGetter(schemaId, schema), kvFormat)
                .decodeValue(value);
    }

    private static void assertRow(InternalRow row, int id, String dt, String name) {
        assertThat(row.getInt(0)).isEqualTo(id);
        assertThat(row.getString(1).toString()).isEqualTo(dt);
        assertThat(row.getString(2).toString()).isEqualTo(name);
    }
}
