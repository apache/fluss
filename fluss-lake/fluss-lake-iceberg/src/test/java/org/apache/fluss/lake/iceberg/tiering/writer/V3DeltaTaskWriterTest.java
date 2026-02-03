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

package org.apache.fluss.lake.iceberg.tiering.writer;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.iceberg.tiering.IcebergCatalogProvider;
import org.apache.fluss.lake.iceberg.version.FormatVersionManager;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link V3DeltaTaskWriter}. */
class V3DeltaTaskWriterTest {

    private @TempDir File tempWarehouseDir;
    private Catalog catalog;

    @BeforeEach
    void setUp() {
        Configuration configuration = new Configuration();
        configuration.setString("warehouse", "file://" + tempWarehouseDir.getAbsolutePath());
        configuration.setString("type", "hadoop");
        configuration.setString("name", "test_catalog");
        IcebergCatalogProvider provider = new IcebergCatalogProvider(configuration);
        catalog = provider.get();

        // Create namespace
        Namespace namespace = Namespace.of("test_db");
        if (catalog instanceof SupportsNamespaces) {
            ((SupportsNamespaces) catalog).createNamespace(namespace);
        }
    }

    @Test
    void testV2TableWriterDoesNotUseDVs() throws Exception {
        Table table = createTableWithFormatVersion(2);

        // Verify the table doesn't support DVs
        assertThat(FormatVersionManager.supportsDeletionVectors(table)).isFalse();
        assertThat(FormatVersionManager.detectFormatVersion(table)).isEqualTo(2);

        // Create writer
        V3DeltaTaskWriter writer = createWriter(table);

        // Write some records
        Record record1 = createRecord(table.schema(), 1L, "test1");
        Record record2 = createRecord(table.schema(), 2L, "test2");
        writer.write(record1);
        writer.write(record2);

        // Complete the write
        WriteResult result = writer.complete();

        // V2 tables should produce data files
        assertThat(result.dataFiles()).hasSize(1);
    }

    @Test
    void testV3TableWriterSupportsDVs() throws Exception {
        Table table = createTableWithFormatVersion(3);

        // Verify the table supports DVs
        assertThat(FormatVersionManager.supportsDeletionVectors(table)).isTrue();
        assertThat(FormatVersionManager.detectFormatVersion(table)).isEqualTo(3);

        // Create writer
        V3DeltaTaskWriter writer = createWriter(table);

        // Write some records
        Record record1 = createRecord(table.schema(), 1L, "test1");
        Record record2 = createRecord(table.schema(), 2L, "test2");
        writer.write(record1);
        writer.write(record2);

        // Complete the write
        WriteResult result = writer.complete();

        // V3 tables should produce data files
        assertThat(result.dataFiles()).hasSize(1);
    }

    @Test
    void testWriteAndDeleteInSameBatch() throws Exception {
        Table table = createTableWithFormatVersion(3);

        // Create writer
        V3DeltaTaskWriter writer = createWriter(table);

        // Write a record
        Record record1 = createRecord(table.schema(), 1L, "test1");
        writer.write(record1);

        // Update the same record (delete + insert)
        Record record1Updated = createRecord(table.schema(), 1L, "test1_updated");
        writer.delete(record1);
        writer.write(record1Updated);

        // Write another record
        Record record2 = createRecord(table.schema(), 2L, "test2");
        writer.write(record2);

        // Complete the write
        WriteResult result = writer.complete();

        // Should have data files
        assertThat(result.dataFiles()).isNotEmpty();
    }

    @Test
    void testFormatVersionDetection() {
        Table v2Table = createTableWithFormatVersion(2);
        Table v3Table = createTableWithFormatVersion(3);

        assertThat(FormatVersionManager.detectFormatVersion(v2Table)).isEqualTo(2);
        assertThat(FormatVersionManager.detectFormatVersion(v3Table)).isEqualTo(3);

        assertThat(FormatVersionManager.supportsDeletionVectors(v2Table)).isFalse();
        assertThat(FormatVersionManager.supportsDeletionVectors(v3Table)).isTrue();
    }

    @Test
    void testV3IntraBatchUpdateProducesPuffinDeleteFile() throws Exception {
        Table table = createTableWithFormatVersion(3);
        V3DeltaTaskWriter writer = createWriter(table);

        // Write record then update same key (triggers intra-batch position delete)
        writer.write(createRecord(table.schema(), 1L, "original"));
        writer.write(createRecord(table.schema(), 1L, "updated"));

        WriteResult result = writer.complete();

        // Should have delete files in Puffin format for V3
        assertThat(result.deleteFiles()).isNotEmpty();
        DeleteFile deleteFile = result.deleteFiles()[0];
        assertThat(deleteFile.format()).isEqualTo(FileFormat.PUFFIN);
    }

    @Test
    void testV3DeleteFileHasReferencedDataFile() throws Exception {
        Table table = createTableWithFormatVersion(3);
        V3DeltaTaskWriter writer = createWriter(table);

        // Write record then update same key
        writer.write(createRecord(table.schema(), 1L, "original"));
        writer.write(createRecord(table.schema(), 1L, "updated"));

        WriteResult result = writer.complete();

        // DV delete files should reference the data file
        assertThat(result.deleteFiles()).isNotEmpty();
        DeleteFile deleteFile = result.deleteFiles()[0];
        assertThat(deleteFile.referencedDataFile()).isNotNull();
    }

    @Test
    void testV2IntraBatchUpdateUsesTraditionalPositionDelete() throws Exception {
        Table table = createTableWithFormatVersion(2);
        V3DeltaTaskWriter writer = createWriter(table);

        // Write record then update same key
        writer.write(createRecord(table.schema(), 1L, "original"));
        writer.write(createRecord(table.schema(), 1L, "updated"));

        WriteResult result = writer.complete();

        // V2 should use traditional position deletes (PARQUET), not Puffin
        assertThat(result.deleteFiles()).isNotEmpty();
        DeleteFile deleteFile = result.deleteFiles()[0];
        assertThat(deleteFile.format()).isNotEqualTo(FileFormat.PUFFIN);
    }

    @Test
    void testV3UniqueKeysProducesNoDV() throws Exception {
        Table table = createTableWithFormatVersion(3);
        V3DeltaTaskWriter writer = createWriter(table);

        // Write unique keys - no updates
        writer.write(createRecord(table.schema(), 1L, "a"));
        writer.write(createRecord(table.schema(), 2L, "b"));
        writer.write(createRecord(table.schema(), 3L, "c"));

        WriteResult result = writer.complete();

        // No duplicate keys = no deletions = no DV file
        assertThat(result.deleteFiles()).isEmpty();
        assertThat(result.dataFiles()).hasSize(1);
    }

    @Test
    void testV3MultipleUpdatesToSameKey() throws Exception {
        Table table = createTableWithFormatVersion(3);
        V3DeltaTaskWriter writer = createWriter(table);

        // Write same key 3 times
        writer.write(createRecord(table.schema(), 1L, "v1")); // position 0 -> deleted
        writer.write(createRecord(table.schema(), 1L, "v2")); // position 1 -> deleted
        writer.write(createRecord(table.schema(), 1L, "v3")); // position 2 -> live

        WriteResult result = writer.complete();

        // Should have DV with 2 deleted positions (0 and 1)
        assertThat(result.deleteFiles()).hasSize(1);
        DeleteFile dvFile = result.deleteFiles()[0];
        assertThat(dvFile.format()).isEqualTo(FileFormat.PUFFIN);

        // Verify positions 0 and 1 are deleted
        PositionDeleteIndex deleteIndex = readDVFile(table, dvFile);
        assertThat(deleteIndex.isDeleted(0)).isTrue();
        assertThat(deleteIndex.isDeleted(1)).isTrue();
        assertThat(deleteIndex.isDeleted(2)).isFalse();
    }

    @Test
    void testV3InterleavedUpdatesMultipleKeys() throws Exception {
        Table table = createTableWithFormatVersion(3);
        V3DeltaTaskWriter writer = createWriter(table);

        // Interleaved updates for two keys
        writer.write(createRecord(table.schema(), 1L, "a1")); // pos 0 -> deleted (key 1)
        writer.write(createRecord(table.schema(), 2L, "b1")); // pos 1 -> deleted (key 2)
        writer.write(createRecord(table.schema(), 1L, "a2")); // pos 2 -> live (key 1)
        writer.write(createRecord(table.schema(), 2L, "b2")); // pos 3 -> live (key 2)

        WriteResult result = writer.complete();

        assertThat(result.deleteFiles()).hasSize(1);
        PositionDeleteIndex deleteIndex = readDVFile(table, result.deleteFiles()[0]);

        // Positions 0 and 1 should be deleted
        assertThat(deleteIndex.isDeleted(0)).isTrue();
        assertThat(deleteIndex.isDeleted(1)).isTrue();
        // Positions 2 and 3 should be live
        assertThat(deleteIndex.isDeleted(2)).isFalse();
        assertThat(deleteIndex.isDeleted(3)).isFalse();
    }

    @Test
    void testV3DVRecordCountMatchesDeletedPositions() throws Exception {
        Table table = createTableWithFormatVersion(3);
        V3DeltaTaskWriter writer = createWriter(table);

        // 3 updates to key=1 (2 deletes) + 2 updates to key=2 (1 delete) = 3 total deletes
        writer.write(createRecord(table.schema(), 1L, "v1"));
        writer.write(createRecord(table.schema(), 1L, "v2"));
        writer.write(createRecord(table.schema(), 1L, "v3")); // 2 deletes for key 1
        writer.write(createRecord(table.schema(), 2L, "x1"));
        writer.write(createRecord(table.schema(), 2L, "x2")); // 1 delete for key 2

        WriteResult result = writer.complete();

        DeleteFile dvFile = result.deleteFiles()[0];
        // DV should report 3 deleted positions
        assertThat(dvFile.recordCount()).isEqualTo(3);
    }

    @Test
    void testV3DVContainsCorrectDeletedPosition() throws Exception {
        Table table = createTableWithFormatVersion(3);
        V3DeltaTaskWriter writer = createWriter(table);

        // Write same key twice
        writer.write(createRecord(table.schema(), 1L, "original")); // position 0
        writer.write(createRecord(table.schema(), 1L, "updated")); // position 1

        WriteResult result = writer.complete();
        DeleteFile dvFile = result.deleteFiles()[0];

        // Read the DV and verify position 0 is marked deleted
        PositionDeleteIndex deleteIndex = readDVFile(table, dvFile);
        assertThat(deleteIndex.isDeleted(0)).isTrue(); // original deleted
        assertThat(deleteIndex.isDeleted(1)).isFalse(); // updated is live
    }

    @Test
    void testV3DVReferencesCorrectDataFile() throws Exception {
        Table table = createTableWithFormatVersion(3);
        V3DeltaTaskWriter writer = createWriter(table);

        writer.write(createRecord(table.schema(), 1L, "original"));
        writer.write(createRecord(table.schema(), 1L, "updated"));

        WriteResult result = writer.complete();

        DataFile dataFile = result.dataFiles()[0];
        DeleteFile dvFile = result.deleteFiles()[0];

        // DV should reference the data file it applies to
        assertThat(dvFile.referencedDataFile()).isEqualTo(dataFile.path().toString());
    }

    @Test
    void testV3ReadSkipsDeletedPositions() throws Exception {
        Table table = createTableWithFormatVersion(3);
        V3DeltaTaskWriter writer = createWriter(table);

        writer.write(createRecord(table.schema(), 1L, "original"));
        writer.write(createRecord(table.schema(), 1L, "updated"));
        writer.write(createRecord(table.schema(), 2L, "other"));

        WriteResult writeResult = writer.complete();

        // Commit to table using RowDelta
        table.newRowDelta()
                .addRows(writeResult.dataFiles()[0])
                .addDeletes(writeResult.deleteFiles()[0])
                .commit();

        // Read back - should only see 2 rows (updated + other), not 3
        List<Record> records = readAllRecords(table);

        assertThat(records).hasSize(2);
        List<String> names = new ArrayList<>();
        for (Record r : records) {
            names.add((String) r.getField("name"));
        }
        assertThat(names).containsExactlyInAnyOrder("updated", "other");
        assertThat(names).doesNotContain("original");
    }

    private Table createTableWithFormatVersion(int formatVersion) {
        // Schema with primary key (id)
        Set<Integer> identifierFieldIds = new HashSet<>();
        identifierFieldIds.add(1);

        Schema schema =
                new Schema(
                        Types.NestedField.required(1, "id", Types.LongType.get()),
                        Types.NestedField.optional(2, "name", Types.StringType.get()),
                        Types.NestedField.required(3, "__bucket", Types.IntegerType.get()));

        String tableName = "test_table_v" + formatVersion + "_" + System.currentTimeMillis();
        TableIdentifier tableId = TableIdentifier.of("test_db", tableName);

        Map<String, String> properties = new HashMap<>();
        properties.put("format-version", String.valueOf(formatVersion));

        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("__bucket").build();

        catalog.createTable(tableId, schema, spec, properties);
        Table table = catalog.loadTable(tableId);

        // Set identifier fields after loading
        table.updateSchema().setIdentifierFields("id").commit();

        return table;
    }

    private V3DeltaTaskWriter createWriter(Table table) {
        Schema schema = table.schema();
        int[] equalityFieldIds =
                schema.identifierFieldIds().stream().mapToInt(Integer::intValue).toArray();

        FileFormat format = FileFormat.PARQUET;
        long targetFileSize = 128 * 1024 * 1024; // 128MB

        OutputFileFactory outputFileFactory =
                OutputFileFactory.builderFor(table, 0, 0).format(format).build();

        FileAppenderFactory<Record> appenderFactory =
                new GenericAppenderFactory(schema, table.spec(), equalityFieldIds, schema, null);

        Schema deleteSchema = schema.select("id");

        return new V3DeltaTaskWriter(
                table,
                deleteSchema,
                format,
                appenderFactory,
                outputFileFactory,
                table.io(),
                targetFileSize,
                null, // no partition
                0 // bucket 0
                );
    }

    private Record createRecord(Schema schema, long id, String name) {
        GenericRecord record = GenericRecord.create(schema);
        record.setField("id", id);
        record.setField("name", name);
        record.setField("__bucket", 0);
        return record;
    }

    private PositionDeleteIndex readDVFile(Table table, DeleteFile dvFile) {
        BaseDeleteLoader deleteLoader =
                new BaseDeleteLoader(deleteFile -> table.io().newInputFile(deleteFile.location()));
        return deleteLoader.loadPositionDeletes(List.of(dvFile), dvFile.referencedDataFile());
    }

    private List<Record> readAllRecords(Table table) throws IOException {
        List<Record> records = new ArrayList<>();
        try (CloseableIterable<Record> iterable = IcebergGenerics.read(table).build()) {
            for (Record record : iterable) {
                records.add(record);
            }
        }
        return records;
    }
}
