/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.iceberg.tiering;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.iceberg.maintenance.RewriteDataFileResult;
import org.apache.fluss.lake.iceberg.tiering.writer.TaskWriterFactory;
import org.apache.fluss.metadata.TablePath;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DataFileSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;
import static org.apache.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;
import static org.apache.fluss.utils.Preconditions.checkState;
import static org.assertj.core.api.Assertions.assertThat;

/** UT for {@link IcebergLakeCommitter} operations. */
class IcebergLakeCommitterTest {

    private @TempDir File tempWarehouseDir;
    private Catalog icebergCatalog;
    private IcebergCatalogProvider catalogProvider;

    @BeforeEach
    void setUp() {
        Configuration configuration = new Configuration();
        configuration.setString("warehouse", "file://" + tempWarehouseDir);
        configuration.setString("type", "hadoop");
        configuration.setString("name", "test");
        catalogProvider = new IcebergCatalogProvider(configuration);
        icebergCatalog = catalogProvider.get();
    }

    /**
     * Reproduces the rewrite conflict bug documented in GitHub issue #2420.
     *
     * <p>This test reproduces the scenario from IcebergRewriteITCase where PK table writes with
     * auto-compaction enabled cause blocking. The flow is:
     *
     * <ol>
     *   <li>IcebergLakeWriter writes data to a PK table, generating delete files for updates
     *   <li>Auto-compaction runs concurrently, producing RewriteDataFileResult
     *   <li>complete() combines both: WriteResult (data + delete files) + RewriteDataFileResult
     *   <li>IcebergLakeCommitter.commit() receives both in the same cycle
     * </ol>
     *
     * <p>Bug behavior: RowDelta commits first (because delete files are present), advancing the
     * snapshot from N to N+1. The rewrite then calls validateFromSnapshot(N), but N is now stale.
     * Iceberg's retry loop detects the mismatch and retries, but the conflict is permanent because
     * the validation snapshot is fixed. With default Iceberg settings
     * (commit.retry.total-timeout-ms = 30 minutes), this causes blocking for 15+ minutes before
     * failing.
     */
    @Test
    void testRewriteFailsWhenDeleteFilesAddedInSameCycle() throws Exception {
        // Create table and seed with initial data files
        TablePath tablePath = TablePath.of("iceberg", "conflict_test_table");
        createTable(tablePath);
        Table icebergTable = icebergCatalog.loadTable(toIceberg(tablePath));
        int bucket = 0;

        // Write 3 initial data files that will be the target of the rewrite
        appendDataFiles(icebergTable, 3, 10, 0, bucket);
        icebergTable.refresh();

        int initialFileCount = countDataFiles(icebergTable);
        assertThat(initialFileCount).isEqualTo(3);

        // Capture the current snapshot ID - this is what the rewrite will validate against
        long rewriteSnapshotId = icebergTable.currentSnapshot().snapshotId();

        // Collect the data files to be rewritten and get one to reference in delete file
        DataFileSet filesToDelete = DataFileSet.create();
        DataFile firstDataFile = null;
        for (FileScanTask task : icebergTable.newScan().planFiles()) {
            filesToDelete.add(task.file());
            if (firstDataFile == null) {
                firstDataFile = task.file();
            }
        }
        assertThat(filesToDelete).hasSize(3);
        assertThat(firstDataFile).isNotNull();

        // Create the rewrite result: compacting the 3 files into 1
        DataFileSet filesToAdd = DataFileSet.create();
        filesToAdd.add(writeDataFile(icebergTable, 30, 0, bucket));

        RewriteDataFileResult rewriteResult =
                new RewriteDataFileResult(rewriteSnapshotId, filesToDelete, filesToAdd);

        // Create a delete file that references one of the data files being rewritten
        // IMPORTANT: when RowDelta commits this delete file, it will conflict with the rewrite's
        // validation
        // since the file now has pending deletes
        DeleteFile deleteFile =
                createDeleteFile(icebergTable, firstDataFile.partition(), firstDataFile.location());

        // Create new data file to include in the commit
        DataFile newDataFile = writeDataFile(icebergTable, 5, 100, bucket);

        // Build a committable with data file, delete file, AND rewrite results
        // The presence of delete file triggers RowDelta path which commits first
        IcebergCommittable committable =
                IcebergCommittable.builder()
                        .addDataFile(newDataFile)
                        .addDeleteFile(deleteFile)
                        .addRewriteDataFileResult(rewriteResult)
                        .build();

        // Perform the commit operation (to trigger the bug)
        IcebergLakeCommitter committer = new IcebergLakeCommitter(catalogProvider, tablePath);
        committer.commit(committable, Collections.emptyMap());
        committer.close();

        // Verify that the rewrite will _silently_ resulting in the incorrect file counts
        icebergTable.refresh();
        int finalFileCount = countDataFiles(icebergTable);
        assertThat(finalFileCount)
                .as(
                        "Rewrite failed silently. Expecting 4 files "
                                + "(3 original + 1 new) to demonstrate bug, should otherwise be 2 (due to compaction).")
                .isEqualTo(4);
    }

    @Test
    void testRewriteWithDataFilesSucceeds() throws Exception {
        // Create table with minimal retry configuration to fail fast instead of blocking
        Map<String, String> tableProps = new HashMap<>();
        tableProps.put("commit.retry.num-retries", "2");
        tableProps.put("commit.retry.min-wait-ms", "10");
        tableProps.put("commit.retry.max-wait-ms", "100");

        TablePath tablePath = TablePath.of("iceberg", "data_rewrite_conflict_table");
        createTable(tablePath, tableProps);
        Table icebergTable = icebergCatalog.loadTable(toIceberg(tablePath));
        int bucket = 0;

        // Write 3 initial data files
        appendDataFiles(icebergTable, 3, 10, 0, bucket);
        icebergTable.refresh();

        int initialFileCount = countDataFiles(icebergTable);
        assertThat(initialFileCount).isEqualTo(3);

        long rewriteSnapshotId = icebergTable.currentSnapshot().snapshotId();

        // Create the rewrite result: compacting the 3 files into 1
        DataFileSet filesToDelete = DataFileSet.create();
        icebergTable.newScan().planFiles().forEach(task -> filesToDelete.add(task.file()));
        assertThat(filesToDelete).hasSize(3);

        DataFileSet filesToAdd = DataFileSet.create();
        filesToAdd.add(writeDataFile(icebergTable, 30, 0, bucket));

        RewriteDataFileResult rewriteResult =
                new RewriteDataFileResult(rewriteSnapshotId, filesToDelete, filesToAdd);

        // Create new data file (NO delete files - simple append path)
        DataFile newDataFile = writeDataFile(icebergTable, 5, 100, bucket);

        // Build committable with ONLY data file + rewrite (no delete files)
        IcebergCommittable committable =
                IcebergCommittable.builder()
                        .addDataFile(newDataFile)
                        .addRewriteDataFileResult(rewriteResult)
                        .build();

        // Perform the commit operation
        IcebergLakeCommitter committer = new IcebergLakeCommitter(catalogProvider, tablePath);
        committer.commit(committable, Collections.emptyMap());
        committer.close();

        // Verify that commit and rewrite worked as expected
        icebergTable.refresh();
        int finalFileCount = countDataFiles(icebergTable);
        assertThat(finalFileCount)
                .as("With data files only, rewrite succeeds - got %d files", finalFileCount)
                .isEqualTo(2);
    }

    @Test
    void testRewriteOnlyCommitSucceeds() throws Exception {
        TablePath tablePath = TablePath.of("iceberg", "rewrite_only_table");
        createTable(tablePath);
        Table icebergTable = icebergCatalog.loadTable(toIceberg(tablePath));
        int bucket = 0;

        // Seed with files to rewrite
        appendDataFiles(icebergTable, 3, 10, 0, bucket);
        icebergTable.refresh();

        long snapshotId = icebergTable.currentSnapshot().snapshotId();

        // Collect files to delete
        DataFileSet filesToDelete = DataFileSet.create();
        icebergTable.newScan().planFiles().forEach(task -> filesToDelete.add(task.file()));

        // Create compacted replacement file
        DataFileSet filesToAdd = DataFileSet.create();
        filesToAdd.add(writeDataFile(icebergTable, 30, 0, bucket));

        RewriteDataFileResult rewriteResult =
                new RewriteDataFileResult(snapshotId, filesToDelete, filesToAdd);

        // Committable with ONLY rewrite results, no new data files
        IcebergCommittable committable =
                IcebergCommittable.builder().addRewriteDataFileResult(rewriteResult).build();

        IcebergLakeCommitter committer = new IcebergLakeCommitter(catalogProvider, tablePath);

        // This should complete successfully and create a new snapshot
        long resultSnapshotId = committer.commit(committable, Collections.emptyMap());
        assertThat(resultSnapshotId).isNotEqualTo(snapshotId);

        // Verify the rewrite completed (should have 1 file instead of 3)
        icebergTable.refresh();
        int finalFileCount = countDataFiles(icebergTable);
        assertThat(finalFileCount).isEqualTo(1);

        committer.close();
    }

    @Test
    void testCommitSucceeds() throws Exception {
        TablePath tablePath = TablePath.of("iceberg", "data_only_table");
        createTable(tablePath);
        Table icebergTable = icebergCatalog.loadTable(toIceberg(tablePath));
        int bucket = 0;

        DataFile newDataFile = writeDataFile(icebergTable, 10, 0, bucket);

        IcebergCommittable committable =
                IcebergCommittable.builder().addDataFile(newDataFile).build();

        IcebergLakeCommitter committer = new IcebergLakeCommitter(catalogProvider, tablePath);

        long snapshotId = committer.commit(committable, Collections.emptyMap());
        assertThat(snapshotId).isGreaterThan(0);

        committer.close();
    }

    /** Tests that committing data files with delete files (no rewrite) succeeds. */
    @Test
    void testCommitWithDeleteFilesSucceeds() throws Exception {
        TablePath tablePath = TablePath.of("iceberg", "data_delete_table");
        createTable(tablePath);
        Table icebergTable = icebergCatalog.loadTable(toIceberg(tablePath));
        int bucket = 0;

        // First, create some initial data
        appendDataFiles(icebergTable, 1, 10, 0, bucket);
        icebergTable.refresh();

        DataFile existingFile = icebergTable.newScan().planFiles().iterator().next().file();

        // Create new data file and a delete file (simulates PK table update without compaction)
        DataFile newDataFile = writeDataFile(icebergTable, 5, 100, bucket);
        DeleteFile deleteFile =
                createDeleteFile(icebergTable, existingFile.partition(), existingFile.location());

        IcebergCommittable committable =
                IcebergCommittable.builder()
                        .addDataFile(newDataFile)
                        .addDeleteFile(deleteFile)
                        .build();

        IcebergLakeCommitter committer = new IcebergLakeCommitter(catalogProvider, tablePath);

        long snapshotId = committer.commit(committable, Collections.emptyMap());
        assertThat(snapshotId).isGreaterThan(0);

        // Verify that we should have 2 data files (1 original + 1 new)
        icebergTable.refresh();
        int finalFileCount = countDataFiles(icebergTable);
        assertThat(finalFileCount).isEqualTo(2);

        committer.close();
    }

    // Helper methods

    private void createTable(TablePath tablePath) {
        createTable(tablePath, Collections.emptyMap());
    }

    private void createTable(TablePath tablePath, Map<String, String> properties) {
        Namespace namespace = Namespace.of(tablePath.getDatabaseName());
        SupportsNamespaces ns = (SupportsNamespaces) icebergCatalog;
        if (!ns.namespaceExists(namespace)) {
            ns.createNamespace(namespace);
        }

        Schema schema =
                new Schema(
                        Types.NestedField.optional(1, "c1", Types.IntegerType.get()),
                        Types.NestedField.optional(2, "c2", Types.StringType.get()),
                        Types.NestedField.required(3, BUCKET_COLUMN_NAME, Types.IntegerType.get()),
                        Types.NestedField.required(4, OFFSET_COLUMN_NAME, Types.LongType.get()),
                        Types.NestedField.required(
                                5, TIMESTAMP_COLUMN_NAME, Types.TimestampType.withZone()));

        PartitionSpec partitionSpec =
                PartitionSpec.builderFor(schema).identity(BUCKET_COLUMN_NAME).build();
        TableIdentifier tableId =
                TableIdentifier.of(tablePath.getDatabaseName(), tablePath.getTableName());
        icebergCatalog.createTable(tableId, schema, partitionSpec, properties);
    }

    private void appendDataFiles(
            Table table, int numFiles, int rowsPerFile, int baseOffset, int bucket)
            throws Exception {
        AppendFiles append = table.newAppend();
        for (int i = 0; i < numFiles; i++) {
            append.appendFile(
                    writeDataFile(table, rowsPerFile, baseOffset + (i * rowsPerFile), bucket));
        }
        append.commit();
    }

    private DataFile writeDataFile(Table table, int rows, int startOffset, int bucket)
            throws Exception {
        try (TaskWriter<Record> taskWriter =
                TaskWriterFactory.createTaskWriter(table, null, bucket)) {
            for (int i = 0; i < rows; i++) {
                Record r = GenericRecord.create(table.schema());
                r.setField("c1", i);
                r.setField("c2", "value_" + i);
                r.setField(BUCKET_COLUMN_NAME, bucket);
                r.setField(OFFSET_COLUMN_NAME, (long) (startOffset + i));
                r.setField(TIMESTAMP_COLUMN_NAME, OffsetDateTime.now(ZoneOffset.UTC));
                taskWriter.write(r);
            }
            DataFile[] dataFiles = taskWriter.dataFiles();
            checkState(dataFiles.length == 1);
            return dataFiles[0];
        }
    }

    private int countDataFiles(Table table) throws IOException {
        int count = 0;
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask ignored : tasks) {
                count++;
            }
        }
        return count;
    }

    private DeleteFile createDeleteFile(
            Table table, StructLike partition, CharSequence dataFilePath) {
        // Create a position delete file that references the given data file
        // This simulates a delete operation on the same file being rewritten
        return FileMetadata.deleteFileBuilder(table.spec())
                .withPath(dataFilePath.toString() + ".deletes.parquet")
                .withFileSizeInBytes(1024)
                .withPartition(partition)
                .withRecordCount(1)
                .withFormat(FileFormat.PARQUET)
                .ofPositionDeletes()
                .build();
    }
}
