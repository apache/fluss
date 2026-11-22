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

package org.apache.fluss.lake.iceberg.tiering;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.committer.LakeCommitResult;
import org.apache.fluss.lake.committer.LakeCommitter.CommitContext;
import org.apache.fluss.lake.iceberg.maintenance.IcebergRewriteDataFiles;
import org.apache.fluss.lake.iceberg.maintenance.RewriteDataFileResult;
import org.apache.fluss.lake.iceberg.tiering.writer.TaskWriterFactory;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;
import static org.apache.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests the failure handling of {@link IcebergLakeCommitter}'s compaction rewrite commit. */
class IcebergLakeCommitterTest {

    private static final TablePath TABLE_PATH = TablePath.of("iceberg", "rewrite_failure_table");
    private static final int BUCKET = 0;

    private @TempDir File tempWarehouseDir;
    private Configuration configuration;
    private Catalog icebergCatalog;

    @BeforeEach
    void beforeEach() {
        configuration = new Configuration();
        configuration.setString("warehouse", "file://" + tempWarehouseDir);
        configuration.setString("type", "hadoop");
        configuration.setString("name", "test");
        icebergCatalog = new IcebergCatalogProvider(configuration).get();
    }

    @Test
    void testRewriteKeepsAddedFilesWhenCommitStateUnknown() throws Exception {
        // The commit reaches the catalog, but its response is lost, as with a REST 5xx/timeout.
        CommitFailureInjectingCatalog failingCatalog =
                newFailingCatalog(
                        new CommitStateUnknownException(new IOException("response lost")), true);
        Table table = createTableWithSmallFiles();
        long rowsBefore = countRows(table);
        long snapshotBefore = table.currentSnapshot().snapshotId();
        RewriteDataFileResult rewrite = planRewrite(table);

        LakeCommitResult result = commitRewrites(failingCatalog, rewrite);

        // The rewrite snapshot is live, so the files it added must survive.
        Set<String> added = locationsOf(rewrite.addedDataFiles());
        assertThat(added).isNotEmpty().allMatch(location -> exists(table, location));
        table.refresh();
        assertThat(currentDataFiles(table)).isEqualTo(added);
        assertThat(countRows(table)).isEqualTo(rowsBefore);
        assertFallsBackToDataCommit(table, result, snapshotBefore);
    }

    @Test
    void testRewriteDeletesAddedFilesWhenCommitFailed() throws Exception {
        CommitFailureInjectingCatalog failingCatalog =
                newFailingCatalog(new CommitFailedException("conflicting commit"), false);
        Table table = createTableWithSmallFiles();
        long rowsBefore = countRows(table);
        long snapshotBefore = table.currentSnapshot().snapshotId();
        Set<String> filesBefore = currentDataFiles(table);
        RewriteDataFileResult rewrite = planRewrite(table);

        LakeCommitResult result = commitRewrites(failingCatalog, rewrite);

        // The commit definitely failed, so the added files are orphans and are removed.
        Set<String> added = locationsOf(rewrite.addedDataFiles());
        assertThat(added).isNotEmpty().noneMatch(location -> exists(table, location));
        table.refresh();
        assertThat(currentDataFiles(table)).isEqualTo(filesBefore);
        assertThat(countRows(table)).isEqualTo(rowsBefore);
        assertFallsBackToDataCommit(table, result, snapshotBefore);
    }

    @Test
    void testRewriteDeletesAddedFilesWhenConflictingWithConcurrentDelete() throws Exception {
        Table table = createTableWithSmallFiles();
        RewriteDataFileResult rewrite = planRewrite(table);
        // Another writer removes a file the rewrite replaces, so Iceberg rejects the rewrite
        // with a ValidationException, without any injected failure.
        DataFile concurrentlyDeleted = rewrite.deletedDataFiles().iterator().next();
        table.newDelete().deleteFile(concurrentlyDeleted).commit();
        table.refresh();
        long rowsBefore = countRows(table);
        long snapshotBefore = table.currentSnapshot().snapshotId();
        Set<String> filesBefore = currentDataFiles(table);

        LakeCommitResult result = commitRewrites(icebergCatalog, rewrite);

        Set<String> added = locationsOf(rewrite.addedDataFiles());
        assertThat(added).isNotEmpty().noneMatch(location -> exists(table, location));
        table.refresh();
        assertThat(currentDataFiles(table)).isEqualTo(filesBefore);
        assertThat(countRows(table)).isEqualTo(rowsBefore);
        assertFallsBackToDataCommit(table, result, snapshotBefore);
    }

    @Test
    void testRewriteDeletesAddedFilesWhenFailingBeforeCommit() throws Exception {
        Table table = createTableWithSmallFiles();
        RewriteDataFileResult first = planRewrite(table);
        table.newAppend().appendFile(writeDataFile(table, 100L)).commit();
        table.refresh();
        RewriteDataFileResult second = planRewrite(table);
        long rowsBefore = countRows(table);
        long snapshotBefore = table.currentSnapshot().snapshotId();
        Set<String> filesBefore = currentDataFiles(table);

        // Rewrites planned from different snapshots are rejected before the commit is sent.
        LakeCommitResult result = commitRewrites(icebergCatalog, first, second);

        Set<String> added = locationsOf(first.addedDataFiles());
        added.addAll(locationsOf(second.addedDataFiles()));
        assertThat(added).isNotEmpty().noneMatch(location -> exists(table, location));
        table.refresh();
        assertThat(currentDataFiles(table)).isEqualTo(filesBefore);
        assertThat(countRows(table)).isEqualTo(rowsBefore);
        assertFallsBackToDataCommit(table, result, snapshotBefore);
    }

    /** Commits a tiering round that carries no new data but the given rewrites. */
    private LakeCommitResult commitRewrites(Catalog catalog, RewriteDataFileResult... rewrites)
            throws Exception {
        IcebergCatalogProvider provider =
                new IcebergCatalogProvider(configuration) {
                    @Override
                    public Catalog get() {
                        return catalog;
                    }
                };
        IcebergCommittable.Builder committable = IcebergCommittable.builder();
        for (RewriteDataFileResult rewrite : rewrites) {
            committable.addRewriteDataFileResult(rewrite);
        }
        try (IcebergLakeCommitter committer = new IcebergLakeCommitter(provider, TABLE_PATH)) {
            return committer.commit(
                    committable.build(),
                    new CommitContext(Collections.emptyMap(), Collections.emptyMap()));
        }
    }

    /**
     * Asserts that the round did not fail but fell back to its own data commit, made on top of
     * {@code parentSnapshotId}, as it does whenever the rewrite is not known to be committed.
     */
    private static void assertFallsBackToDataCommit(
            Table table, LakeCommitResult result, long parentSnapshotId) {
        assertThat(result.committedIsReadable()).isTrue();
        Snapshot dataCommit = table.snapshot(result.getCommittedSnapshotId());
        assertThat(dataCommit.operation()).isEqualTo(DataOperations.APPEND);
        assertThat(dataCommit.parentId()).isEqualTo(parentSnapshotId);
    }

    private CommitFailureInjectingCatalog newFailingCatalog(
            RuntimeException failure, boolean applyCommitBeforeFailing) {
        CommitFailureInjectingCatalog catalog =
                new CommitFailureInjectingCatalog(failure, applyCommitBeforeFailing);
        catalog.setConf(new org.apache.hadoop.conf.Configuration());
        catalog.initialize("test", configuration.toMap());
        return catalog;
    }

    private Table createTableWithSmallFiles() throws Exception {
        SupportsNamespaces namespaces = (SupportsNamespaces) icebergCatalog;
        Namespace namespace = Namespace.of(TABLE_PATH.getDatabaseName());
        if (!namespaces.namespaceExists(namespace)) {
            namespaces.createNamespace(namespace);
        }
        Schema schema =
                new Schema(
                        Types.NestedField.optional(1, "c1", Types.IntegerType.get()),
                        Types.NestedField.required(2, BUCKET_COLUMN_NAME, Types.IntegerType.get()),
                        Types.NestedField.required(3, OFFSET_COLUMN_NAME, Types.LongType.get()),
                        Types.NestedField.required(
                                4, TIMESTAMP_COLUMN_NAME, Types.TimestampType.withZone()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity(BUCKET_COLUMN_NAME).build();
        // No commit retries: an injected CommitFailedException is final.
        Table table =
                icebergCatalog.createTable(
                        toIceberg(TABLE_PATH),
                        schema,
                        spec,
                        Collections.singletonMap(TableProperties.COMMIT_NUM_RETRIES, "0"));
        // Three small files in one bucket are enough for IcebergRewriteDataFiles to compact.
        AppendFiles append = table.newAppend();
        for (int i = 0; i < 3; i++) {
            append.appendFile(writeDataFile(table, i * 10L));
        }
        append.commit();
        table.refresh();
        return table;
    }

    private static RewriteDataFileResult planRewrite(Table table) {
        RewriteDataFileResult rewrite =
                new IcebergRewriteDataFiles(table, null, new TableBucket(0, BUCKET)).execute();
        assertThat(rewrite).isNotNull();
        return rewrite;
    }

    private static DataFile writeDataFile(Table table, long startOffset) throws Exception {
        try (TaskWriter<Record> writer = TaskWriterFactory.createTaskWriter(table, null, BUCKET)) {
            for (int i = 0; i < 3; i++) {
                Record record = GenericRecord.create(table.schema());
                record.setField("c1", i);
                record.setField(BUCKET_COLUMN_NAME, BUCKET);
                record.setField(OFFSET_COLUMN_NAME, startOffset + i);
                record.setField(TIMESTAMP_COLUMN_NAME, OffsetDateTime.now(ZoneOffset.UTC));
                writer.write(record);
            }
            DataFile[] files = writer.complete().dataFiles();
            assertThat(files).hasSize(1);
            return files[0];
        }
    }

    private static Set<String> locationsOf(Iterable<DataFile> files) {
        Set<String> locations = new HashSet<>();
        files.forEach(file -> locations.add(file.location()));
        return locations;
    }

    private static Set<String> currentDataFiles(Table table) throws IOException {
        Set<String> locations = new HashSet<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            tasks.forEach(task -> locations.add(task.file().location()));
        }
        return locations;
    }

    private static boolean exists(Table table, String location) {
        return table.io().newInputFile(location).exists();
    }

    /** Reads every row, so it fails if the current snapshot references a missing data file. */
    private static long countRows(Table table) throws IOException {
        long count = 0;
        try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
            for (Record ignored : records) {
                count++;
            }
        }
        return count;
    }

    /**
     * A Hadoop catalog whose tables fail every rewrite (replace) commit with the given exception,
     * optionally after the commit was applied, as when the catalog commits but the client never
     * sees the response. Other commits succeed.
     */
    private static class CommitFailureInjectingCatalog extends HadoopCatalog {

        private final RuntimeException failure;
        private final boolean applyCommitBeforeFailing;

        CommitFailureInjectingCatalog(RuntimeException failure, boolean applyCommitBeforeFailing) {
            this.failure = failure;
            this.applyCommitBeforeFailing = applyCommitBeforeFailing;
        }

        @Override
        protected TableOperations newTableOps(TableIdentifier identifier) {
            return new FailingCommitTableOperations(super.newTableOps(identifier));
        }

        private class FailingCommitTableOperations implements TableOperations {

            private final TableOperations delegate;

            FailingCommitTableOperations(TableOperations delegate) {
                this.delegate = delegate;
            }

            @Override
            public void commit(TableMetadata base, TableMetadata metadata) {
                Snapshot snapshot = metadata.currentSnapshot();
                if (snapshot == null || !DataOperations.REPLACE.equals(snapshot.operation())) {
                    delegate.commit(base, metadata);
                    return;
                }
                if (applyCommitBeforeFailing) {
                    delegate.commit(base, metadata);
                }
                throw failure;
            }

            @Override
            public TableMetadata current() {
                return delegate.current();
            }

            @Override
            public TableMetadata refresh() {
                return delegate.refresh();
            }

            @Override
            public FileIO io() {
                return delegate.io();
            }

            @Override
            public EncryptionManager encryption() {
                return delegate.encryption();
            }

            @Override
            public String metadataFileLocation(String fileName) {
                return delegate.metadataFileLocation(fileName);
            }

            @Override
            public LocationProvider locationProvider() {
                return delegate.locationProvider();
            }

            @Override
            public TableOperations temp(TableMetadata uncommittedMetadata) {
                return delegate.temp(uncommittedMetadata);
            }

            @Override
            public long newSnapshotId() {
                return delegate.newSnapshotId();
            }

            @Override
            public boolean requireStrictCleanup() {
                return delegate.requireStrictCleanup();
            }
        }
    }
}
