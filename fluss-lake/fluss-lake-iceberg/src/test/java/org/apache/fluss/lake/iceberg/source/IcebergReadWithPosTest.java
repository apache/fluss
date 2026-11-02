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

package org.apache.fluss.lake.iceberg.source;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.iceberg.IcebergLakeCatalog;
import org.apache.fluss.lake.iceberg.maintenance.IcebergRewriteDataFiles;
import org.apache.fluss.lake.iceberg.maintenance.RewriteDataFileResult;
import org.apache.fluss.lake.iceberg.tiering.IcebergCatalogProvider;
import org.apache.fluss.lake.iceberg.tiering.IcebergLakeCommitter;
import org.apache.fluss.lake.iceberg.tiering.IcebergLakeTieringFactory;
import org.apache.fluss.lake.iceberg.tiering.IcebergWriteResult;
import org.apache.fluss.lake.iceberg.tiering.writer.IcebergDvFileWriter;
import org.apache.fluss.lake.lakestorage.TestingLakeCatalogContext;
import org.apache.fluss.lake.source.DataDeltaPlan;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.lake.source.RowWithPosResult;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.lake.writer.WriterInitContext;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.GenericRecord;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.CloseableIterator;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DeleteWriteResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;
import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests {@code IcebergRecordReader.readWithPos()}, {@code IcebergSplit.fileName()}, planDelta. */
class IcebergReadWithPosTest {

    private static final int BUCKET_NUM = 1;
    // lake schema = c1, c2, __bucket, __offset, __timestamp, __rowid -> __rowid at index 5.
    private static final int ROWID_COLUMN_INDEX = 5;

    @TempDir private File tempWarehouseDir;

    private Configuration configuration;
    private IcebergLakeTieringFactory tieringFactory;
    private IcebergCatalogProvider catalogProvider;

    @BeforeEach
    void beforeEach() {
        configuration = new Configuration();
        configuration.setString("warehouse", "file://" + tempWarehouseDir);
        configuration.setString("type", "hadoop");
        configuration.setString("name", "test");
        tieringFactory = new IcebergLakeTieringFactory(configuration);
        catalogProvider = new IcebergCatalogProvider(configuration);
    }

    @Test
    void readWithPosYieldsRowIdAndPositionAndFileName() throws Exception {
        TablePath tablePath = createDvTableAndWrite("dv_readpos_table");

        IcebergLakeSource source = new IcebergLakeSource(configuration, tablePath);
        DataDeltaPlan<IcebergSplit> plan = source.planDelta(-1L);
        assertThat(plan).isNotNull();
        assertThat(plan.getSplits()).hasSize(1);
        IcebergSplit split = plan.getSplits().get(0);
        assertThat(split.fileName()).isNotNull().endsWith(".parquet");

        source.withProject(new int[][] {{ROWID_COLUMN_INDEX}});
        RecordReader reader = source.createRecordReader(() -> split);
        List<long[]> rowIdAndPos = collectRowIdAndPos(reader);

        // fresh append, no DV -> positions 0,1,2 for rowIds 0,1,2.
        assertThat(rowIdAndPos)
                .containsExactly(new long[] {0, 0}, new long[] {1, 1}, new long[] {2, 2});
    }

    @Test
    void readWithPosReportsOriginalPositionsWithDeletionVectorApplied() throws Exception {
        TablePath tablePath = createDvTableAndWrite("dv_readpos_dv_table");

        Table table = catalogProvider.get().loadTable(toIceberg(tablePath));
        table.refresh();
        long baseSnapshotId = table.currentSnapshot().snapshotId();

        // delete position 1 via a Puffin DV.
        DataFile dataFile = table.currentSnapshot().addedDataFiles(table.io()).iterator().next();
        List<DeleteFile> dvFiles;
        try (IcebergDvFileWriter dvWriter = new IcebergDvFileWriter(table, 0)) {
            dvWriter.delete(dataFile, 1L);
            DeleteWriteResult result = dvWriter.complete();
            dvFiles = result.deleteFiles();
        }
        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(catalogProvider, tablePath)) {
            committer.commitDeletionVectors(
                    dvFiles,
                    Collections.singletonList(dataFile.location()),
                    baseSnapshotId,
                    Collections.singletonMap("round", "2"));
        }

        // plan a split at the current snapshot so the FileScanTask carries the DV.
        table.refresh();
        IcebergSplit split;
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            split = new IcebergSplit(tasks.iterator().next(), 0, Collections.emptyList());
        }

        IcebergLakeSource source = new IcebergLakeSource(configuration, tablePath);
        source.withProject(new int[][] {{ROWID_COLUMN_INDEX}});
        RecordReader reader = source.createRecordReader(() -> split);
        List<long[]> rowIdAndPos = collectRowIdAndPos(reader);

        // position 1 is masked by the DV; survivors keep their ORIGINAL positions 0 and 2.
        assertThat(rowIdAndPos).containsExactly(new long[] {0, 0}, new long[] {2, 2});
    }

    @Test
    void materializeDeletionVectorsMasksRowsAndSupersedesAcrossRounds() throws Exception {
        TablePath tablePath = createDvTableAndWrite("dv_materialize_table");
        Table table = catalogProvider.get().loadTable(toIceberg(tablePath));
        table.refresh();
        long baseSnapshotId = table.currentSnapshot().snapshotId();
        String dataFile =
                table.currentSnapshot().addedDataFiles(table.io()).iterator().next().location();

        // Round 1: materialize a DV masking position 0 (rowId 0).
        materialize(tablePath, dataFile, baseSnapshotId, 0L);
        assertThat(readSurvivingRowIds(tablePath)).containsExactly(1L, 2L);

        // Round 2: cumulative DV masking positions 0 and 1 — must SUPERSEDE the round-1 DV
        // (Iceberg v3 allows one DV per data file), not accumulate a second one.
        materialize(tablePath, dataFile, baseSnapshotId, 0L, 1L);
        assertThat(readSurvivingRowIds(tablePath)).containsExactly(2L);
    }

    @Test
    void materializeTagsReadableSnapshotSoExpirationPreservesIt() throws Exception {
        TablePath tablePath = createDvTableAndWrite("dv_expire_protect_table");
        Table table = catalogProvider.get().loadTable(toIceberg(tablePath));
        table.refresh();
        long readableSnapshotId = table.currentSnapshot().snapshotId();
        String dataFile =
                table.currentSnapshot().addedDataFiles(table.io()).iterator().next().location();

        // Materialize a DV -> commits a new (DV) snapshot AND pins the readable snapshot with a
        // tag.
        materialize(tablePath, dataFile, readableSnapshotId, 0L);

        table.refresh();
        // the readable snapshot is now an ancestor (materialize advanced current) but is
        // tag-pinned.
        assertThat(table.currentSnapshot().snapshotId()).isNotEqualTo(readableSnapshotId);
        assertThat(table.refs()).containsKey("fluss-dv-readable-snapshot");
        assertThat(table.refs().get("fluss-dv-readable-snapshot").snapshotId())
                .isEqualTo(readableSnapshotId);

        // An external expiration of everything old must PRESERVE the tag-pinned readable snapshot.
        table.expireSnapshots().expireOlderThan(Long.MAX_VALUE).commit();
        table.refresh();
        assertThat(table.snapshot(readableSnapshotId)).isNotNull();
    }

    private void materialize(
            TablePath tablePath, String dataFile, long baseSnapshotId, long... positions)
            throws Exception {
        Roaring64Bitmap bitmap = new Roaring64Bitmap();
        for (long p : positions) {
            bitmap.addLong(p);
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        bitmap.serialize(new DataOutputStream(out));
        // LakeDv keys are data-file basenames (as the server reports them).
        String key = dataFile.substring(dataFile.lastIndexOf('/') + 1);
        Map<String, byte[]> lakeDv = Collections.singletonMap(key, out.toByteArray());
        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(catalogProvider, tablePath)) {
            committer.materializeDeletionVectors(
                    lakeDv, baseSnapshotId, Collections.singletonMap("round", "m"));
        }
    }

    private List<Long> readSurvivingRowIds(TablePath tablePath) throws Exception {
        Table table = catalogProvider.get().loadTable(toIceberg(tablePath));
        table.refresh();
        IcebergSplit split;
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            split = new IcebergSplit(tasks.iterator().next(), 0, Collections.emptyList());
        }
        IcebergLakeSource source = new IcebergLakeSource(configuration, tablePath);
        source.withProject(new int[][] {{ROWID_COLUMN_INDEX}});
        RecordReader reader = source.createRecordReader(() -> split);
        List<Long> rowIds = new ArrayList<>();
        try (CloseableIterator<RowWithPosResult> it = reader.readWithPos()) {
            while (it.hasNext()) {
                rowIds.add(it.next().getRow().getLong(0));
            }
        }
        return rowIds;
    }

    @Test
    void planDeltaAfterDvMaterializationDoesNotBreak() throws Exception {
        TablePath tablePath = createDvTableAndWrite("dv_pd_probe");
        Table table = catalogProvider.get().loadTable(toIceberg(tablePath));
        table.refresh();
        long base = table.currentSnapshot().snapshotId();
        String dataFile =
                table.currentSnapshot().addedDataFiles(table.io()).iterator().next().location();
        materialize(tablePath, dataFile, base, 0L);
        // After a DV snapshot enters the lineage, planDelta (used by the RowPos scan) must still
        // find the latest append's files, not return null (the DV snapshot is skipped).
        IcebergLakeSource source = new IcebergLakeSource(configuration, tablePath);
        DataDeltaPlan<IcebergSplit> plan = source.planDelta(-1L);
        assertThat(plan).isNotNull();
        assertThat(plan.getSplits()).hasSize(1);
    }

    @Test
    void planDeltaOnCompactionReportsRemovedFilesAndReindexesCompactedFiles() throws Exception {
        // Compaction needs >= MIN_FILES_TO_COMPACT (3) files, so make three data files in bucket 0:
        // round 1 (via helper) + two more append rounds.
        TablePath tablePath = createDvTableAndWrite("dv_compaction_table");
        TableInfo tableInfo = dvTableInfo(tablePath);
        appendRound(tablePath, tableInfo, "2", insert(3, 4, "d"), insert(4, 5, "e"));
        appendRound(tablePath, tableInfo, "3", insert(5, 6, "f"), insert(6, 7, "g"));

        Table table = catalogProvider.get().loadTable(toIceberg(tablePath));
        table.refresh();
        long appendSnapshotId = table.currentSnapshot().snapshotId();

        // capture the three pre-compaction data file basenames.
        Set<String> oldBasenames = new HashSet<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask t : tasks) {
                String loc = t.file().location();
                oldBasenames.add(loc.substring(loc.lastIndexOf('/') + 1));
            }
        }
        assertThat(oldBasenames).hasSize(3);

        // compact bucket 0's two files into one -> a REPLACE snapshot.
        RewriteDataFileResult rr =
                new IcebergRewriteDataFiles(table, null, new TableBucket(0L, 0))
                        .targetSizeInBytes(Long.MAX_VALUE)
                        .execute();
        assertThat(rr).isNotNull();
        table.newRewrite()
                .rewriteFiles(
                        new HashSet<>(rr.deletedDataFiles()), new HashSet<>(rr.addedDataFiles()))
                .commit();
        table.refresh();
        long compactSnapshotId = table.currentSnapshot().snapshotId();

        // planDelta from the append snapshot surfaces the compaction (Paimon model).
        IcebergLakeSource source = new IcebergLakeSource(configuration, tablePath);
        DataDeltaPlan<IcebergSplit> plan = source.planDelta(appendSnapshotId);
        assertThat(plan).isNotNull();
        // returns the compaction snapshot id (not "current"/append).
        assertThat(plan.getCompactSnapshotId()).isEqualTo(compactSnapshotId);
        // removed files reported as deletedFiles under bucket 0, except from's own append file:
        // it was never indexed (indexed one round late), so there is nothing to clean up
        String fromFile =
                table.snapshot(appendSnapshotId)
                        .addedDataFiles(table.io())
                        .iterator()
                        .next()
                        .location();
        oldBasenames.remove(fromFile.substring(fromFile.lastIndexOf('/') + 1));
        assertThat(plan.getDeletedFiles(null, 0))
                .hasSize(2)
                .containsExactlyInAnyOrderElementsOf(oldBasenames);
        // compacted output files re-indexed as splits.
        assertThat(plan.getSplits()).hasSize(rr.addedDataFiles().size());
    }

    @Test
    void compactionSnapshotIsNotMistakenForLatestTieredDataSnapshot() throws Exception {
        // 3 append rounds -> 3 data files under the tiering user, then a compaction.
        TablePath tablePath = createDvTableAndWrite("dv_compaction_reconcile_table");
        TableInfo tableInfo = dvTableInfo(tablePath);
        appendRound(tablePath, tableInfo, "2", insert(3, 4, "d"), insert(4, 5, "e"));
        appendRound(tablePath, tableInfo, "3", insert(5, 6, "f"), insert(6, 7, "g"));

        Table table = catalogProvider.get().loadTable(toIceberg(tablePath));
        table.refresh();
        long latestDataSnapshotId = table.currentSnapshot().snapshotId();

        // Compact bucket 0 and commit the REPLACE the way IcebergLakeCommitter.commitRewrite now
        // does: under the distinct compaction committer user (not the tiering user).
        RewriteDataFileResult rr =
                new IcebergRewriteDataFiles(table, null, new TableBucket(0L, 0))
                        .targetSizeInBytes(Long.MAX_VALUE)
                        .execute();
        assertThat(rr).isNotNull();
        table.newRewrite()
                .rewriteFiles(
                        new HashSet<>(rr.deletedDataFiles()), new HashSet<>(rr.addedDataFiles()))
                .set("commit-user", "fluss-lake-compaction")
                .commit();
        table.refresh();
        // the current snapshot is the compaction (REPLACE).
        assertThat(table.currentSnapshot().operation()).isEqualTo(DataOperations.REPLACE);

        // The restart reconcile must NOT flag a missing snapshot: the compaction is committed under
        // a
        // distinct user, so the latest tiered DATA snapshot is still latestDataSnapshotId (which
        // Fluss already knows). Without the distinct user, the compaction would be returned here.
        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(catalogProvider, tablePath)) {
            assertThat(committer.getMissingLakeSnapshot(latestDataSnapshotId)).isNull();
        }
    }

    private TableInfo dvTableInfo(TablePath tablePath) {
        Schema flussSchema =
                Schema.newBuilder()
                        .column("c1", DataTypes.INT())
                        .column("c2", DataTypes.STRING())
                        .primaryKey("c1")
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(flussSchema)
                        .distributedBy(BUCKET_NUM, "c1")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .property(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED, true)
                        .build();
        return TableInfo.of(tablePath, 0, 1, descriptor, DEFAULT_REMOTE_DATA_DIR, 1L, 1L);
    }

    private void appendRound(
            TablePath tablePath, TableInfo tableInfo, String round, LogRecord... records)
            throws Exception {
        List<IcebergWriteResult> results = new ArrayList<>();
        try (LakeWriter<IcebergWriteResult> writer = createDvWriter(tablePath, tableInfo)) {
            for (LogRecord record : records) {
                writer.write(record);
            }
            results.add(writer.complete());
        }
        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(catalogProvider, tablePath)) {
            committer.commit(
                    committer.toCommittable(results), Collections.singletonMap("round", round));
        }
    }

    private TablePath createDvTableAndWrite(String tableName) throws Exception {
        TablePath tablePath = TablePath.of("iceberg", tableName);
        Schema flussSchema =
                Schema.newBuilder()
                        .column("c1", DataTypes.INT())
                        .column("c2", DataTypes.STRING())
                        .primaryKey("c1")
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(flussSchema)
                        .distributedBy(BUCKET_NUM, "c1")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .property(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED, true)
                        .build();
        try (IcebergLakeCatalog catalog = new IcebergLakeCatalog(configuration)) {
            catalog.createTable(tablePath, descriptor, new TestingLakeCatalogContext());
        }
        TableInfo tableInfo =
                TableInfo.of(tablePath, 0, 1, descriptor, DEFAULT_REMOTE_DATA_DIR, 1L, 1L);

        List<IcebergWriteResult> writeResults = new ArrayList<>();
        try (LakeWriter<IcebergWriteResult> writer = createDvWriter(tablePath, tableInfo)) {
            writer.write(insert(0, 1, "a"));
            writer.write(insert(1, 2, "b"));
            writer.write(insert(2, 3, "c"));
            writeResults.add(writer.complete());
        }
        try (IcebergLakeCommitter committer =
                new IcebergLakeCommitter(catalogProvider, tablePath)) {
            committer.commit(
                    committer.toCommittable(writeResults), Collections.singletonMap("round", "1"));
        }
        return tablePath;
    }

    private static List<long[]> collectRowIdAndPos(RecordReader reader) throws Exception {
        List<long[]> out = new ArrayList<>();
        try (CloseableIterator<RowWithPosResult> it = reader.readWithPos()) {
            while (it.hasNext()) {
                RowWithPosResult rwp = it.next();
                out.add(new long[] {rwp.getRow().getLong(0), rwp.getPos()});
            }
        }
        return out;
    }

    private LakeWriter<IcebergWriteResult> createDvWriter(TablePath tablePath, TableInfo tableInfo)
            throws Exception {
        return tieringFactory.createLakeWriter(
                new WriterInitContext() {
                    @Override
                    public TablePath tablePath() {
                        return tablePath;
                    }

                    @Override
                    public TableBucket tableBucket() {
                        return new TableBucket(0, 0);
                    }

                    @Nullable
                    @Override
                    public String partition() {
                        return null;
                    }

                    @Override
                    public TableInfo tableInfo() {
                        return tableInfo;
                    }
                });
    }

    private static LogRecord insert(long offset, int c1, String c2) {
        GenericRow row = new GenericRow(2);
        row.setField(0, c1);
        row.setField(1, BinaryString.fromString(c2));
        return new GenericRecord(offset, 1_000_000_000L + offset, ChangeType.INSERT, row);
    }
}
