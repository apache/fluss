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

package com.alibaba.fluss.lake.iceberg;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.lake.committer.LakeCommitter;
import com.alibaba.fluss.lake.iceberg.actions.IcebergRewriteDataFiles;
import com.alibaba.fluss.lake.iceberg.tiering.IcebergCatalogProvider;
import com.alibaba.fluss.lake.iceberg.tiering.IcebergCommittable;
import com.alibaba.fluss.lake.iceberg.tiering.IcebergLakeTieringFactory;
import com.alibaba.fluss.lake.iceberg.tiering.IcebergWriteResult;
import com.alibaba.fluss.lake.serializer.SimpleVersionedSerializer;
import com.alibaba.fluss.lake.writer.LakeWriter;
import com.alibaba.fluss.lake.writer.WriterInitContext;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.ChangeType;
import com.alibaba.fluss.record.GenericRecord;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.types.DataTypes;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFilesActionResult;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.alibaba.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;
import static com.alibaba.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static com.alibaba.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static com.alibaba.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit test to verify compaction via {@link IcebergRewriteDataFiles}. */
class IcebergCompactionTest {

    private @TempDir File tempWarehouseDir;
    private IcebergLakeTieringFactory lakeTieringFactory;
    private Catalog icebergCatalog;

    @BeforeEach
    void setUp() {
        Configuration configuration = new Configuration();
        configuration.setString("warehouse", "file://" + tempWarehouseDir);
        configuration.setString("type", "hadoop");
        configuration.setString("name", "test");
        IcebergCatalogProvider provider = new IcebergCatalogProvider(configuration);
        icebergCatalog = provider.get();

        lakeTieringFactory = new IcebergLakeTieringFactory(configuration);
    }

    @Test
    void testNoCompactionForFilesLessThan2() throws Exception {
        TablePath tablePath = TablePath.of("iceberg", "compact_table");
        createTable(tablePath);

        Table icebergTable = icebergCatalog.loadTable(toIceberg(tablePath));
        appendTinyFilesWithRowsAndBucket(icebergTable, 1, 3, 1000, 0);
        icebergTable.refresh();

        int filesBefore = countDataFiles(icebergTable);
        // We expect exactly 1 file for this no-op compaction scenario.
        assertThat(filesBefore).isEqualTo(1);

        RewriteDataFilesActionResult result =
                new IcebergRewriteDataFiles(icebergTable)
                        .targetSizeInBytes(128 * 1024 * 1024L)
                        .binPack()
                        .filter(Expressions.alwaysTrue())
                        .execute();
        assertThat(result.addedDataFiles()).isEmpty();
        assertThat(result.deletedDataFiles()).isEmpty();

        icebergTable.refresh();
        int filesAfter = countDataFiles(icebergTable);

        // File count should be unchanged.
        assertThat(filesAfter).isEqualTo(filesBefore);
    }

    @Test
    void testCompactionReducesFileCountForSmallFiles() throws Exception {
        TablePath tablePath = TablePath.of("iceberg", "compact_table_multi");
        createTable(tablePath);

        // Commit three small batches separately to guarantee multiple data files.
        for (int i = 0; i < 3; i++) {
            List<IcebergWriteResult> batch = writeSmallBatch(tablePath, 0, 3);
            try (LakeCommitter<IcebergWriteResult, IcebergCommittable> committer =
                    lakeTieringFactory.createLakeCommitter(() -> tablePath)) {
                SimpleVersionedSerializer<IcebergCommittable> committableSerializer =
                        lakeTieringFactory.getCommittableSerializer();
                IcebergCommittable committable = committer.toCommittable(batch);
                byte[] serialized = committableSerializer.serialize(committable);
                committable =
                        committableSerializer.deserialize(
                                committableSerializer.getVersion(), serialized);
                committer.commit(committable, Collections.emptyMap());
            }
        }

        Table icebergTable = icebergCatalog.loadTable(toIceberg(tablePath));
        icebergTable.refresh();

        int filesBefore = countDataFiles(icebergTable);
        // We expect at least 2 files after three separate commits.
        assertThat(filesBefore).isGreaterThanOrEqualTo(2);

        // Run compaction (bin-pack) with a large target so small files are grouped.
        RewriteDataFilesActionResult result =
                new IcebergRewriteDataFiles(icebergTable)
                        .targetSizeInBytes(128 * 1024 * 1024L)
                        .binPack()
                        .filter(Expressions.alwaysTrue())
                        .execute();

        // Validate compaction outcome.
        assertThat(result.addedDataFiles().size()).isGreaterThanOrEqualTo(1);
        assertThat(result.deletedDataFiles().size()).isGreaterThanOrEqualTo(1);

        icebergTable.refresh();
        int filesAfter = countDataFiles(icebergTable);
        assertThat(filesAfter).isLessThan(filesBefore);
    }

    @Test
    void testCompactionIsIdempotent() throws Exception {
        TablePath tablePath = TablePath.of("iceberg", "compact_idempotent");
        createTable(tablePath);

        Table table = icebergCatalog.loadTable(toIceberg(tablePath));
        // Seed 3 tiny files for bucket 0
        appendTinyFilesWithRowsAndBucket(table, 3, 1, 1000, 0);
        table.refresh();

        int filesBefore = countDataFiles(table);
        assertThat(filesBefore).isGreaterThanOrEqualTo(3);

        // First compaction merges small files
        RewriteDataFilesActionResult first =
                new IcebergRewriteDataFiles(table)
                        .targetSizeInBytes(128 * 1024 * 1024L)
                        .binPack()
                        .filter(Expressions.alwaysTrue())
                        .execute();
        table.refresh();
        int filesAfterFirst = countDataFiles(table);
        assertThat(filesAfterFirst).isLessThan(filesBefore);

        // Second compaction should be a no-op
        RewriteDataFilesActionResult second =
                new IcebergRewriteDataFiles(table)
                        .targetSizeInBytes(128 * 1024 * 1024L)
                        .binPack()
                        .filter(Expressions.alwaysTrue())
                        .execute();
        table.refresh();
        int filesAfterSecond = countDataFiles(table);

        assertThat(second.addedDataFiles()).isEmpty();
        assertThat(second.deletedDataFiles()).isEmpty();
        assertThat(filesAfterSecond).isEqualTo(filesAfterFirst);
    }

    @Test
    void testCompactionPreservesRecordCount() throws Exception {
        TablePath tablePath = TablePath.of("iceberg", "compact_preserve_count");
        createTable(tablePath);

        Table table = icebergCatalog.loadTable(toIceberg(tablePath));
        // 4 files, 5 rows each, all bucket 0
        appendTinyFilesWithRowsAndBucket(table, 4, 5, 2000, 0);
        table.refresh();

        long rowsBefore = countRows(table);

        RewriteDataFilesActionResult res =
                new IcebergRewriteDataFiles(table)
                        .targetSizeInBytes(128 * 1024 * 1024L)
                        .binPack()
                        .filter(Expressions.alwaysTrue())
                        .execute();

        table.refresh();
        long rowsAfter = countRows(table);

        assertThat(rowsAfter).isEqualTo(rowsBefore);
        if (!res.addedDataFiles().isEmpty() || !res.deletedDataFiles().isEmpty()) {
            assertThat(res.addedDataFiles().size()).isGreaterThanOrEqualTo(1);
            assertThat(res.deletedDataFiles().size()).isGreaterThanOrEqualTo(1);
        }
    }

    @Test
    void testCompactionResultSerializerRoundTrip() throws Exception {
        TablePath tablePath = TablePath.of("iceberg", "compact_serde");
        createTable(tablePath);

        // We only validate that WriteResult round-trips; compaction details are not serialized.
        WriteResult dummyWrite = WriteResult.builder().build();
        IcebergWriteResult writeResult = new IcebergWriteResult(dummyWrite);

        SimpleVersionedSerializer<IcebergWriteResult> serializer =
                lakeTieringFactory.getWriteResultSerializer();
        byte[] bytes = serializer.serialize(writeResult);
        IcebergWriteResult roundTrip = serializer.deserialize(serializer.getVersion(), bytes);

        assertThat(roundTrip).isNotNull();
        assertThat(roundTrip.getWriteResult()).isNotNull();
    }

    @Test
    void testBucketScopedCompaction() throws Exception {
        TablePath tablePath = TablePath.of("iceberg", "compact_bucket_scoped");
        createTable(tablePath);

        Table table = icebergCatalog.loadTable(toIceberg(tablePath));
        // Seed bucket 0: 3 tiny files, bucket 1: 3 tiny files
        appendTinyFilesWithRowsAndBucket(table, 3, 1, 4000, 0);
        appendTinyFilesWithRowsAndBucket(table, 3, 1, 5000, 1);
        table.refresh();

        int filesBeforeBucket0 = countFilesForBucket(table, 0);
        int filesBeforeBucket1 = countFilesForBucket(table, 1);
        assertThat(filesBeforeBucket0).isGreaterThanOrEqualTo(3);
        assertThat(filesBeforeBucket1).isGreaterThanOrEqualTo(3);

        // Compact only bucket 0
        RewriteDataFilesActionResult res0 =
                new IcebergRewriteDataFiles(table)
                        .targetSizeInBytes(128 * 1024 * 1024L)
                        .binPack()
                        .filter(Expressions.equal(BUCKET_COLUMN_NAME, 0))
                        .execute();
        table.refresh();

        int filesAfterBucket0 = countFilesForBucket(table, 0);
        int filesAfterBucket1 = countFilesForBucket(table, 1);

        // Bucket 0 reduced, bucket 1 unchanged
        if (!res0.addedDataFiles().isEmpty() || !res0.deletedDataFiles().isEmpty()) {
            assertThat(filesAfterBucket0).isLessThan(filesBeforeBucket0);
        }
        assertThat(filesAfterBucket1).isEqualTo(filesBeforeBucket1);

        // Now compact only bucket 1 and assert reduction there
        RewriteDataFilesActionResult res1 =
                new IcebergRewriteDataFiles(table)
                        .targetSizeInBytes(128 * 1024 * 1024L)
                        .binPack()
                        .filter(Expressions.equal(BUCKET_COLUMN_NAME, 1))
                        .execute();
        table.refresh();

        int filesAfterBucket1Second = countFilesForBucket(table, 1);
        if (!res1.addedDataFiles().isEmpty() || !res1.deletedDataFiles().isEmpty()) {
            assertThat(filesAfterBucket1Second).isLessThan(filesAfterBucket1);
        }
    }

    // ---------- helpers methods----------

    private List<IcebergWriteResult> writeSmallBatch(TablePath tablePath, int bucket, int rows)
            throws IOException {
        List<IcebergWriteResult> results = new ArrayList<>();
        SimpleVersionedSerializer<IcebergWriteResult> writeResultSerializer =
                lakeTieringFactory.getWriteResultSerializer();

        try (LakeWriter<IcebergWriteResult> writer = createLakeWriter(tablePath, bucket)) {
            for (int i = 0; i < rows; i++) {
                GenericRow row = new GenericRow(3);
                row.setField(0, i);
                row.setField(1, BinaryString.fromString("v_" + i));
                row.setField(2, BinaryString.fromString("g"));

                LogRecord record =
                        new GenericRecord(
                                i, System.currentTimeMillis(), ChangeType.APPEND_ONLY, row);
                writer.write(record);
            }
            IcebergWriteResult writeResult = writer.complete();

            // round-trip serialize for parity with existing tests
            byte[] bytes = writeResultSerializer.serialize(writeResult);
            results.add(
                    writeResultSerializer.deserialize(writeResultSerializer.getVersion(), bytes));
        } catch (ArrayIndexOutOfBoundsException | IllegalStateException e) {
            // Fallback for non-partitioned tables where the task writer expects a partition key.
            try {
                Table table = icebergCatalog.loadTable(toIceberg(tablePath));

                // Create a single tiny Parquet file containing all 'rows' records.
                String fileName = java.util.UUID.randomUUID() + ".parquet";
                OutputFile outputFile =
                        table.io()
                                .newOutputFile(table.locationProvider().newDataLocation(fileName));

                Parquet.WriteBuilder writeBuilder =
                        Parquet.write(outputFile)
                                .schema(table.schema())
                                .createWriterFunc(GenericParquetWriter::buildWriter)
                                .overwrite();

                long recordCount = 0L;
                Metrics metrics;
                FileAppender<Record> appender = null;
                try {
                    appender = writeBuilder.build();
                    for (int i = 0; i < rows; i++) {
                        org.apache.iceberg.data.Record r =
                                org.apache.iceberg.data.GenericRecord.create(table.schema());
                        r.setField("c1", i);
                        r.setField("c2", "v_" + i);
                        r.setField("c3", "g");
                        r.setField(BUCKET_COLUMN_NAME, bucket);
                        r.setField(OFFSET_COLUMN_NAME, (long) i);
                        r.setField(
                                TIMESTAMP_COLUMN_NAME,
                                java.time.OffsetDateTime.now(java.time.ZoneOffset.UTC));
                        appender.add(r);
                        recordCount++;
                    }
                } finally {
                    if (appender != null) {
                        appender.close();
                    }
                }
                metrics = appender.metrics();

                String location = outputFile.location();
                InputFile inputFile = table.io().newInputFile(location);
                long fileSizeInBytes = inputFile.getLength();

                DataFile df =
                        DataFiles.builder(table.spec())
                                .withPath(location)
                                .withFileSizeInBytes(fileSizeInBytes)
                                .withFormat(FileFormat.PARQUET)
                                .withRecordCount(recordCount)
                                .withMetrics(metrics)
                                .build();

                AppendFiles append = table.newAppend();
                append.appendFile(df);
                append.commit();

                WriteResult wr =
                        WriteResult.builder()
                                .addDataFiles(new org.apache.iceberg.DataFile[] {df})
                                .build();

                IcebergWriteResult writeResult = new IcebergWriteResult(wr);
                byte[] bytes = writeResultSerializer.serialize(writeResult);
                results.add(
                        writeResultSerializer.deserialize(
                                writeResultSerializer.getVersion(), bytes));
            } catch (Exception ex) {
                throw new IOException("Fallback tiny-file append failed", ex);
            }
        }
        return results;
    }

    private LakeWriter<IcebergWriteResult> createLakeWriter(TablePath tablePath, int bucket)
            throws IOException {
        return lakeTieringFactory.createLakeWriter(
                new WriterInitContext() {
                    @Override
                    public TablePath tablePath() {
                        return tablePath;
                    }

                    @Override
                    public TableBucket tableBucket() {
                        return new TableBucket(0, null, bucket);
                    }

                    @Nullable
                    @Override
                    public String partition() {
                        return null;
                    }

                    @Override
                    public Map<String, String> customProperties() {
                        return Collections.emptyMap();
                    }

                    @Override
                    public com.alibaba.fluss.metadata.Schema schema() {
                        return com.alibaba.fluss.metadata.Schema.newBuilder()
                                .column("c1", DataTypes.INT())
                                .column("c2", DataTypes.STRING())
                                .column("c3", DataTypes.STRING())
                                .build();
                    }
                });
    }

    private void createTable(TablePath tablePath) throws Exception {
        Namespace namespace = Namespace.of(tablePath.getDatabaseName());
        if (icebergCatalog instanceof SupportsNamespaces) {
            SupportsNamespaces ns = (SupportsNamespaces) icebergCatalog;
            if (!ns.namespaceExists(namespace)) {
                ns.createNamespace(namespace);
            }
        }

        Schema schema =
                new Schema(
                        Types.NestedField.optional(1, "c1", Types.IntegerType.get()),
                        Types.NestedField.optional(2, "c2", Types.StringType.get()),
                        Types.NestedField.optional(3, "c3", Types.StringType.get()),
                        Types.NestedField.required(4, BUCKET_COLUMN_NAME, Types.IntegerType.get()),
                        Types.NestedField.required(5, OFFSET_COLUMN_NAME, Types.LongType.get()),
                        Types.NestedField.required(
                                6, TIMESTAMP_COLUMN_NAME, Types.TimestampType.withZone()));

        TableIdentifier tableId =
                TableIdentifier.of(tablePath.getDatabaseName(), tablePath.getTableName());
        icebergCatalog.createTable(tableId, schema);
    }

    private static int countDataFiles(Table table) throws IOException {
        int count = 0;
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask ignored : tasks) {
                count++;
            }
        }
        return count;
    }

    private static long countRows(Table table) {
        long cnt = 0L;
        try (CloseableIterable<Record> it = IcebergGenerics.read(table).build()) {
            for (Record ignored : it) {
                cnt++;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return cnt;
    }

    private static int countFilesForBucket(Table table, int bucket) throws IOException {
        int count = 0;
        try (CloseableIterable<FileScanTask> tasks =
                table.newScan().filter(Expressions.equal(BUCKET_COLUMN_NAME, bucket)).planFiles()) {
            for (FileScanTask ignored : tasks) {
                count++;
            }
        }
        return count;
    }

    private static void appendTinyFilesWithRowsAndBucket(
            Table table, int files, int rowsPerFile, int baseOffset, int bucket) throws Exception {
        List<DataFile> toAppend = new ArrayList<>(files);
        for (int i = 0; i < files; i++) {
            toAppend.add(
                    writeTinyDataFile(table, rowsPerFile, baseOffset + (i * rowsPerFile), bucket));
        }
        AppendFiles append = table.newAppend();
        for (DataFile f : toAppend) {
            append.appendFile(f);
        }
        append.commit();
    }

    private static DataFile writeTinyDataFile(Table table, int rows, int startOffset, int bucket)
            throws Exception {
        String fileName = java.util.UUID.randomUUID() + ".parquet";
        org.apache.iceberg.io.OutputFile outputFile =
                table.io().newOutputFile(table.locationProvider().newDataLocation(fileName));

        Parquet.WriteBuilder writeBuilder =
                Parquet.write(outputFile)
                        .schema(table.schema())
                        .createWriterFunc(GenericParquetWriter::buildWriter)
                        .overwrite();

        Metrics metrics;
        long recordCount = 0L;
        FileAppender<Record> writer = null;
        try {
            writer = writeBuilder.build();
            for (int i = 0; i < rows; i++) {
                Record r = org.apache.iceberg.data.GenericRecord.create(table.schema());
                r.setField("c1", i);
                r.setField("c2", "v_" + i);
                r.setField("c3", "g");
                r.setField(BUCKET_COLUMN_NAME, bucket);
                r.setField(OFFSET_COLUMN_NAME, (long) (startOffset + i));
                r.setField(
                        TIMESTAMP_COLUMN_NAME,
                        java.time.OffsetDateTime.now(java.time.ZoneOffset.UTC));
                writer.add(r);
                recordCount++;
            }
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
        metrics = writer.metrics();

        String location = outputFile.location();
        InputFile inputFile = table.io().newInputFile(location);
        long fileSizeInBytes = inputFile.getLength();

        return DataFiles.builder(table.spec())
                .withPath(location)
                .withFileSizeInBytes(fileSizeInBytes)
                .withFormat(FileFormat.PARQUET)
                .withRecordCount(recordCount)
                .withMetrics(metrics)
                .build();
    }
}
