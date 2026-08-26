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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.exception.DiskWriteLockedException;
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
import org.apache.fluss.utils.ExecutorUtils;

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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.fluss.config.ConfigOptions.KV_FORMAT_VERSION_2;
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
    private static final long LOOKUP_CACHE_MAX_DISK_BYTES = MemorySize.parse("8gb").getBytes();
    private static final LakeTableLookuper.LookupMetricRecorder NO_OP_LOOKUP_METRIC_RECORDER =
            (lookupTimeNanos, lookupFileDownloaded) -> {};
    private static final Runnable NO_OP_DISK_WRITE_GUARD = () -> {};

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
                new PaimonLakeTableLookuper(
                        paimonConfig,
                        tablePath,
                        tempWarehouseDir.getAbsolutePath(),
                        tableConfig(KvFormat.COMPACTED),
                        LOOKUP_CACHE_MAX_DISK_BYTES,
                        NO_OP_DISK_WRITE_GUARD)) {
            List<Boolean> lookupFileDownloads = new ArrayList<>();
            LakeTableLookuper.LookupContext context =
                    lookupContext(
                            schema,
                            "20240101",
                            0,
                            SCHEMA_ID,
                            (lookupTimeNanos, lookupFileDownloaded) ->
                                    lookupFileDownloads.add(lookupFileDownloaded));

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

            // The first lookup creates the local lookup file, while subsequent lookups reuse it.
            assertThat(lookupFileDownloads).containsExactly(true, false, false);
        }
    }

    @Test
    void testDiskWriteLockBlocksOnlyLookupFileDownloads() throws Exception {
        TablePath tablePath = TablePath.of(DB, "disk_write_lock");
        Schema schema = pkSchema();
        FileStoreTable table = createPaimonTable(tablePath, partitionedPkDescriptor(schema));
        writeAndCommitData(
                table,
                Collections.singletonMap(
                        0,
                        Arrays.asList(
                                paimonRow(1, "20240101", "Alice"),
                                paimonRow(2, "20240102", "Bob"))));
        AtomicBoolean diskWriteLocked = new AtomicBoolean();
        Runnable diskWriteGuard =
                () -> {
                    if (diskWriteLocked.get()) {
                        throw new DiskWriteLockedException("Data disk is write-locked.");
                    }
                };

        try (LakeTableLookuper lookuper =
                new PaimonLakeTableLookuper(
                        paimonConfig,
                        tablePath,
                        tempWarehouseDir.getAbsolutePath(),
                        tableConfig(KvFormat.COMPACTED),
                        LOOKUP_CACHE_MAX_DISK_BYTES,
                        diskWriteGuard)) {
            LakeTableLookuper.LookupContext cachedPartition =
                    lookupContext(schema, "20240101", 0, SCHEMA_ID);
            LakeTableLookuper.LookupContext uncachedPartition =
                    lookupContext(schema, "20240102", 0, SCHEMA_ID);

            assertThat(lookuper.lookup(paimonKey(schema, 1, "20240101"), cachedPartition))
                    .isNotNull();
            diskWriteLocked.set(true);

            // Cache hits remain available, while a lookup that needs a new local file is rejected.
            assertThat(lookuper.lookup(paimonKey(schema, 1, "20240101"), cachedPartition))
                    .isNotNull();
            assertThatThrownBy(
                            () ->
                                    lookuper.lookup(
                                            paimonKey(schema, 2, "20240102"), uncachedPartition))
                    .isInstanceOf(DiskWriteLockedException.class);

            diskWriteLocked.set(false);
            assertThat(lookuper.lookup(paimonKey(schema, 2, "20240102"), uncachedPartition))
                    .isNotNull();
        }
    }

    @Test
    void testLookupPartitionsWithSameHashCode() throws Exception {
        // These distinct partition values produce the same BinaryRow hash code, reproducing the
        // mutable-key collision that previously made one partition reuse another partition's files.
        String firstPartition = "b8";
        String secondPartition = "17k3";
        BinaryRow firstPartitionRow =
                BinaryRow.singleColumn(BinaryString.fromString(firstPartition));
        BinaryRow secondPartitionRow =
                BinaryRow.singleColumn(BinaryString.fromString(secondPartition));
        assertThat(firstPartitionRow).isNotEqualTo(secondPartitionRow);
        assertThat(firstPartitionRow.hashCode()).isEqualTo(secondPartitionRow.hashCode());

        TablePath tablePath = TablePath.of(DB, "partition_hash_collision");
        Schema schema = pkSchema();
        FileStoreTable table = createPaimonTable(tablePath, partitionedPkDescriptor(schema));
        writeAndCommitData(
                table,
                Collections.singletonMap(
                        0, Collections.singletonList(paimonRow(1, firstPartition, "Alice"))));
        writeAndCommitData(
                table,
                Collections.singletonMap(
                        0, Collections.singletonList(paimonRow(2, secondPartition, "Bob"))));

        try (LakeTableLookuper lookuper =
                new PaimonLakeTableLookuper(
                        paimonConfig,
                        tablePath,
                        tempWarehouseDir.getAbsolutePath(),
                        tableConfig(KvFormat.COMPACTED),
                        LOOKUP_CACHE_MAX_DISK_BYTES,
                        NO_OP_DISK_WRITE_GUARD)) {
            BinaryValue firstValue =
                    decodeValue(
                            lookuper.lookup(
                                    paimonKey(schema, 1, firstPartition),
                                    lookupContext(schema, firstPartition, 0, SCHEMA_ID)),
                            SCHEMA_ID,
                            schema);
            BinaryValue secondValue =
                    decodeValue(
                            lookuper.lookup(
                                    paimonKey(schema, 2, secondPartition),
                                    lookupContext(schema, secondPartition, 0, SCHEMA_ID)),
                            SCHEMA_ID,
                            schema);

            assertRow(firstValue.row, 1, firstPartition, "Alice");
            assertRow(secondValue.row, 2, secondPartition, "Bob");
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
                new PaimonLakeTableLookuper(
                        paimonConfig,
                        tablePath,
                        tempWarehouseDir.getAbsolutePath(),
                        tableConfig(KvFormat.INDEXED),
                        LOOKUP_CACHE_MAX_DISK_BYTES,
                        NO_OP_DISK_WRITE_GUARD)) {
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
    void testLookupKvFormatV2WithNonDefaultBucketKey() throws Exception {
        TablePath tablePath = TablePath.of(DB, "non_default_bucket_key");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("sub_id", DataTypes.STRING())
                        .column("dt", DataTypes.STRING())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id", "sub_id", "dt")
                        .build();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .partitionedBy("dt")
                        .distributedBy(1, "id")
                        .build();
        FileStoreTable table = createPaimonTable(tablePath, tableDescriptor);
        writeAndCommitData(
                table,
                Collections.singletonMap(
                        0, Collections.singletonList(paimonRow(1, "sub-1", "20240101", "Alice"))));

        try (LakeTableLookuper lookuper =
                new PaimonLakeTableLookuper(
                        paimonConfig,
                        tablePath,
                        tempWarehouseDir.getAbsolutePath(),
                        tableConfig(KvFormat.COMPACTED, KV_FORMAT_VERSION_2),
                        LOOKUP_CACHE_MAX_DISK_BYTES,
                        NO_OP_DISK_WRITE_GUARD)) {
            LakeTableLookuper.LookupContext context =
                    lookupContext(schema, "20240101", 0, SCHEMA_ID);
            byte[] compactedKey =
                    CompactedKeyEncoder.createKeyEncoder(
                                    schema.getRowType(), Arrays.asList("id", "sub_id"))
                            .encodeKey(row(1, "sub-1", "20240101", ""));

            BinaryValue decodedValue =
                    decodeValue(lookuper.lookup(compactedKey, context), SCHEMA_ID, schema);

            assertRow(decodedValue.row, 1, "sub-1", "20240101", "Alice");
        }
    }

    @Test
    void testRetriesInitializationAfterLookupKeyConverterFailure() throws Exception {
        TablePath tablePath = TablePath.of(DB, "retry_initialization");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("sub_id", DataTypes.STRING())
                        .column("dt", DataTypes.STRING())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id", "sub_id", "dt")
                        .build();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .partitionedBy("dt")
                        .distributedBy(1, "id")
                        .build();
        FileStoreTable table = createPaimonTable(tablePath, tableDescriptor);
        writeAndCommitData(
                table,
                Collections.singletonMap(
                        0, Collections.singletonList(paimonRow(1, "sub-1", "20240101", "Alice"))));

        byte[] compactedKey =
                CompactedKeyEncoder.createKeyEncoder(
                                schema.getRowType(), Arrays.asList("id", "sub_id"))
                        .encodeKey(row(1, "sub-1", "20240101", ""));
        Schema invalidSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("dt", DataTypes.STRING())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id", "dt")
                        .build();

        try (LakeTableLookuper lookuper =
                new PaimonLakeTableLookuper(
                        paimonConfig,
                        tablePath,
                        tempWarehouseDir.getAbsolutePath(),
                        tableConfig(KvFormat.COMPACTED, KV_FORMAT_VERSION_2),
                        LOOKUP_CACHE_MAX_DISK_BYTES,
                        NO_OP_DISK_WRITE_GUARD)) {
            // Inject a late initialization failure: the Paimon table requires sub_id in its
            // lookup key, but the first lookup's value row type deliberately omits that field.
            assertThatThrownBy(
                            () ->
                                    lookuper.lookup(
                                            compactedKey,
                                            lookupContext(invalidSchema, "20240101", 0, SCHEMA_ID)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("sub_id");

            // The failed attempt must not leave localTableQuery as a false initialization marker.
            // A second lookup on the same object should rebuild all resources and succeed.
            byte[] value =
                    lookuper.lookup(compactedKey, lookupContext(schema, "20240101", 0, SCHEMA_ID));
            assertThat(value).isNotNull();
            BinaryValue decodedValue = decodeValue(value, SCHEMA_ID, schema);
            assertRow(decodedValue.row, 1, "sub-1", "20240101", "Alice");
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
        writeAndCommitData(
                table,
                Collections.singletonMap(
                        0, Collections.singletonList(paimonRow(6, "20240102", "name-6"))));

        BinaryRow partition = BinaryRow.singleColumn(BinaryString.fromString("20240101"));
        List<DataFileMeta> filesBeforeCompaction = dataFiles(table, partition, 0);
        assertThat(filesBeforeCompaction).hasSize(5);

        AtomicBoolean blockDownloads = new AtomicBoolean();
        CountDownLatch downloadStarted = new CountDownLatch(1);
        CountDownLatch continueDownload = new CountDownLatch(1);
        Runnable diskWriteGuard =
                () -> {
                    if (blockDownloads.get()) {
                        downloadStarted.countDown();
                        try {
                            continueDownload.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        }
                    }
                };

        try (LakeTableLookuper lookuper =
                new PaimonLakeTableLookuper(
                        paimonConfig,
                        tablePath,
                        tempWarehouseDir.getAbsolutePath(),
                        tableConfig(KvFormat.COMPACTED),
                        LOOKUP_CACHE_MAX_DISK_BYTES,
                        diskWriteGuard)) {
            assertThat(
                            lookuper.lookup(
                                    paimonKey(schema, 5, "20240101"),
                                    lookupContext(schema, "20240101", 0, SCHEMA_ID)))
                    .isNotNull();
            assertThat(
                            lookuper.lookup(
                                    paimonKey(schema, 6, "20240102"),
                                    lookupContext(schema, "20240102", 0, SCHEMA_ID)))
                    .isNotNull();

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

            List<Boolean> refreshedLookupDownloads = new ArrayList<>();
            List<Boolean> cachedLookupDownloads = new ArrayList<>();
            LakeTableLookuper.LookupContext refreshedContext =
                    lookupContext(
                            schema,
                            "20240101",
                            0,
                            SCHEMA_ID,
                            (lookupTimeNanos, lookupFileDownloaded) ->
                                    refreshedLookupDownloads.add(lookupFileDownloaded));
            LakeTableLookuper.LookupContext cachedContext =
                    lookupContext(
                            schema,
                            "20240102",
                            0,
                            SCHEMA_ID,
                            (lookupTimeNanos, lookupFileDownloaded) ->
                                    cachedLookupDownloads.add(lookupFileDownloaded));
            ExecutorService executor = Executors.newFixedThreadPool(2);
            blockDownloads.set(true);
            try {
                Future<byte[]> refreshedLookup =
                        executor.submit(
                                () ->
                                        lookuper.lookup(
                                                paimonKey(schema, 1, "20240101"),
                                                refreshedContext));
                assertThat(downloadStarted.await(30, TimeUnit.SECONDS)).isTrue();

                Future<byte[]> cachedLookup =
                        executor.submit(
                                () ->
                                        lookuper.lookup(
                                                paimonKey(schema, 6, "20240102"), cachedContext));
                BinaryValue cachedValue =
                        decodeValue(cachedLookup.get(30, TimeUnit.SECONDS), SCHEMA_ID, schema);
                assertRow(cachedValue.row, 6, "20240102", "name-6");

                continueDownload.countDown();
                BinaryValue refreshedValue =
                        decodeValue(refreshedLookup.get(30, TimeUnit.SECONDS), SCHEMA_ID, schema);
                assertRow(refreshedValue.row, 1, "20240101", "name-1");
            } finally {
                continueDownload.countDown();
                ExecutorUtils.gracefulShutdown(30, TimeUnit.SECONDS, executor);
            }

            assertThat(refreshedLookupDownloads).containsExactly(true);
            assertThat(cachedLookupDownloads).containsExactly(false);
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
                new PaimonLakeTableLookuper(
                        paimonConfig,
                        tablePath,
                        tempWarehouseDir.getAbsolutePath(),
                        tableConfig(KvFormat.COMPACTED),
                        LOOKUP_CACHE_MAX_DISK_BYTES,
                        NO_OP_DISK_WRITE_GUARD)) {
            LakeTableLookuper.LookupContext context =
                    new LakeTableLookuper.LookupContext(
                            ResolvedPartitionSpec.fromPartitionName(
                                    Collections.singletonList("pt"), "7"),
                            0,
                            SCHEMA_ID,
                            schema.getRowType(),
                            NO_OP_LOOKUP_METRIC_RECORDER);

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
                new PaimonLakeTableLookuper(
                        paimonConfig,
                        tablePath,
                        tempWarehouseDir.getAbsolutePath(),
                        tableConfig(KvFormat.COMPACTED),
                        LOOKUP_CACHE_MAX_DISK_BYTES,
                        NO_OP_DISK_WRITE_GUARD)) {
            LakeTableLookuper.LookupContext context =
                    new LakeTableLookuper.LookupContext(
                            new ResolvedPartitionSpec(
                                    Collections.emptyList(), Collections.emptyList()),
                            0,
                            SCHEMA_ID,
                            schema.getRowType(),
                            NO_OP_LOOKUP_METRIC_RECORDER);

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
                new PaimonLakeTableLookuper(
                        paimonConfig,
                        tablePath,
                        tempWarehouseDir.getAbsolutePath(),
                        tableConfig(KvFormat.COMPACTED),
                        LOOKUP_CACHE_MAX_DISK_BYTES,
                        NO_OP_DISK_WRITE_GUARD)) {
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

    private static TableConfig tableConfig(KvFormat kvFormat) {
        return tableConfig(kvFormat, 1);
    }

    private static TableConfig tableConfig(KvFormat kvFormat, int kvFormatVersion) {
        Configuration config = new Configuration();
        config.set(ConfigOptions.TABLE_KV_FORMAT, kvFormat);
        config.set(ConfigOptions.TABLE_KV_FORMAT_VERSION, kvFormatVersion);
        return new TableConfig(config);
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
                schema.getRowType(),
                NO_OP_LOOKUP_METRIC_RECORDER);
    }

    private static LakeTableLookuper.LookupContext lookupContext(
            Schema schema,
            String partitionName,
            int bucket,
            short schemaId,
            LakeTableLookuper.LookupMetricRecorder lookupMetricRecorder) {
        return new LakeTableLookuper.LookupContext(
                ResolvedPartitionSpec.fromPartitionName(
                        Collections.singletonList("dt"), partitionName),
                bucket,
                schemaId,
                schema.getRowType(),
                lookupMetricRecorder);
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

    private static void assertRow(InternalRow row, int id, String subId, String dt, String name) {
        assertThat(row.getInt(0)).isEqualTo(id);
        assertThat(row.getString(1).toString()).isEqualTo(subId);
        assertThat(row.getString(2).toString()).isEqualTo(dt);
        assertThat(row.getString(3).toString()).isEqualTo(name);
    }
}
