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
import org.apache.fluss.exception.KvStorageException;
import org.apache.fluss.lake.lakestorage.LakeTableLookuper;
import org.apache.fluss.lake.paimon.utils.PaimonPartitionBucket;
import org.apache.fluss.lake.paimon.utils.PaimonRowAsFlussRow;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.IOUtils;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.query.LocalTableQuery;
import org.apache.paimon.table.sink.RowPartitionKeyExtractor;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import static org.apache.fluss.lake.paimon.PaimonLakeCatalog.SYSTEM_COLUMNS;
import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimon;
import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimonPartition;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Paimon implementation of {@link LakeTableLookuper} for primary-key tables.
 *
 * <p>The catalog, table, local query, and I/O manager are initialized lazily on the first lookup.
 * For each partition and bucket, the lookuper scans the latest Paimon snapshot once and registers
 * its data files with {@link LocalTableQuery}. Paimon then creates local lookup files lazily as
 * individual remote data files are queried.
 *
 * <p>A cached partition-bucket file set can become stale when Paimon compaction replaces its data
 * files and snapshot expiration physically deletes the old files. Because {@code FileIO}
 * implementations may represent a missing file with different {@link IOException} types, the first
 * lookup I/O failure closes the cached query state, reopens the table from the latest snapshot, and
 * retries once.
 *
 * <p>Lookup and close operations are synchronized because they share mutable Paimon query, local
 * cache, and value-encoding state.
 */
public class PaimonLakeTableLookuper implements LakeTableLookuper {

    // Each TabletServer caches at most ten table lookupers, bounding their retained lookup cache
    // capacity to 20GB. See HistoricalLakeLookupManager for the follow-up to make this configurable
    // and use a global Paimon IOManager limit.
    private static final String LOOKUP_CACHE_MAX_DISK_SIZE = "2gb";
    private static final MemorySize LOOKUP_CACHE_MAX_DISK_MEMORY_SIZE =
            MemorySize.parse(LOOKUP_CACHE_MAX_DISK_SIZE);

    private final Configuration paimonConfig;
    private final TablePath tablePath;
    private final String ioTmpDir;
    private final KvFormat kvFormat;

    private final Set<PaimonPartitionBucket> initializedBuckets;

    private @Nullable Catalog catalog;
    private @Nullable FileStoreTable fileStoreTable;
    private @Nullable IOManager ioManager;
    private @Nullable LocalTableQuery localTableQuery;
    private @Nullable RowPartitionKeyExtractor partitionKeyExtractor;
    private int primaryKeyFieldCount;
    private boolean hasCachedValueEncoder;
    private short cachedValueSchemaId;
    private @Nullable RowEncoder cachedValueRowEncoder;
    private @Nullable InternalRow.FieldGetter[] cachedValueFieldGetters;
    private boolean closed;

    public PaimonLakeTableLookuper(
            Configuration paimonConfig, TablePath tablePath, KvFormat kvFormat) {
        this(paimonConfig, tablePath, ConfigOptions.IO_TMP_DIR.defaultValue(), kvFormat);
    }

    public PaimonLakeTableLookuper(
            Configuration paimonConfig, TablePath tablePath, String ioTmpDir, KvFormat kvFormat) {
        this.paimonConfig = checkNotNull(paimonConfig, "paimonConfig must not be null.");
        this.tablePath = checkNotNull(tablePath, "tablePath must not be null.");
        this.ioTmpDir = checkNotNull(ioTmpDir, "ioTmpDir must not be null.");
        this.kvFormat = checkNotNull(kvFormat, "kvFormat must not be null.");
        this.initializedBuckets = new HashSet<>();
    }

    @Override
    public synchronized @Nullable byte[] lookup(byte[] key, LookupContext context)
            throws Exception {
        checkNotNull(key, "key must not be null.");
        checkNotNull(context, "context must not be null.");
        checkNotClosed();
        ensureInitialized();

        org.apache.paimon.data.BinaryRow partition =
                convertPartition(context.partitionSpec(), context.valueRowType());
        org.apache.paimon.data.BinaryRow keyRow = wrapLookupKey(key);
        initializeFilesIfNeeded(partition, context.bucketId());

        org.apache.paimon.data.InternalRow paimonRow =
                lookupWithFileRefresh(partition, context.bucketId(), keyRow);
        if (paimonRow == null) {
            return null;
        }
        return encodeValue(paimonRow, context.schemaId(), context.valueRowType());
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        IOUtils.closeQuietly(cachedValueRowEncoder, "Fluss value row encoder");
        IOUtils.closeQuietly(localTableQuery, "Paimon local table query");
        IOUtils.closeQuietly(ioManager, "Paimon lookup IO manager");
        IOUtils.closeQuietly(catalog, "Paimon catalog");
        initializedBuckets.clear();
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("Paimon lake table lookuper has been closed.");
        }
    }

    private void ensureInitialized() throws Exception {
        if (localTableQuery != null) {
            return;
        }

        catalog =
                CatalogFactory.createCatalog(
                        CatalogContext.create(Options.fromMap(paimonConfig.toMap())));
        fileStoreTable =
                withLookupCacheOptions((FileStoreTable) catalog.getTable(toPaimon(tablePath)));
        if (fileStoreTable.primaryKeys().isEmpty()) {
            throw new UnsupportedOperationException(
                    "Point lookup is only supported for primary-key Paimon tables.");
        }
        ioManager = createIOManager(ioTmpDir);
        localTableQuery = createLocalTableQuery();
        partitionKeyExtractor = new RowPartitionKeyExtractor(fileStoreTable.schema());
        primaryKeyFieldCount = fileStoreTable.primaryKeys().size();
    }

    private LocalTableQuery createLocalTableQuery() {
        return fileStoreTable()
                .newLocalTableQuery()
                .withValueProjection(businessFieldProjection(fileStoreTable()))
                .withIOManager(checkNotNull(ioManager, "ioManager must be initialized."));
    }

    private FileStoreTable withLookupCacheOptions(FileStoreTable table) {
        String key = CoreOptions.LOOKUP_CACHE_MAX_DISK_SIZE.key();
        String configuredMaxDiskSize = table.options().get(key);
        if (configuredMaxDiskSize != null
                && MemorySize.parse(configuredMaxDiskSize)
                                .compareTo(LOOKUP_CACHE_MAX_DISK_MEMORY_SIZE)
                        <= 0) {
            return table;
        }
        return table.copy(Collections.singletonMap(key, LOOKUP_CACHE_MAX_DISK_SIZE));
    }

    private static IOManager createIOManager(String ioTmpDir) {
        return IOManager.create(ioTmpDir);
    }

    private static int[] businessFieldProjection(FileStoreTable fileStoreTable) {
        List<DataField> fields = fileStoreTable.schema().logicalRowType().getFields();
        List<Integer> projectedFields = new ArrayList<>();
        for (int i = 0; i < fields.size(); i++) {
            if (!SYSTEM_COLUMNS.containsKey(fields.get(i).name())) {
                projectedFields.add(i);
            }
        }

        int[] projection = new int[projectedFields.size()];
        for (int i = 0; i < projectedFields.size(); i++) {
            projection[i] = projectedFields.get(i);
        }
        return projection;
    }

    private org.apache.paimon.data.BinaryRow convertPartition(
            ResolvedPartitionSpec partitionSpec, RowType valueRowType) {
        return toPaimonPartition(
                partitionSpec,
                valueRowType,
                fileStoreTable().schema().logicalRowType(),
                partitionKeyExtractor()::partition);
    }

    private org.apache.paimon.data.BinaryRow wrapLookupKey(byte[] key) {
        org.apache.paimon.data.BinaryRow keyRow =
                new org.apache.paimon.data.BinaryRow(primaryKeyFieldCount);
        keyRow.pointTo(MemorySegment.wrap(key), 0, key.length);
        return keyRow;
    }

    private void initializeFilesIfNeeded(org.apache.paimon.data.BinaryRow partition, int bucketId) {
        PaimonPartitionBucket partitionBucket = new PaimonPartitionBucket(partition, bucketId);
        if (initializedBuckets.contains(partitionBucket)) {
            return;
        }

        LinkedHashMap<String, DataFileMeta> beforeFilesByName = new LinkedHashMap<>();
        LinkedHashMap<String, DataFileMeta> dataFilesByName = new LinkedHashMap<>();

        InnerTableScan tableScan =
                fileStoreTable()
                        .newScan()
                        .withPartitionFilter(Collections.singletonList(partition))
                        .withBucket(bucketId);
        for (Split split : tableScan.plan().splits()) {
            if (!(split instanceof DataSplit)) {
                continue;
            }
            DataSplit dataSplit = (DataSplit) split;
            addFilesByName(beforeFilesByName, dataSplit.beforeFiles());
            addFilesByName(dataFilesByName, dataSplit.dataFiles());
        }

        // TODO: Refresh the file set if writes to expired partitions are supported in the future.
        // Historical lookup is triggered only after the original Fluss partition has expired and
        // been dropped. This PR does not support writes to expired partitions, so no new rows are
        // expected and initializing the file set once is sufficient. Compaction-related missing
        // files are handled by the IOException refresh path below.
        localTableQuery()
                .refreshFiles(
                        partition,
                        bucketId,
                        new ArrayList<>(beforeFilesByName.values()),
                        new ArrayList<>(dataFilesByName.values()));
        initializedBuckets.add(partitionBucket);
    }

    private org.apache.paimon.data.InternalRow lookupWithFileRefresh(
            org.apache.paimon.data.BinaryRow partition,
            int bucketId,
            org.apache.paimon.data.InternalRow keyRow)
            throws Exception {
        try {
            return localTableQuery().lookup(partition, bucketId, keyRow);
        } catch (IOException e) {
            // FileIO only guarantees IOException and storage plugins may use different exception
            // types for a missing file. The missing old file after compaction may therefore
            // surface as any IOException. Refresh and retry only once so persistent I/O failures
            // do not repeatedly rebuild Paimon lookup state within one request.
            try {
                refreshFiles(partition, bucketId);
                return localTableQuery().lookup(partition, bucketId, keyRow);
            } catch (IOException retryError) {
                retryError.addSuppressed(e);
                // Historical Paimon point lookup is part of the Fluss KV lookup path. Expose a
                // persistent I/O failure as a retriable KV error so the existing KV RPC retry
                // semantics can handle it consistently.
                throw new KvStorageException(
                        "Failed to lookup historical data from Paimon after refreshing files for "
                                + tablePath
                                + ".",
                        retryError);
            }
        }
    }

    private void refreshFiles(org.apache.paimon.data.BinaryRow partition, int bucketId)
            throws Exception {
        IOUtils.closeQuietly(localTableQuery, "Paimon local table query");
        IOUtils.closeQuietly(ioManager, "Paimon lookup IO manager");
        IOUtils.closeQuietly(catalog, "Paimon catalog");
        localTableQuery = null;
        ioManager = null;
        catalog = null;
        fileStoreTable = null;
        partitionKeyExtractor = null;
        initializedBuckets.clear();
        ensureInitialized();
        initializeFilesIfNeeded(partition, bucketId);
    }

    private static void addFilesByName(
            LinkedHashMap<String, DataFileMeta> filesByName, List<DataFileMeta> files) {
        for (DataFileMeta file : files) {
            filesByName.put(file.fileName(), file);
        }
    }

    private byte[] encodeValue(
            org.apache.paimon.data.InternalRow paimonRow, short schemaId, RowType valueRowType) {
        PaimonRowAsFlussRow flussRow = new PaimonRowAsFlussRow(paimonRow);
        try {
            ensureValueEncoder(schemaId, valueRowType);
            RowEncoder rowEncoder =
                    checkNotNull(cachedValueRowEncoder, "cachedValueRowEncoder must not be null.");
            InternalRow.FieldGetter[] fieldGetters =
                    checkNotNull(
                            cachedValueFieldGetters, "cachedValueFieldGetters must not be null.");

            rowEncoder.startNewRow();
            for (int i = 0; i < fieldGetters.length; i++) {
                rowEncoder.encodeField(i, fieldGetters[i].getFieldOrNull(flussRow));
            }
            BinaryRow row = rowEncoder.finishRow();
            return ValueEncoder.encodeValue(schemaId, row);
        } catch (Exception e) {
            throw new RuntimeException("Failed to encode Paimon lookup row as Fluss value.", e);
        }
    }

    private void ensureValueEncoder(short schemaId, RowType valueRowType) {
        if (hasCachedValueEncoder && cachedValueSchemaId == schemaId) {
            return;
        }

        IOUtils.closeQuietly(cachedValueRowEncoder, "Fluss value row encoder");
        cachedValueRowEncoder = RowEncoder.create(kvFormat, valueRowType);
        cachedValueFieldGetters = InternalRow.createFieldGetters(valueRowType);
        cachedValueSchemaId = schemaId;
        hasCachedValueEncoder = true;
    }

    private FileStoreTable fileStoreTable() {
        return checkNotNull(fileStoreTable, "fileStoreTable must be initialized.");
    }

    private LocalTableQuery localTableQuery() {
        return checkNotNull(localTableQuery, "localTableQuery must be initialized.");
    }

    private RowPartitionKeyExtractor partitionKeyExtractor() {
        return checkNotNull(partitionKeyExtractor, "partitionKeyExtractor must be initialized.");
    }
}
