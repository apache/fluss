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
import org.apache.fluss.lake.lakestorage.LakeTableLookuper;
import org.apache.fluss.lake.paimon.utils.PaimonPartitionBucket;
import org.apache.fluss.lake.paimon.utils.PaimonRowAsFlussRow;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.CompactedRowEncoder;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.IOUtils;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.query.LocalTableQuery;
import org.apache.paimon.table.sink.RowPartitionKeyExtractor;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;

import javax.annotation.Nullable;

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

/** Paimon implementation of {@link LakeTableLookuper}. */
public class PaimonLakeTableLookuper implements LakeTableLookuper {

    // Hard-code a conservative 1GB default for now to avoid unbounded lookup cache growth.
    private static final String DEFAULT_LOOKUP_CACHE_MAX_DISK_SIZE = "1gb";

    private final Configuration paimonConfig;
    private final TablePath tablePath;
    private final String ioTmpDir;

    private final Set<PaimonPartitionBucket> initializedBuckets;

    private @Nullable Catalog catalog;
    private @Nullable FileStoreTable fileStoreTable;
    private @Nullable IOManager ioManager;
    private @Nullable LocalTableQuery localTableQuery;
    private @Nullable RowPartitionKeyExtractor partitionKeyExtractor;
    private int primaryKeyFieldCount;
    private boolean hasCachedValueEncoder;
    private short cachedValueSchemaId;
    private @Nullable CompactedRowEncoder cachedValueRowEncoder;
    private @Nullable InternalRow.FieldGetter[] cachedValueFieldGetters;
    private boolean closed;

    public PaimonLakeTableLookuper(Configuration paimonConfig, TablePath tablePath) {
        this(paimonConfig, tablePath, ConfigOptions.IO_TMP_DIR.defaultValue());
    }

    public PaimonLakeTableLookuper(
            Configuration paimonConfig, TablePath tablePath, String ioTmpDir) {
        this.paimonConfig = checkNotNull(paimonConfig, "paimonConfig must not be null.");
        this.tablePath = checkNotNull(tablePath, "tablePath must not be null.");
        this.ioTmpDir = checkNotNull(ioTmpDir, "ioTmpDir must not be null.");
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
                localTableQuery().lookup(partition, context.bucketId(), keyRow);
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
        localTableQuery =
                fileStoreTable
                        .newLocalTableQuery()
                        .withValueProjection(businessFieldProjection(fileStoreTable))
                        .withIOManager(ioManager);
        partitionKeyExtractor = new RowPartitionKeyExtractor(fileStoreTable.schema());
        primaryKeyFieldCount = fileStoreTable.primaryKeys().size();
    }

    private FileStoreTable withLookupCacheOptions(FileStoreTable table) {
        String key = CoreOptions.LOOKUP_CACHE_MAX_DISK_SIZE.key();
        String configuredSize = paimonConfig.toMap().get(key);
        if (configuredSize != null) {
            return table.copy(Collections.singletonMap(key, configuredSize));
        }
        if (table.options().containsKey(key)) {
            return table;
        }
        return table.copy(Collections.singletonMap(key, DEFAULT_LOOKUP_CACHE_MAX_DISK_SIZE));
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

        // TODO: Refresh files when historical lookup needs to observe new lake snapshots.
        localTableQuery()
                .refreshFiles(
                        partition,
                        bucketId,
                        new ArrayList<>(beforeFilesByName.values()),
                        new ArrayList<>(dataFilesByName.values()));
        initializedBuckets.add(partitionBucket);
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
            CompactedRowEncoder rowEncoder =
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
        DataType[] fieldTypes = valueRowType.getChildren().toArray(new DataType[0]);
        cachedValueRowEncoder = new CompactedRowEncoder(fieldTypes);
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
