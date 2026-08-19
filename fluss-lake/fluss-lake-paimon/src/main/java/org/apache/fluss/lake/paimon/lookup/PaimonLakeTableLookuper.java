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
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.exception.DiskWriteLockedException;
import org.apache.fluss.exception.KvStorageException;
import org.apache.fluss.lake.lakestorage.LakeTableLookuper;
import org.apache.fluss.lake.paimon.utils.PaimonPartitionBucket;
import org.apache.fluss.lake.paimon.utils.PaimonRowAsFlussRow;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.decode.CompactedKeyDecoder;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.row.encode.paimon.PaimonKeyEncoder;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.IOUtils;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.disk.BufferFileReader;
import org.apache.paimon.disk.BufferFileWriter;
import org.apache.paimon.disk.FileIOChannel;
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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.apache.fluss.config.ConfigOptions.KV_FORMAT_VERSION_2;
import static org.apache.fluss.lake.paimon.PaimonLakeCatalog.LEGACY_SYSTEM_COLUMNS;
import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimon;
import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimonPartition;
import static org.apache.fluss.utils.Preconditions.checkArgument;
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
 * lookup I/O failure refreshes that partition-bucket with the files from the latest snapshot and
 * retries once.
 *
 * <p>Lookup calls do not acquire a Fluss-level lock, allowing a Paimon version that supports
 * concurrent lookup to serve them concurrently. Lazy initialization is synchronized only on its
 * slow path. File scans and {@link LocalTableQuery#refreshFiles} are synchronized on the query
 * instance to coordinate with Paimon 1.3's synchronized lookup. Paimon 2.0 does not acquire that
 * monitor for lookup and coordinates lookup with refresh through its internal bucket locks.
 *
 * <p>Close is expected only after the owner has drained active lookups. It is synchronized with
 * lazy initialization, but deliberately does not add a lifecycle lock to every lookup.
 */
public class PaimonLakeTableLookuper implements LakeTableLookuper {

    private final Configuration paimonConfig;
    private final TablePath tablePath;
    private final String ioTmpDir;
    private final TableConfig tableConfig;
    private final long lookupCacheMaxDiskBytes;
    private final Runnable diskWriteGuard;

    private final AtomicLong lookupFileDownloadCount;
    private final Object initializationLock;

    private volatile @Nullable QueryState queryState;
    // Guarded by initializationLock.
    private boolean closed;

    /** Creates a lookuper with the specified local lookup cache limit. */
    public PaimonLakeTableLookuper(
            Configuration paimonConfig,
            TablePath tablePath,
            String ioTmpDir,
            TableConfig tableConfig,
            long lookupCacheMaxDiskBytes,
            Runnable diskWriteGuard) {
        this.paimonConfig = checkNotNull(paimonConfig, "paimonConfig must not be null.");
        this.tablePath = checkNotNull(tablePath, "tablePath must not be null.");
        this.ioTmpDir = checkNotNull(ioTmpDir, "ioTmpDir must not be null.");
        this.tableConfig = checkNotNull(tableConfig, "tableConfig must not be null.");
        checkArgument(
                lookupCacheMaxDiskBytes > 0, "lookupCacheMaxDiskBytes must be greater than 0.");
        this.lookupCacheMaxDiskBytes = lookupCacheMaxDiskBytes;
        this.diskWriteGuard = checkNotNull(diskWriteGuard, "diskWriteGuard must not be null.");
        this.lookupFileDownloadCount = new AtomicLong();
        this.initializationLock = new Object();
    }

    @Override
    public @Nullable byte[] lookup(byte[] key, LookupContext context) throws Exception {
        checkNotNull(key, "key must not be null.");
        checkNotNull(context, "context must not be null.");
        QueryState state = ensureInitialized(context.valueRowType());

        LookupRow lookupRow = createLookupRow(state, key, context);
        initializeFilesIfNeeded(state, lookupRow.partition, context.bucketId());

        long downloadCountBeforeLookup = lookupFileDownloadCount.get();
        long lookupStartNanos = System.nanoTime();
        try {
            return lookupWithFileRefresh(state, lookupRow, context);
        } catch (Exception e) {
            DiskWriteLockedException diskWriteLockedException =
                    ExceptionUtils.findThrowable(e, DiskWriteLockedException.class).orElse(null);
            if (diskWriteLockedException != null) {
                throw diskWriteLockedException;
            }
            throw e;
        } finally {
            // TODO: Attribute downloads to the request that triggered them. The global counter is
            // thread-safe but may report another concurrent request's download for this lookup.
            context.lookupMetricRecorder()
                    .recordLookup(
                            System.nanoTime() - lookupStartNanos,
                            lookupFileDownloadCount.get() > downloadCountBeforeLookup);
        }
    }

    @Override
    public void close() {
        synchronized (initializationLock) {
            if (closed) {
                return;
            }
            closed = true;
            if (queryState != null) {
                queryState.close();
                queryState = null;
            }
        }
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("Paimon lake table lookuper has been closed.");
        }
    }

    private QueryState ensureInitialized(RowType valueRowType) throws Exception {
        if (queryState == null) {
            synchronized (initializationLock) {
                if (queryState == null) {
                    queryState = createQueryState(valueRowType);
                }
            }
        }
        return queryState;
    }

    private QueryState createQueryState(RowType valueRowType) throws Exception {
        Catalog newCatalog = null;
        IOManager newIOManager = null;
        LocalTableQuery newLocalTableQuery = null;
        boolean initialized = false;
        try {
            newCatalog =
                    CatalogFactory.createCatalog(
                            CatalogContext.create(Options.fromMap(paimonConfig.toMap())));
            FileStoreTable newFileStoreTable =
                    withLookupCacheOptions(
                            (FileStoreTable) newCatalog.getTable(toPaimon(tablePath)));
            if (newFileStoreTable.primaryKeys().isEmpty()) {
                throw new UnsupportedOperationException(
                        "Point lookup is only supported for primary-key Paimon tables.");
            }

            List<String> trimmedPrimaryKeys =
                    Collections.unmodifiableList(
                            new ArrayList<>(newFileStoreTable.schema().trimmedPrimaryKeys()));
            CompactedKeyDecoder newCompactedKeyDecoder = null;

            // Legacy/v1 tables and v2 tables with a default bucket key already encode Fluss
            // lookup keys with Paimon's key encoder. Only v2 tables with a non-default bucket
            // key use the compacted key encoding and need conversion before querying Paimon.
            if (tableConfig.getKvFormatVersion().orElse(1) == KV_FORMAT_VERSION_2
                    && !newFileStoreTable.schema().bucketKeys().equals(trimmedPrimaryKeys)) {
                // Kv-format-v2 tables with a non-default bucket key store Fluss keys using the
                // compacted encoding to support prefix lookup. Paimon's LocalTableQuery expects
                // its own BinaryRow encoding, so convert the key at the lake lookup boundary.
                newCompactedKeyDecoder =
                        CompactedKeyDecoder.createKeyDecoder(valueRowType, trimmedPrimaryKeys);
            }

            newIOManager = createIOManager(ioTmpDir);
            newLocalTableQuery =
                    newFileStoreTable
                            .newLocalTableQuery()
                            .withValueProjection(businessFieldProjection(newFileStoreTable))
                            .withIOManager(newIOManager);

            QueryState newQueryState =
                    new QueryState(
                            newCatalog,
                            newFileStoreTable,
                            newIOManager,
                            newLocalTableQuery,
                            trimmedPrimaryKeys,
                            newCompactedKeyDecoder);
            initialized = true;
            return newQueryState;
        } finally {
            if (!initialized) {
                IOUtils.closeQuietly(newLocalTableQuery, "Paimon local table query");
                IOUtils.closeQuietly(newIOManager, "Paimon lookup IO manager");
                IOUtils.closeQuietly(newCatalog, "Paimon catalog");
            }
        }
    }

    private FileStoreTable withLookupCacheOptions(FileStoreTable table) {
        String key = CoreOptions.LOOKUP_CACHE_MAX_DISK_SIZE.key();
        String maxDiskSize = new MemorySize(lookupCacheMaxDiskBytes).toString();
        return table.copy(Collections.singletonMap(key, maxDiskSize));
    }

    private IOManager createIOManager(String ioTmpDir) {
        return new TrackingIOManager(IOManager.create(ioTmpDir));
    }

    private static int[] businessFieldProjection(FileStoreTable fileStoreTable) {
        List<DataField> fields = fileStoreTable.schema().logicalRowType().getFields();
        List<Integer> projectedFields = new ArrayList<>();
        for (int i = 0; i < fields.size(); i++) {
            if (!LEGACY_SYSTEM_COLUMNS.containsKey(fields.get(i).name())) {
                projectedFields.add(i);
            }
        }

        int[] projection = new int[projectedFields.size()];
        for (int i = 0; i < projectedFields.size(); i++) {
            projection[i] = projectedFields.get(i);
        }
        return projection;
    }

    private LookupRow createLookupRow(QueryState state, byte[] key, LookupContext context) {
        // Both generated helpers reuse mutable writers or projections, so keep them confined to
        // this lookup call.
        RowPartitionKeyExtractor partitionKeyExtractor =
                new RowPartitionKeyExtractor(state.fileStoreTable.schema());
        org.apache.paimon.data.BinaryRow partition =
                toPaimonPartition(
                                context.partitionSpec(),
                                context.valueRowType(),
                                state.fileStoreTable.schema().logicalRowType(),
                                partitionKeyExtractor::partition)
                        .copy();

        byte[] paimonKey = key;
        if (state.compactedKeyDecoder != null) {
            InternalRow decodedKey = state.compactedKeyDecoder.decodeKey(key);
            RowType keyRowType = context.valueRowType().project(state.trimmedPrimaryKeys);
            PaimonKeyEncoder paimonKeyEncoder =
                    new PaimonKeyEncoder(keyRowType, state.trimmedPrimaryKeys);
            paimonKey = paimonKeyEncoder.encodeKey(decodedKey);
        }

        org.apache.paimon.data.BinaryRow keyRow =
                new org.apache.paimon.data.BinaryRow(state.trimmedPrimaryKeys.size());
        keyRow.pointTo(MemorySegment.wrap(paimonKey), 0, paimonKey.length);
        return new LookupRow(partition, keyRow);
    }

    private void initializeFilesIfNeeded(
            QueryState state, org.apache.paimon.data.BinaryRow partition, int bucketId) {
        PaimonPartitionBucket partitionBucket = new PaimonPartitionBucket(partition, bucketId);
        Supplier<Boolean> exists = () -> state.initializedBucketFiles.containsKey(partitionBucket);
        if (!exists.get()) {
            synchronized (state.localTableQuery) {
                if (!exists.get()) {
                    List<DataFileMeta> currentFiles =
                            scanCurrentDataFiles(state.fileStoreTable, partition, bucketId);
                    state.localTableQuery.refreshFiles(
                            partition, bucketId, Collections.emptyList(), currentFiles);
                    // Publish only after Paimon accepted the complete file set.
                    state.initializedBucketFiles.put(partitionBucket, currentFiles);
                }
            }
        }
    }

    private @Nullable byte[] lookupWithFileRefresh(
            QueryState state, LookupRow lookupRow, LookupContext context) throws Exception {
        int bucketId = context.bucketId();
        PaimonPartitionBucket partitionBucket =
                new PaimonPartitionBucket(lookupRow.partition, bucketId);
        List<DataFileMeta> filesBeforeLookup = registeredFiles(state, partitionBucket);
        try {
            return lookupAndEncode(state, lookupRow, context);
        } catch (IOException e) {
            // FileIO only guarantees IOException and storage plugins may use different exception
            // types for a missing file. The missing old file after compaction may therefore
            // surface as any IOException. Refresh and retry only once so persistent I/O failures
            // do not repeatedly refresh Paimon lookup state within one request.
            try {
                refreshFilesIfUnchanged(
                        state, lookupRow.partition, bucketId, partitionBucket, filesBeforeLookup);
                return lookupAndEncode(state, lookupRow, context);
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

    private @Nullable byte[] lookupAndEncode(
            QueryState state, LookupRow lookupRow, LookupContext context) throws IOException {
        org.apache.paimon.data.InternalRow paimonRow =
                state.localTableQuery.lookup(
                        lookupRow.partition, context.bucketId(), lookupRow.keyRow);
        if (paimonRow == null) {
            return null;
        }
        return encodeValue(paimonRow, context.schemaId(), context.valueRowType());
    }

    private List<DataFileMeta> registeredFiles(
            QueryState state, PaimonPartitionBucket partitionBucket) {
        return checkNotNull(
                state.initializedBucketFiles.get(partitionBucket),
                "Partition-bucket files must be initialized.");
    }

    private void refreshFilesIfUnchanged(
            QueryState state,
            org.apache.paimon.data.BinaryRow partition,
            int bucketId,
            PaimonPartitionBucket partitionBucket,
            List<DataFileMeta> filesBeforeLookup) {
        synchronized (state.localTableQuery) {
            if (state.initializedBucketFiles.get(partitionBucket) != filesBeforeLookup) {
                return;
            }
            List<DataFileMeta> latestFiles =
                    scanCurrentDataFiles(state.fileStoreTable, partition, bucketId);
            state.localTableQuery.refreshFiles(partition, bucketId, filesBeforeLookup, latestFiles);
            // The immutable list reference is also the bucket's refresh version. Publishing a new
            // reference lets concurrent failures observe that refresh has already completed.
            state.initializedBucketFiles.put(partitionBucket, latestFiles);
        }
    }

    private static List<DataFileMeta> scanCurrentDataFiles(
            FileStoreTable fileStoreTable,
            org.apache.paimon.data.BinaryRow partition,
            int bucketId) {
        LinkedHashMap<String, DataFileMeta> dataFilesByName = new LinkedHashMap<>();
        InnerTableScan tableScan =
                fileStoreTable
                        .newScan()
                        .withPartitionFilter(Collections.singletonList(partition))
                        .withBucket(bucketId);
        for (Split split : tableScan.plan().splits()) {
            if (split instanceof DataSplit) {
                addFilesByName(dataFilesByName, ((DataSplit) split).dataFiles());
            }
        }
        return Collections.unmodifiableList(new ArrayList<>(dataFilesByName.values()));
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
        InternalRow.FieldGetter[] fieldGetters = InternalRow.createFieldGetters(valueRowType);
        try (RowEncoder rowEncoder = RowEncoder.create(tableConfig.getKvFormat(), valueRowType)) {
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

    private static final class LookupRow {
        private final org.apache.paimon.data.BinaryRow partition;
        private final org.apache.paimon.data.BinaryRow keyRow;

        private LookupRow(
                org.apache.paimon.data.BinaryRow partition,
                org.apache.paimon.data.BinaryRow keyRow) {
            this.partition = partition;
            this.keyRow = keyRow;
        }
    }

    private static final class QueryState {
        private final Catalog catalog;
        private final FileStoreTable fileStoreTable;
        private final IOManager ioManager;
        private final LocalTableQuery localTableQuery;
        private final List<String> trimmedPrimaryKeys;
        private final Map<PaimonPartitionBucket, List<DataFileMeta>> initializedBucketFiles;

        // CompactedKeyDecoder contains immutable type metadata and creates all decode state per
        // invocation, so it can be shared by concurrent lookups.
        private final @Nullable CompactedKeyDecoder compactedKeyDecoder;

        private QueryState(
                Catalog catalog,
                FileStoreTable fileStoreTable,
                IOManager ioManager,
                LocalTableQuery localTableQuery,
                List<String> trimmedPrimaryKeys,
                @Nullable CompactedKeyDecoder compactedKeyDecoder) {
            this.catalog = catalog;
            this.fileStoreTable = fileStoreTable;
            this.ioManager = ioManager;
            this.localTableQuery = localTableQuery;
            this.trimmedPrimaryKeys = trimmedPrimaryKeys;
            this.initializedBucketFiles = new ConcurrentHashMap<>();
            this.compactedKeyDecoder = compactedKeyDecoder;
        }

        private void close() {
            IOUtils.closeQuietly(localTableQuery, "Paimon local table query");
            IOUtils.closeQuietly(ioManager, "Paimon lookup IO manager");
            IOUtils.closeQuietly(catalog, "Paimon catalog");
            initializedBucketFiles.clear();
        }
    }

    /** Tracks creation of Paimon lookup files while delegating all local I/O operations. */
    private final class TrackingIOManager implements IOManager {

        private final IOManager delegate;

        private TrackingIOManager(IOManager delegate) {
            this.delegate = delegate;
        }

        @Override
        public FileIOChannel.ID createChannel() {
            return delegate.createChannel();
        }

        @Override
        public FileIOChannel.ID createChannel(String prefix) {
            try {
                diskWriteGuard.run();
            } catch (DiskWriteLockedException e) {
                // IOManager does not allow createChannel to declare IOException. Preserve the
                // I/O boundary here and unwrap the retriable Fluss exception in lookup().
                throw new UncheckedIOException(new IOException(e));
            }
            lookupFileDownloadCount.incrementAndGet();
            return delegate.createChannel(prefix);
        }

        @Override
        public String[] tempDirs() {
            return delegate.tempDirs();
        }

        // Paimon 2.0 adds this method to IOManager. Do not add @Override so the same source also
        // compiles against Paimon 1.3. This lookuper configures exactly one temporary directory.
        public String pickTempDir() {
            return delegate.tempDirs()[0];
        }

        @Override
        public FileIOChannel.Enumerator createChannelEnumerator() {
            return delegate.createChannelEnumerator();
        }

        @Override
        public BufferFileWriter createBufferFileWriter(FileIOChannel.ID channelID)
                throws IOException {
            return delegate.createBufferFileWriter(channelID);
        }

        @Override
        public BufferFileReader createBufferFileReader(FileIOChannel.ID channelID)
                throws IOException {
            return delegate.createBufferFileReader(channelID);
        }

        @Override
        public void close() throws Exception {
            delegate.close();
        }
    }
}
