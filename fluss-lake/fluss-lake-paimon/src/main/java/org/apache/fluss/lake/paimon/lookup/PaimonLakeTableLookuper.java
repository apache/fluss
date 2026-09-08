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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
 * <p>An explicit refresh request is applied during the next lookup initialization. It rescans every
 * registered partition-bucket and updates its file set in place. Paimon keeps lookup files for data
 * files that remain active and lazily downloads lookup files only for newly added data files.
 *
 * <p>Calls to {@link LocalTableQuery#lookup} are serialized because Paimon 2.0 shares mutable
 * lookup-store comparator state across local lookup files.
 *
 * <p>Close is expected only after the owner has drained active lookups. It is synchronized with
 * lazy initialization and file-set updates, but deliberately does not add a lifecycle lock to every
 * lookup.
 */
public class PaimonLakeTableLookuper implements LakeTableLookuper {

    private final Configuration paimonConfig;
    private final TablePath tablePath;
    private final String ioTmpDir;
    private final TableConfig tableConfig;
    private final long lookupCacheMaxDiskBytes;
    private final Runnable diskWriteGuard;

    private final ThreadLocal<Boolean> lookupFileDownloaded;
    private final Object paimonLookupLock;
    // Guards lazy initialization, close, and registered file-set updates.
    private final Object lookupStateLock;
    private final Map<PaimonPartitionBucket, List<DataFileMeta>> registeredFiles;
    // Remains non-zero until a refresh completes without observing another request.
    private final AtomicLong pendingRefreshRequests;

    private @Nullable Catalog catalog;
    private @Nullable FileStoreTable fileStoreTable;
    private @Nullable IOManager ioManager;
    private @Nullable List<String> trimmedPrimaryKeys;

    // CompactedKeyDecoder contains immutable type metadata and creates all decode state per
    // invocation, so it can be shared by concurrent lookups.
    private @Nullable CompactedKeyDecoder compactedKeyDecoder;

    private volatile @Nullable LocalTableQuery localTableQuery;
    // Guarded by lookupStateLock.
    private volatile boolean closed;

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
        this.lookupFileDownloaded = new ThreadLocal<>();
        this.paimonLookupLock = new Object();
        this.lookupStateLock = new Object();
        this.registeredFiles = new ConcurrentHashMap<>();
        this.pendingRefreshRequests = new AtomicLong();
    }

    @Override
    public @Nullable byte[] lookup(byte[] key, LookupContext context) throws Exception {
        checkNotNull(key, "key must not be null.");
        checkNotNull(context, "context must not be null.");
        checkNotClosed();
        initialize(context.valueRowType());

        try (TrackingMetrics ignored = new TrackingMetrics(lookupFileDownloaded, context)) {
            return lookupInternal(key, context);
        } catch (Exception e) {
            DiskWriteLockedException diskWriteLockedException =
                    ExceptionUtils.findThrowable(e, DiskWriteLockedException.class).orElse(null);
            if (diskWriteLockedException != null) {
                throw diskWriteLockedException;
            }
            throw e;
        }
    }

    @Override
    public void requestRefresh() {
        checkNotClosed();
        pendingRefreshRequests.incrementAndGet();
    }

    @Override
    public void close() {
        synchronized (lookupStateLock) {
            if (closed) {
                return;
            }
            closed = true;
            IOUtils.closeQuietly(localTableQuery, "Paimon lookup engine");
            IOUtils.closeQuietly(ioManager, "Paimon lookup IO manager");
            IOUtils.closeQuietly(catalog, "Paimon catalog");
            registeredFiles.clear();
            localTableQuery = null;
            compactedKeyDecoder = null;
            trimmedPrimaryKeys = null;
            ioManager = null;
            fileStoreTable = null;
            catalog = null;
        }
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("Paimon lake table lookuper has been closed.");
        }
    }

    private void initialize(RowType valueRowType) throws Exception {
        if (localTableQuery == null || pendingRefreshRequests.get() != 0) {
            synchronized (lookupStateLock) {
                if (localTableQuery == null) {
                    initializeLookupState(valueRowType);
                }
                long observedRefreshRequests;
                while ((observedRefreshRequests = pendingRefreshRequests.get()) != 0) {
                    // A single scan handles all currently pending refresh requests.
                    refreshFilesFromLatestSnapshot();
                    // Clear the whole observed count only if it remained unchanged. If another
                    // refresh request arrived during the scan, the count was incremented, the CAS
                    // fails, and the next loop iteration performs another scan for that new
                    // refresh request.
                    pendingRefreshRequests.compareAndSet(observedRefreshRequests, 0);
                }
            }
        }
    }

    private void initializeLookupState(RowType valueRowType) throws Exception {
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

            List<String> newTrimmedPrimaryKeys =
                    Collections.unmodifiableList(
                            new ArrayList<>(newFileStoreTable.schema().trimmedPrimaryKeys()));
            CompactedKeyDecoder newCompactedKeyDecoder = null;

            // Legacy/v1 tables and v2 tables with a default bucket key already encode Fluss
            // lookup keys with Paimon's key encoder. Only v2 tables with a non-default bucket
            // key use the compacted key encoding and need conversion before querying Paimon.
            if (tableConfig.getKvFormatVersion().orElse(1) == KV_FORMAT_VERSION_2
                    && !newFileStoreTable.schema().bucketKeys().equals(newTrimmedPrimaryKeys)) {
                // Kv-format-v2 tables with a non-default bucket key store Fluss keys using the
                // compacted encoding to support prefix lookup. Paimon's LocalTableQuery expects
                // its own BinaryRow encoding, so convert the key at the lake lookup boundary.
                newCompactedKeyDecoder =
                        CompactedKeyDecoder.createKeyDecoder(valueRowType, newTrimmedPrimaryKeys);
            }

            newIOManager = createIOManager(ioTmpDir);
            newLocalTableQuery =
                    newFileStoreTable
                            .newLocalTableQuery()
                            .withValueProjection(businessFieldProjection(newFileStoreTable))
                            .withIOManager(newIOManager);

            catalog = newCatalog;
            fileStoreTable = newFileStoreTable;
            ioManager = newIOManager;
            trimmedPrimaryKeys = newTrimmedPrimaryKeys;
            compactedKeyDecoder = newCompactedKeyDecoder;
            // Keep this volatile write last to publish all initialized fields together.
            localTableQuery = newLocalTableQuery;
            initialized = true;
        } finally {
            if (!initialized) {
                IOUtils.closeQuietly(newLocalTableQuery, "Paimon local table query");
                IOUtils.closeQuietly(newIOManager, "Paimon lookup IO manager");
                IOUtils.closeQuietly(newCatalog, "Paimon catalog");
            }
        }
    }

    private void refreshFilesFromLatestSnapshot() {
        Map<PaimonPartitionBucket, List<DataFileMeta>> filesBeforeRefresh =
                new LinkedHashMap<>(registeredFiles);
        Map<PaimonPartitionBucket, List<DataFileMeta>> latestFiles =
                scanDataFiles(filesBeforeRefresh.keySet());
        filesBeforeRefresh.forEach(
                (partitionBucket, files) ->
                        refreshFiles(
                                partitionBucket, files, () -> latestFiles.get(partitionBucket)));
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

    private org.apache.paimon.data.BinaryRow getPartition(LookupContext context) {
        // Both generated helpers reuse mutable writers or projections, so keep them confined to
        // this lookup call.
        RowPartitionKeyExtractor partitionKeyExtractor =
                new RowPartitionKeyExtractor(fileStoreTable.schema());
        org.apache.paimon.data.BinaryRow partition =
                toPaimonPartition(
                                context.partitionSpec(),
                                context.valueRowType(),
                                fileStoreTable.schema().logicalRowType(),
                                partitionKeyExtractor::partition)
                        .copy();
        return partition;
    }

    private org.apache.paimon.data.BinaryRow getKey(byte[] key, LookupContext context) {
        byte[] paimonKey = key;
        if (compactedKeyDecoder != null) {
            InternalRow decodedKey = compactedKeyDecoder.decodeKey(key);
            RowType keyRowType = context.valueRowType().project(trimmedPrimaryKeys);
            PaimonKeyEncoder paimonKeyEncoder =
                    new PaimonKeyEncoder(keyRowType, trimmedPrimaryKeys);
            paimonKey = paimonKeyEncoder.encodeKey(decodedKey);
        }

        org.apache.paimon.data.BinaryRow keyRow =
                new org.apache.paimon.data.BinaryRow(trimmedPrimaryKeys.size());
        keyRow.pointTo(MemorySegment.wrap(paimonKey), 0, paimonKey.length);
        return keyRow;
    }

    private @Nullable byte[] lookupInternal(byte[] key, LookupContext context) {
        org.apache.paimon.data.InternalRow paimonRow;
        try {
            paimonRow =
                    lookupPaimon(getPartition(context), context.bucketId(), getKey(key, context));
        } catch (IOException e) {
            // Historical Paimon point lookup is part of the Fluss KV lookup path. Expose a
            // persistent I/O failure as a retriable KV error so the existing KV RPC retry
            // semantics can handle it consistently.
            throw new KvStorageException(
                    "Failed to lookup historical data from Paimon after refreshing files for "
                            + tablePath
                            + ".",
                    e);
        }
        if (paimonRow == null) {
            return null;
        }
        return encodeValue(paimonRow, context.schemaId(), context.valueRowType());
    }

    private @Nullable org.apache.paimon.data.InternalRow lookupPaimon(
            org.apache.paimon.data.BinaryRow partition,
            int bucket,
            org.apache.paimon.data.InternalRow key)
            throws IOException {
        PaimonPartitionBucket partitionBucket = new PaimonPartitionBucket(partition, bucket);
        List<DataFileMeta> filesBeforeLookup = getOrInitializeFiles(partitionBucket);
        try {
            return lookupLocalTable(partition, bucket, key);
        } catch (IOException firstError) {
            refreshFiles(partitionBucket, filesBeforeLookup, () -> scanDataFiles(partitionBucket));
            try {
                return lookupLocalTable(partition, bucket, key);
            } catch (IOException retryError) {
                retryError.addSuppressed(firstError);
                throw retryError;
            }
        }
    }

    private @Nullable org.apache.paimon.data.InternalRow lookupLocalTable(
            org.apache.paimon.data.BinaryRow partition,
            int bucket,
            org.apache.paimon.data.InternalRow key)
            throws IOException {
        // TODO: Remove this lock once https://github.com/apache/paimon/issues/9483 is fixed in the
        // Paimon version used by Fluss. If concurrent lookup is needed sooner, bring Paimon's
        // LocalTableQuery implementation into Fluss and make it thread-safe, following the approach
        // in https://github.com/apache/fluss/pull/4113.
        synchronized (paimonLookupLock) {
            return localTableQuery.lookup(partition, bucket, key);
        }
    }

    private List<DataFileMeta> getOrInitializeFiles(PaimonPartitionBucket partitionBucket) {
        List<DataFileMeta> files = registeredFiles.get(partitionBucket);
        if (files != null) {
            return files;
        }
        // Coordinate only first registration with bulk refresh; existing entries use the fast path.
        synchronized (lookupStateLock) {
            return registeredFiles.computeIfAbsent(
                    partitionBucket,
                    ignored -> {
                        List<DataFileMeta> latestFiles = scanDataFiles(partitionBucket);
                        return applyFileRefresh(
                                partitionBucket, Collections.emptyList(), latestFiles);
                    });
        }
    }

    private void refreshFiles(
            PaimonPartitionBucket partitionBucket,
            List<DataFileMeta> filesBeforeRefresh,
            Supplier<List<DataFileMeta>> latestFilesSupplier) {
        synchronized (lookupStateLock) {
            registeredFiles.compute(
                    partitionBucket,
                    (ignored, currentFiles) -> {
                        List<DataFileMeta> files =
                                checkNotNull(
                                        currentFiles,
                                        "Partition-bucket files must be initialized.");
                        // File lists are immutable and replaced on refresh. Identity equality means
                        // no other refresh has updated this partition-bucket since the caller
                        // captured it.
                        return files == filesBeforeRefresh
                                ? applyFileRefresh(
                                        partitionBucket,
                                        filesBeforeRefresh,
                                        latestFilesSupplier.get())
                                : files;
                    });
        }
    }

    private List<DataFileMeta> applyFileRefresh(
            PaimonPartitionBucket partitionBucket,
            List<DataFileMeta> filesBeforeRefresh,
            List<DataFileMeta> latestFiles) {
        org.apache.paimon.data.BinaryRow partition = partitionBucket.getPartition();
        int bucket = partitionBucket.getBucket();
        localTableQuery.refreshFiles(partition, bucket, filesBeforeRefresh, latestFiles);
        return latestFiles;
    }

    private List<DataFileMeta> scanDataFiles(PaimonPartitionBucket partitionBucket) {
        return scanDataFiles(Collections.singleton(partitionBucket)).get(partitionBucket);
    }

    private Map<PaimonPartitionBucket, List<DataFileMeta>> scanDataFiles(
            Set<PaimonPartitionBucket> partitionBuckets) {
        if (partitionBuckets.isEmpty()) {
            return Collections.emptyMap();
        }

        Set<org.apache.paimon.data.BinaryRow> partitions = new HashSet<>();
        Set<Integer> buckets = new HashSet<>();
        Map<PaimonPartitionBucket, LinkedHashMap<String, DataFileMeta>> dataFilesByPartitionBucket =
                new LinkedHashMap<>();
        for (PaimonPartitionBucket partitionBucket : partitionBuckets) {
            partitions.add(partitionBucket.getPartition());
            buckets.add(partitionBucket.getBucket());
            dataFilesByPartitionBucket.put(partitionBucket, new LinkedHashMap<>());
        }

        InnerTableScan tableScan =
                fileStoreTable
                        .newScan()
                        .withPartitionFilter(new ArrayList<>(partitions))
                        .withBucketFilter(buckets::contains);
        for (Split split : tableScan.plan().splits()) {
            if (split instanceof DataSplit) {
                DataSplit dataSplit = (DataSplit) split;
                LinkedHashMap<String, DataFileMeta> dataFilesByName =
                        dataFilesByPartitionBucket.get(
                                new PaimonPartitionBucket(
                                        dataSplit.partition(), dataSplit.bucket()));
                if (dataFilesByName != null) {
                    for (DataFileMeta file : dataSplit.dataFiles()) {
                        dataFilesByName.put(file.fileName(), file);
                    }
                }
            }
        }

        Map<PaimonPartitionBucket, List<DataFileMeta>> latestFiles = new LinkedHashMap<>();
        dataFilesByPartitionBucket.forEach(
                (partitionBucket, dataFilesByName) ->
                        latestFiles.put(
                                partitionBucket,
                                Collections.unmodifiableList(
                                        new ArrayList<>(dataFilesByName.values()))));
        return latestFiles;
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
            FileIOChannel.ID channel = delegate.createChannel(prefix);
            // Paimon creates lookup files synchronously in the lookup thread, so this marks only
            // the request that caused this channel to be created.
            if (lookupFileDownloaded.get() != null) {
                lookupFileDownloaded.set(true);
            }
            return channel;
        }

        @Override
        public String[] tempDirs() {
            return delegate.tempDirs();
        }

        @Override
        public String pickTempDir() {
            return delegate.pickTempDir();
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

    private static final class TrackingMetrics implements AutoCloseable {
        private final ThreadLocal<Boolean> lookupFileDownloaded;
        private final long startNanoTime;
        private final LookupContext context;

        private TrackingMetrics(ThreadLocal<Boolean> lookupFileDownloaded, LookupContext context) {
            this.lookupFileDownloaded = lookupFileDownloaded;
            this.lookupFileDownloaded.set(false);
            this.startNanoTime = System.nanoTime();
            this.context = context;
        }

        @Override
        public void close() throws Exception {
            boolean fileDownloaded = lookupFileDownloaded.get();
            lookupFileDownloaded.remove();
            context.lookupMetricRecorder()
                    .recordLookup(System.nanoTime() - startNanoTime, fileDownloaded);
        }
    }
}
