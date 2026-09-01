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

package org.apache.fluss.server.kv;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.compression.ArrowCompressionInfo;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.exception.KvStorageException;
import org.apache.fluss.exception.StorageBackpressureException;
import org.apache.fluss.memory.MemorySegmentPool;
import org.apache.fluss.metadata.ChangelogImage;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.row.arrow.ArrowWriterPool;
import org.apache.fluss.row.encode.KvValueLayout;
import org.apache.fluss.row.encode.ValueDecoder;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.server.kv.autoinc.AutoIncIDRange;
import org.apache.fluss.server.kv.autoinc.AutoIncrementManager;
import org.apache.fluss.server.kv.historical.HistoricalValueLookup;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.PreparedFlush;
import org.apache.fluss.server.kv.rocksdb.RocksDBKv;
import org.apache.fluss.server.kv.rocksdb.RocksDBKvBuilder;
import org.apache.fluss.server.kv.rocksdb.RocksDBResourceContainer;
import org.apache.fluss.server.kv.rocksdb.RocksDBStatistics;
import org.apache.fluss.server.kv.rowmerger.RowMerger;
import org.apache.fluss.server.kv.scan.OpenScanResult;
import org.apache.fluss.server.kv.scan.ScannerContext;
import org.apache.fluss.server.kv.snapshot.KvFileHandleAndLocalPath;
import org.apache.fluss.server.kv.snapshot.KvSnapshotDataUploader;
import org.apache.fluss.server.kv.snapshot.RocksIncrementalSnapshot;
import org.apache.fluss.server.kv.snapshot.TabletState;
import org.apache.fluss.server.log.LogAppendInfo;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.server.utils.FatalErrorHandler;
import org.apache.fluss.server.utils.ResourceGuard;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.utils.ByteArraySlice;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.IOUtils;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.SystemClock;

import org.rocksdb.AbstractCompactionFilter;
import org.rocksdb.AbstractCompactionFilterFactory;
import org.rocksdb.Cache;
import org.rocksdb.RateLimiter;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksIterator;
import org.rocksdb.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.fluss.server.kv.KvStateAccessor.HISTORICAL_TOMBSTONE;
import static org.apache.fluss.utils.PartitionUtils.HISTORICAL_PARTITION_VALUE;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;
import static org.apache.fluss.utils.concurrent.LockUtils.inReadLock;
import static org.apache.fluss.utils.concurrent.LockUtils.inWriteLock;

/** A kv tablet which presents a unified view of kv storage. */
@ThreadSafe
public final class KvTablet {
    private static final Logger LOG = LoggerFactory.getLogger(KvTablet.class);

    private static final long MIN_FLUSH_RETRY_DELAY_MS = 100L;

    private static final long MAX_FLUSH_RETRY_DELAY_MS = 1_000L;

    /**
     * Number of backoff doublings after which the retry delay saturates at {@link
     * #MAX_FLUSH_RETRY_DELAY_MS}. Derived from the delay bounds so that {@code
     * MIN_FLUSH_RETRY_DELAY_MS << shift} can never overflow: the shifted value is bounded by {@code
     * 2 * MAX_FLUSH_RETRY_DELAY_MS}.
     */
    private static final int MAX_FLUSH_RETRY_BACKOFF_SHIFT =
            64 - Long.numberOfLeadingZeros(MAX_FLUSH_RETRY_DELAY_MS / MIN_FLUSH_RETRY_DELAY_MS);

    private static final long ROW_COUNT_DISABLED = -1;

    /**
     * Max records per native write of the asynchronous flush; mirrors the batching capacity of
     * {@code RocksDBWriteBatchWrapper} (hundreds of keys per write batch is RocksDB best practice).
     * Together with {@code writeBatchSize} this bounds one atomic native write.
     */
    private static final int MAX_RECORDS_PER_NATIVE_WRITE = 500;

    private final PhysicalTablePath physicalPath;
    private final TableBucket tableBucket;
    private final boolean historicalPartition;

    private final LogTablet logTablet;

    private final File kvTabletDir;
    private final long writeBatchSize;
    private final RocksDBKv rocksDBKv;
    private final KvPreWriteBuffer kvPreWriteBuffer;
    private final KvStateAccessor kvStateAccessor;
    private final KvWriteProcessor kvWriteProcessor;
    private final TabletServerMetricGroup serverMetricGroup;
    private final KvFlushScheduler kvFlushScheduler;
    private final boolean closeFlushScheduler;

    // A lock that guards all modifications to the kv.
    private final ReadWriteLock kvLock = new ReentrantReadWriteLock();
    private final KvValueLayout kvValueLayout;
    private final ValueEncoder valueEncoder;
    @Nullable private final RowTtlTimestampProvider rowTtlTimestampProvider;
    private final boolean rowTtlEnabled;
    private final AutoIncrementManager autoIncrementManager;

    // RocksDB statistics accessor for this tablet
    @Nullable private final RocksDBStatistics rocksDBStatistics;

    /**
     * The kv data in pre-write buffer whose log offset is less than the flushedLogOffset has been
     * flushed into kv.
     */
    private volatile long flushedLogOffset = 0;

    @GuardedBy("kvLock")
    private FlushState flushState = FlushState.IDLE;

    @GuardedBy("kvLock")
    private long requestedFlushOffset = 0;

    @GuardedBy("kvLock")
    private int flushRetryAttempts = 0;

    /** Invoked after each flush that made progress; set at construction time by the owner. */
    private volatile @Nullable Runnable flushCompleteListener;

    private volatile @Nullable FatalErrorHandler asyncFatalErrorHandler;

    private volatile long rowCount;

    @GuardedBy("kvLock")
    private volatile boolean isClosed = false;

    private KvTablet(
            PhysicalTablePath physicalPath,
            TableBucket tableBucket,
            LogTablet logTablet,
            File kvTabletDir,
            TabletServerMetricGroup serverMetricGroup,
            RocksDBKv rocksDBKv,
            long writeBatchSize,
            BufferAllocator arrowBufferAllocator,
            MemorySegmentPool memorySegmentPool,
            KvFormat kvFormat,
            RowMerger rowMerger,
            ArrowCompressionInfo arrowCompressionInfo,
            SchemaGetter schemaGetter,
            ChangelogImage changelogImage,
            KvValueLayout kvValueLayout,
            ValueEncoder valueEncoder,
            ValueDecoder valueDecoder,
            @Nullable RocksDBStatistics rocksDBStatistics,
            KvFlushScheduler kvFlushScheduler,
            boolean closeFlushScheduler,
            @Nullable Runnable flushCompleteListener,
            AutoIncrementManager autoIncrementManager,
            @Nullable RowTtlTimestampProvider rowTtlTimestampProvider,
            Clock clock,
            boolean rowTtlEnabled) {
        this.physicalPath = physicalPath;
        this.tableBucket = tableBucket;
        this.historicalPartition =
                HISTORICAL_PARTITION_VALUE.equals(physicalPath.getPartitionName());
        this.logTablet = logTablet;
        this.kvTabletDir = kvTabletDir;
        this.rocksDBKv = rocksDBKv;
        this.writeBatchSize = writeBatchSize;
        this.serverMetricGroup = serverMetricGroup;
        this.kvFlushScheduler = kvFlushScheduler;
        this.closeFlushScheduler = closeFlushScheduler;
        this.kvPreWriteBuffer = new KvPreWriteBuffer(serverMetricGroup);
        this.kvStateAccessor =
                new KvStateAccessor(kvPreWriteBuffer, rocksDBKv, historicalPartition);
        this.kvValueLayout = kvValueLayout;
        this.valueEncoder = valueEncoder;
        this.rowTtlTimestampProvider = rowTtlTimestampProvider;
        this.rowTtlEnabled = rowTtlEnabled;
        this.kvWriteProcessor =
                new KvWriteProcessor(
                        tableBucket,
                        logTablet,
                        new ArrowWriterPool(arrowBufferAllocator),
                        memorySegmentPool,
                        kvFormat,
                        rowMerger,
                        arrowCompressionInfo,
                        schemaGetter,
                        changelogImage,
                        autoIncrementManager,
                        valueEncoder,
                        valueDecoder,
                        rowTtlTimestampProvider,
                        clock);
        this.rocksDBStatistics = rocksDBStatistics;
        this.autoIncrementManager = autoIncrementManager;
        this.flushCompleteListener = flushCompleteListener;
        // TODO: Support row count for historical partitions.
        // Historical state only contains the WAL tail that has not been tiered to lake, so it
        // cannot maintain a table-level row count.
        this.rowCount =
                historicalPartition || changelogImage == ChangelogImage.WAL || rowTtlEnabled
                        ? ROW_COUNT_DISABLED
                        : 0L;
    }

    /**
     * Creates a kv tablet with a dedicated {@link KvFlushScheduler} that is closed together with
     * the tablet. Production code must use {@link #create(PhysicalTablePath, TableBucket,
     * LogTablet, File, Configuration, TabletServerMetricGroup, BufferAllocator, MemorySegmentPool,
     * KvFormat, RowMerger, ArrowCompressionInfo, SchemaGetter, ChangelogImage, RateLimiter, Cache,
     * KvFlushScheduler, Runnable, AutoIncrementManager, Clock, TableConfig)} with the shared
     * scheduler owned by {@link KvManager}.
     */
    @VisibleForTesting
    public static KvTablet create(
            PhysicalTablePath tablePath,
            TableBucket tableBucket,
            LogTablet logTablet,
            File kvTabletDir,
            Configuration serverConf,
            TabletServerMetricGroup serverMetricGroup,
            BufferAllocator arrowBufferAllocator,
            MemorySegmentPool memorySegmentPool,
            KvFormat kvFormat,
            RowMerger rowMerger,
            ArrowCompressionInfo arrowCompressionInfo,
            SchemaGetter schemaGetter,
            ChangelogImage changelogImage,
            RateLimiter sharedRateLimiter,
            AutoIncrementManager autoIncrementManager)
            throws IOException {
        return create(
                tablePath,
                tableBucket,
                logTablet,
                kvTabletDir,
                serverConf,
                serverMetricGroup,
                arrowBufferAllocator,
                memorySegmentPool,
                kvFormat,
                rowMerger,
                arrowCompressionInfo,
                schemaGetter,
                changelogImage,
                sharedRateLimiter,
                null,
                new KvFlushScheduler(serverConf),
                true,
                null,
                autoIncrementManager,
                SystemClock.getInstance(),
                new TableConfig(new Configuration()));
    }

    public static KvTablet create(
            PhysicalTablePath tablePath,
            TableBucket tableBucket,
            LogTablet logTablet,
            File kvTabletDir,
            Configuration serverConf,
            TabletServerMetricGroup serverMetricGroup,
            BufferAllocator arrowBufferAllocator,
            MemorySegmentPool memorySegmentPool,
            KvFormat kvFormat,
            RowMerger rowMerger,
            ArrowCompressionInfo arrowCompressionInfo,
            SchemaGetter schemaGetter,
            ChangelogImage changelogImage,
            RateLimiter sharedRateLimiter,
            @Nullable Cache sharedBlockCache,
            KvFlushScheduler kvFlushScheduler,
            @Nullable Runnable flushCompleteListener,
            AutoIncrementManager autoIncrementManager,
            Clock clock,
            TableConfig tableConfig)
            throws IOException {
        return create(
                tablePath,
                tableBucket,
                logTablet,
                kvTabletDir,
                serverConf,
                serverMetricGroup,
                arrowBufferAllocator,
                memorySegmentPool,
                kvFormat,
                rowMerger,
                arrowCompressionInfo,
                schemaGetter,
                changelogImage,
                sharedRateLimiter,
                sharedBlockCache,
                kvFlushScheduler,
                false,
                flushCompleteListener,
                autoIncrementManager,
                clock,
                tableConfig);
    }

    private static KvTablet create(
            PhysicalTablePath tablePath,
            TableBucket tableBucket,
            LogTablet logTablet,
            File kvTabletDir,
            Configuration serverConf,
            TabletServerMetricGroup serverMetricGroup,
            BufferAllocator arrowBufferAllocator,
            MemorySegmentPool memorySegmentPool,
            KvFormat kvFormat,
            RowMerger rowMerger,
            ArrowCompressionInfo arrowCompressionInfo,
            SchemaGetter schemaGetter,
            ChangelogImage changelogImage,
            RateLimiter sharedRateLimiter,
            @Nullable Cache sharedBlockCache,
            KvFlushScheduler kvFlushScheduler,
            boolean closeFlushScheduler,
            @Nullable Runnable flushCompleteListener,
            AutoIncrementManager autoIncrementManager,
            Clock clock,
            TableConfig tableConfig)
            throws IOException {
        checkNotNull(tableConfig, "tableConfig must not be null.");
        Optional<Duration> rowTtl = tableConfig.getKvTTL();
        KvValueLayout kvValueLayout = KvValueLayout.fromTableConfig(tableConfig);
        @Nullable
        RowTtlTimestampProvider rowTtlTimestampProvider =
                kvValueLayout.hasValueTag()
                        ? RowTtlTimestampProvider.create(
                                tableConfig, schemaGetter, ZoneId.systemDefault())
                        : null;
        ValueEncoder valueEncoder =
                rowTtlTimestampProvider == null
                        ? ValueEncoder.forLayout(kvValueLayout)
                        : ValueEncoder.forLayout(kvValueLayout, rowTtlTimestampProvider);
        ValueDecoder valueDecoder = new ValueDecoder(schemaGetter, kvFormat, kvValueLayout);
        @Nullable
        AbstractCompactionFilterFactory<? extends AbstractCompactionFilter<?>>
                compactionFilterFactory =
                        rowTtl.isPresent()
                                ? RowTtlCompactionFilterFactory.create(
                                        kvValueLayout, rowTtl.get(), clock)
                                : null;
        RocksDBKv kv =
                buildRocksDBKv(
                        serverConf,
                        kvTabletDir,
                        sharedRateLimiter,
                        sharedBlockCache,
                        compactionFilterFactory);

        // Create RocksDB statistics accessor (will be registered to TableMetricGroup by Replica)
        // Pass ResourceGuard to ensure thread-safe access during concurrent close operations
        // Pass ColumnFamilyHandle for column family specific properties like num-files-at-level0
        // Pass Cache for accurate block cache memory tracking
        RocksDBStatistics rocksDBStatistics =
                new RocksDBStatistics(
                        kv.getDb(),
                        kv.getStatistics(),
                        kv.getResourceGuard(),
                        kv.getDefaultColumnFamilyHandle(),
                        sharedBlockCache == null ? kv.getBlockCache() : null);

        return new KvTablet(
                tablePath,
                tableBucket,
                logTablet,
                kvTabletDir,
                serverMetricGroup,
                kv,
                serverConf.get(ConfigOptions.KV_WRITE_BATCH_SIZE).getBytes(),
                arrowBufferAllocator,
                memorySegmentPool,
                kvFormat,
                rowMerger,
                arrowCompressionInfo,
                schemaGetter,
                changelogImage,
                kvValueLayout,
                valueEncoder,
                valueDecoder,
                rocksDBStatistics,
                kvFlushScheduler,
                closeFlushScheduler,
                flushCompleteListener,
                autoIncrementManager,
                rowTtlTimestampProvider,
                clock,
                rowTtl.isPresent());
    }

    public static KvTablet create(
            PhysicalTablePath tablePath,
            TableBucket tableBucket,
            LogTablet logTablet,
            File kvTabletDir,
            Configuration serverConf,
            TabletServerMetricGroup serverMetricGroup,
            BufferAllocator arrowBufferAllocator,
            MemorySegmentPool memorySegmentPool,
            KvFormat kvFormat,
            RowMerger rowMerger,
            ArrowCompressionInfo arrowCompressionInfo,
            SchemaGetter schemaGetter,
            ChangelogImage changelogImage,
            RateLimiter sharedRateLimiter,
            AutoIncrementManager autoIncrementManager,
            Clock clock,
            TableConfig tableConfig)
            throws IOException {
        return create(
                tablePath,
                tableBucket,
                logTablet,
                kvTabletDir,
                serverConf,
                serverMetricGroup,
                arrowBufferAllocator,
                memorySegmentPool,
                kvFormat,
                rowMerger,
                arrowCompressionInfo,
                schemaGetter,
                changelogImage,
                sharedRateLimiter,
                null,
                new KvFlushScheduler(serverConf),
                true,
                null,
                autoIncrementManager,
                clock,
                tableConfig);
    }

    private static RocksDBKv buildRocksDBKv(
            Configuration configuration,
            File kvDir,
            RateLimiter sharedRateLimiter,
            @Nullable Cache sharedBlockCache,
            @Nullable
                    AbstractCompactionFilterFactory<? extends AbstractCompactionFilter<?>>
                            compactionFilterFactory)
            throws IOException {
        @Nullable RocksDBResourceContainer rocksDBResourceContainer = null;
        boolean resourcesOwnedByBuilder = false;
        try {
            rocksDBResourceContainer =
                    new RocksDBResourceContainer(
                            configuration, kvDir, true, sharedRateLimiter, sharedBlockCache);
            RocksDBKvBuilder rocksDBKvBuilder =
                    new RocksDBKvBuilder(
                                    kvDir,
                                    rocksDBResourceContainer,
                                    rocksDBResourceContainer.getColumnOptions())
                            .setFlussL0SlowdownTrigger(
                                    configuration.get(
                                            ConfigOptions.KV_BACKPRESSURE_L0_SLOWDOWN_TRIGGER));
            if (compactionFilterFactory != null) {
                rocksDBKvBuilder.setCompactionFilterFactory(compactionFilterFactory);
            }
            resourcesOwnedByBuilder = true;
            return rocksDBKvBuilder.build();
        } finally {
            if (!resourcesOwnedByBuilder) {
                IOUtils.closeQuietly(rocksDBResourceContainer);
                IOUtils.closeQuietly(compactionFilterFactory);
            }
        }
    }

    ValueEncoder getValueEncoder() {
        return valueEncoder;
    }

    @Nullable
    RowTtlTimestampProvider getRowTtlTimestampProvider() {
        return rowTtlTimestampProvider;
    }

    /** Returns the physical value layout used by this tablet. */
    public KvValueLayout getKvValueLayout() {
        return kvValueLayout;
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }

    public TablePath getTablePath() {
        return physicalPath.getTablePath();
    }

    public long getAutoIncrementCacheSize() {
        return autoIncrementManager.getAutoIncrementCacheSize();
    }

    public void updateAutoIncrementIDRange(AutoIncIDRange newRange) {
        autoIncrementManager.updateIDRange(newRange);
    }

    @Nullable
    public String getPartitionName() {
        return physicalPath.getPartitionName();
    }

    public File getKvTabletDir() {
        return kvTabletDir;
    }

    /** Returns the total size in bytes of the live RocksDB SST files. */
    public long liveSstFilesSize() {
        return rocksDBKv.liveSstFilesSize();
    }

    /**
     * Get RocksDB statistics accessor for this tablet.
     *
     * @return the RocksDB statistics accessor, or null if not available
     */
    @Nullable
    public RocksDBStatistics getRocksDBStatistics() {
        return rocksDBStatistics;
    }

    void setFlushedLogOffset(long flushedLogOffset) {
        this.flushedLogOffset = flushedLogOffset;
    }

    void setRowCount(long rowCount) {
        if (this.rowCount != ROW_COUNT_DISABLED) {
            this.rowCount = rowCount;
        }
    }

    // row_count is volatile, so it's safe to read without lock
    public long getRowCount() {
        if (rowCount == ROW_COUNT_DISABLED) {
            if (rowTtlEnabled) {
                throw new InvalidTableException(
                        String.format(
                                "Row count is disabled for this table '%s' because row TTL cleanup does not maintain exact row count.",
                                getTablePath()));
            }
            throw new InvalidTableException(
                    String.format(
                            "Row count is disabled for this table '%s'. This usually happens when the table is"
                                    + " created before v0.9 or the changelog image is set to WAL, "
                                    + "as maintaining row count in WAL mode is costly and not necessary for most use cases. "
                                    + "If you want to enable row count, please set changelog image to FULL.",
                            getTablePath()));
        }
        return rowCount;
    }

    /**
     * Get the current state of the tablet, including the log offset, row count and auto-increment
     * ID range. This is used for snapshot and recovery to capture the state of the tablet at a
     * specific log offset.
     *
     * <p>Note: this method must be called under the kvLock to ensure the consistency between the
     * returned state and the log offset.
     */
    @GuardedBy("kvLock")
    public TabletState getTabletState() {
        return new TabletState(
                flushedLogOffset,
                rowCount == ROW_COUNT_DISABLED ? null : rowCount,
                autoIncrementManager.getCurrentIDRanges());
    }

    /**
     * Put the KvRecordBatch into the kv storage with default DEFAULT mode.
     *
     * <p>This is a convenience method that calls {@link #putAsLeader(KvRecordBatch, int[],
     * MergeMode)} with {@link MergeMode#DEFAULT}.
     *
     * @param kvRecords the kv records to put into
     * @param targetColumns the target columns to put, null if put all columns
     */
    public LogAppendInfo putAsLeader(KvRecordBatch kvRecords, @Nullable int[] targetColumns)
            throws Exception {
        return putAsLeader(kvRecords, targetColumns, MergeMode.DEFAULT);
    }

    /**
     * Put the KvRecordBatch into the kv storage, and return the appended wal log info.
     *
     * <p>Schema Evolution Handling:
     *
     * <p>We don't allow shema of input kv records to be larger than the latest schema id known by
     * the tablet. Besides, we currently only support ADD COLUMN LAST operation, so the input row or
     * old row must have same or fewer columns than latest schema. This helps to simplify the schema
     * change handling.
     *
     * <p>1. We write the kv records into KvStore without converting it into latest schema for
     * performance consideration. We have mechanisms that writer client dynamically use latest
     * schema for writing records.
     *
     * <p>2. We always use the latest schema for writing WAL logs, because it anyway happens
     * deserialization&serialization to convert the compacted format into Arrow format.
     *
     * @param kvRecords the kv records to put into
     * @param targetColumns the target columns to put, null if put all columns
     * @param mergeMode the merge mode (DEFAULT or OVERWRITE)
     */
    public LogAppendInfo putAsLeader(
            KvRecordBatch kvRecords, @Nullable int[] targetColumns, MergeMode mergeMode)
            throws Exception {
        checkState(
                !historicalPartition,
                "putAsLeader is not supported for historical KV tablet %s",
                tableBucket);
        return putAsLeader(kvRecords, targetColumns, mergeMode, null, null);
    }

    /**
     * Puts records for one original partition into this historical KV tablet.
     *
     * <p>The original partition name namespaces the physical primary keys because one historical
     * bucket can contain records from multiple original partitions. The supplied fallback may only
     * read lake results already resolved for this request; it must not perform lake I/O while the
     * tablet lock is held.
     */
    public LogAppendInfo putHistoricalAsLeader(
            KvRecordBatch kvRecords,
            @Nullable int[] targetColumns,
            MergeMode mergeMode,
            String originalPartitionName,
            HistoricalValueLookup memoizedLakeLookup)
            throws Exception {
        checkState(historicalPartition, "%s is not a historical KV tablet", tableBucket);
        return putAsLeader(
                kvRecords,
                targetColumns,
                mergeMode,
                checkNotNull(originalPartitionName, "originalPartitionName must not be null"),
                checkNotNull(memoizedLakeLookup, "memoizedLakeLookup must not be null"));
    }

    /**
     * Finds keys whose historical write requires an old value that is absent from local state.
     *
     * <p>This method only reads KV entries and uses the tablet read lock. Lake I/O must be
     * performed by the caller after this method releases the tablet lock.
     */
    public List<byte[]> findKeysRequiringLakeLookup(
            KvRecordBatch kvRecords,
            @Nullable int[] targetColumns,
            MergeMode mergeMode,
            String originalPartitionName)
            throws Exception {
        checkState(historicalPartition, "%s is not a historical KV tablet", tableBucket);
        return inReadLock(
                kvLock,
                () -> {
                    rocksDBKv.checkIfRocksDBClosed();
                    return kvWriteProcessor.findKeysRequiringLakeLookup(
                            kvRecords,
                            targetColumns,
                            mergeMode,
                            kvStateAccessor,
                            checkNotNull(
                                    originalPartitionName,
                                    "originalPartitionName must not be null"));
                });
    }

    private LogAppendInfo putAsLeader(
            KvRecordBatch kvRecords,
            @Nullable int[] targetColumns,
            MergeMode mergeMode,
            @Nullable String originalPartitionName,
            @Nullable HistoricalValueLookup memoizedLakeLookup)
            throws Exception {
        return inWriteLock(
                kvLock,
                () -> {
                    rocksDBKv.checkIfRocksDBClosed();

                    long pendingFlushBytesAfterWrite =
                            kvPreWriteBuffer.pendingFlushBytes() + kvRecords.sizeInBytes();

                    // Write-path admission gate: reject the request if accepting this batch would
                    // push the buffered KV view beyond RocksDB's safe write budget.
                    if (rocksDBKv.wouldExceedFlushBudget(pendingFlushBytesAfterWrite)) {
                        requestFlushInternal(
                                Math.max(requestedFlushOffset, logTablet.getHighWatermark()));
                        throw new StorageBackpressureException(
                                String.format(
                                        "Write rejected for %s: flush budget exceeded "
                                                + "(storage pressure or buffer size limit reached). "
                                                + "Retry after backoff.",
                                        tableBucket));
                    }

                    return kvWriteProcessor.putAsLeader(
                            kvRecords,
                            targetColumns,
                            mergeMode,
                            kvStateAccessor,
                            originalPartitionName,
                            memoizedLakeLookup);
                });
    }

    @VisibleForTesting
    long localLogEndOffset() {
        return logTablet.localLogEndOffset();
    }

    public void requestFlush(long exclusiveUpToLogOffset, FatalErrorHandler fatalErrorHandler) {
        asyncFatalErrorHandler = fatalErrorHandler;
        inWriteLock(kvLock, () -> requestFlushInternal(exclusiveUpToLogOffset));
    }

    /** Detaches or replaces the flush-complete listener set at construction time. */
    @VisibleForTesting
    public void setFlushCompleteListener(@Nullable Runnable flushCompleteListener) {
        this.flushCompleteListener = flushCompleteListener;
    }

    public long getFlushedLogOffset() {
        return flushedLogOffset;
    }

    @VisibleForTesting
    FlushState getFlushState() {
        return inReadLock(kvLock, () -> flushState);
    }

    @VisibleForTesting
    void setFlushState(FlushState state) {
        inWriteLock(kvLock, () -> flushState = state);
    }

    @GuardedBy("kvLock")
    private void requestFlushInternal(long exclusiveUpToLogOffset) {
        if (isClosed || exclusiveUpToLogOffset <= flushedLogOffset) {
            return;
        }
        if (exclusiveUpToLogOffset > requestedFlushOffset) {
            requestedFlushOffset = exclusiveUpToLogOffset;
        }
        if (flushState == FlushState.IDLE) {
            transitionFlushState(FlushState.IDLE, FlushState.QUEUED);
            kvFlushScheduler.enqueue(this);
        }
    }

    void requestFlushRetry() {
        inWriteLock(
                kvLock,
                () -> {
                    if (!isClosed && flushState == FlushState.STORAGE_BLOCKED) {
                        transitionFlushState(FlushState.STORAGE_BLOCKED, FlushState.QUEUED);
                        kvFlushScheduler.enqueue(this);
                    }
                });
    }

    void runScheduledFlush() {
        if (!tryAcquireScheduledFlush()) {
            return;
        }
        // Each run is bounded to the flush target captured at prepare time: work requested after
        // that point is handed to a freshly scheduled run (see completeScheduledFlush) so that
        // flush completion, and thus high watermark advancement, is published per bounded
        // target instead of chasing an ever-increasing requestedFlushOffset within one run.
        long flushedOffsetBefore = flushedLogOffset;
        try {
            // The whole prepare -> write -> complete sequence runs under the kvLock write lock,
            // so snapshots, scans and puts can only observe states where the RocksDB content
            // matches flushedLogOffset/rowCount. The RocksDB lease is acquired strictly inside
            // kvLock, keeping the lock order kvLock -> lease on every path.
            inWriteLock(kvLock, this::doScheduledFlush);
        } catch (StorageBackpressureException e) {
            delayScheduledFlush(e);
        } catch (Throwable t) {
            failScheduledFlush(t);
        } finally {
            // Publish progress (including the completed prefix of a partially rejected run) so
            // the high watermark can advance. Runs outside kvLock because the listener acquires
            // the replica's leaderIsrUpdateLock while the write path acquires
            // leaderIsrUpdateLock -> kvLock; invoking it under kvLock would invert that order.
            if (flushedLogOffset > flushedOffsetBefore) {
                notifyFlushComplete();
            }
        }
    }

    @GuardedBy("kvLock")
    private void doScheduledFlush() throws Exception {
        PreparedFlush preparedFlush = prepareScheduledFlush();
        if (preparedFlush == null) {
            return;
        }
        if (!preparedFlush.isEmpty()) {
            writePreparedFlush(preparedFlush);
        } else {
            // The empty flush already advanced flushedLogOffset in prepare; completing it here
            // only resets the retry backoff.
            completeFlushedSegment(preparedFlush);
        }
        finishScheduledFlush();
    }

    private boolean tryAcquireScheduledFlush() {
        return inWriteLock(
                kvLock,
                () -> {
                    if (isClosed || flushState != FlushState.QUEUED) {
                        return false;
                    }
                    transitionFlushState(FlushState.QUEUED, FlushState.RUNNING);
                    return true;
                });
    }

    @GuardedBy("kvLock")
    private @Nullable PreparedFlush prepareScheduledFlush() {
        if (isClosed) {
            // close() already forced the state machine to its terminal IDLE state.
            return null;
        }
        long targetOffset = requestedFlushOffset;
        if (targetOffset <= flushedLogOffset) {
            transitionFlushState(FlushState.RUNNING, FlushState.IDLE);
            return null;
        }
        PreparedFlush preparedFlush;
        try {
            preparedFlush = kvPreWriteBuffer.prepareFlush(targetOffset);
        } catch (IllegalStateException e) {
            // Orphaned PREPARED entries from a previous incomplete flush cycle. Structurally
            // unreachable now that every flush run completes or aborts all its prepared entries
            // before releasing kvLock; kept as defense in depth.
            LOG.warn("Found orphaned PREPARED entries in {}, aborting.", tableBucket, e);
            kvPreWriteBuffer.abortAllPrepared();
            transitionFlushState(FlushState.RUNNING, FlushState.STORAGE_BLOCKED);
            kvFlushScheduler.retryLater(this);
            return null;
        }
        if (preparedFlush.isEmpty()) {
            flushedLogOffset = targetOffset;
        }
        return preparedFlush;
    }

    /**
     * Writes the prepared entries to RocksDB in segments of at most {@code
     * MAX_RECORDS_PER_NATIVE_WRITE} records / {@code writeBatchSize} bytes. Each segment forms
     * exactly one atomic native write (the writer has implicit flushes disabled) and is completed
     * immediately after it lands, so {@code flushedLogOffset}/{@code rowCount} stay consistent with
     * the RocksDB content even if a later segment is rejected by the no-slowdown gate.
     */
    @GuardedBy("kvLock")
    private void writePreparedFlush(PreparedFlush preparedFlush) throws Exception {
        List<PreparedFlush> segments =
                preparedFlush.split(writeBatchSize, MAX_RECORDS_PER_NATIVE_WRITE);
        int nextSegment = 0;
        try (ResourceGuard.Lease lease = rocksDBKv.getResourceGuard().acquireResource();
                KvBatchWriter kvBatchWriter = createNoSlowdownKvBatchWriter()) {
            while (nextSegment < segments.size()) {
                PreparedFlush segment = segments.get(nextSegment);
                for (KvPreWriteBuffer.KvEntry entry : segment.entries()) {
                    KvPreWriteBuffer.Value value = entry.getValue();
                    if (value.get() == null) {
                        if (historicalPartition) {
                            // A physical delete would turn a local miss into a lake lookup and
                            // could expose the stale value that this mutation deleted.
                            kvBatchWriter.put(entry.getKey().get(), HISTORICAL_TOMBSTONE);
                        } else {
                            kvBatchWriter.delete(entry.getKey().get());
                        }
                    } else {
                        kvBatchWriter.put(entry.getKey().get(), value.get());
                    }
                }
                kvBatchWriter.flush();
                completeFlushedSegment(segment);
                nextSegment++;
            }
        } catch (Throwable t) {
            // Segments before nextSegment are already in RocksDB and stay completed; roll only
            // the not-yet-written rest back to ACTIVE so the retry re-prepares exactly the
            // remaining range.
            for (int i = nextSegment; i < segments.size(); i++) {
                kvPreWriteBuffer.abortFlush(segments.get(i));
            }
            throw t;
        }
    }

    /**
     * Publishes one flushed segment: removes its entries from the pre-write buffer and advances
     * {@code flushedLogOffset}/{@code rowCount} to cover exactly the data now in RocksDB.
     */
    @GuardedBy("kvLock")
    private void completeFlushedSegment(PreparedFlush segment) {
        int rowCountDiff = kvPreWriteBuffer.completeFlush(segment);
        if (segment.exclusiveUpToLogSequenceNumber() > flushedLogOffset) {
            flushedLogOffset = segment.exclusiveUpToLogSequenceNumber();
        }
        if (rowCount != ROW_COUNT_DISABLED) {
            rowCount += rowCountDiff;
        }
        if (!segment.isEmpty()) {
            rocksDBKv.recordWriteSucceeded();
        }
        resetFlushRetryBackoff();
    }

    @GuardedBy("kvLock")
    private void finishScheduledFlush() {
        if (isClosed) {
            // close() already forced the state machine to its terminal IDLE state.
            return;
        }
        if (requestedFlushOffset > flushedLogOffset) {
            // More flush work arrived while this run was flushing: requeue a fresh run instead
            // of extending this one, so the completed target is published first via
            // notifyFlushComplete.
            transitionFlushState(FlushState.RUNNING, FlushState.QUEUED);
            kvFlushScheduler.enqueue(this);
        } else {
            transitionFlushState(FlushState.RUNNING, FlushState.IDLE);
        }
    }

    @VisibleForTesting
    void completeScheduledFlush(PreparedFlush preparedFlush) {
        inWriteLock(
                kvLock,
                () -> {
                    if (!isClosed) {
                        completeFlushedSegment(preparedFlush);
                    }
                    finishScheduledFlush();
                });
    }

    @VisibleForTesting
    void abortScheduledFlush(PreparedFlush preparedFlush) {
        inWriteLock(kvLock, () -> kvPreWriteBuffer.abortFlush(preparedFlush));
    }

    @VisibleForTesting
    void delayScheduledFlush(StorageBackpressureException e) {
        LOG.debug("KV flush for {} delayed by RocksDB backpressure.", tableBucket, e);
        inWriteLock(
                kvLock,
                () -> {
                    if (!isClosed) {
                        transitionFlushState(FlushState.RUNNING, FlushState.STORAGE_BLOCKED);
                        kvFlushScheduler.retryLater(this, nextFlushRetryDelayMs());
                    }
                });
    }

    /**
     * Returns the delay before the next flush retry and advances the exponential backoff.
     *
     * <p>Retries are unbounded: a storage-blocked flush keeps retrying (at most every {@link
     * #MAX_FLUSH_RETRY_DELAY_MS}) until it makes progress or the tablet is closed, since giving up
     * would permanently stall {@code flushedLogOffset} and thus the high watermark. Only the delay
     * saturates; the shift is capped at {@link #MAX_FLUSH_RETRY_BACKOFF_SHIFT} so the shifted value
     * stays bounded and cannot overflow.
     */
    @GuardedBy("kvLock")
    private long nextFlushRetryDelayMs() {
        int shift = Math.min(flushRetryAttempts, MAX_FLUSH_RETRY_BACKOFF_SHIFT);
        if (flushRetryAttempts < MAX_FLUSH_RETRY_BACKOFF_SHIFT) {
            flushRetryAttempts++;
        }
        return Math.min(MIN_FLUSH_RETRY_DELAY_MS << shift, MAX_FLUSH_RETRY_DELAY_MS);
    }

    @GuardedBy("kvLock")
    private void resetFlushRetryBackoff() {
        flushRetryAttempts = 0;
    }

    private void failScheduledFlush(Throwable t) {
        inWriteLock(
                kvLock,
                () -> {
                    // Fatal path: force the state machine back to IDLE regardless of the current
                    // state (a concurrent close() may have forced IDLE already).
                    flushState = FlushState.IDLE;
                    FatalErrorHandler fatalErrorHandler = asyncFatalErrorHandler;
                    if (fatalErrorHandler != null) {
                        fatalErrorHandler.onFatalError(
                                new KvStorageException("Failed to flush kv pre-write buffer.", t));
                    } else {
                        LOG.error("Failed to flush kv pre-write buffer for {}.", tableBucket, t);
                    }
                });
    }

    private void notifyFlushComplete() {
        Runnable listener = flushCompleteListener;
        if (listener != null) {
            listener.run();
        }
    }

    /** put key,value,logOffset into pre-write buffer directly. */
    void putToPreWriteBuffer(
            ChangeType changeType, byte[] key, @Nullable byte[] value, long logOffset) {
        KvPreWriteBuffer.Key wrapKey = KvPreWriteBuffer.Key.of(key);
        if (changeType == ChangeType.DELETE && value == null) {
            kvStateAccessor.delete(wrapKey, logOffset);
        } else if (changeType == ChangeType.INSERT) {
            kvStateAccessor.insert(wrapKey, value, logOffset);
        } else if (changeType == ChangeType.UPDATE_AFTER) {
            kvStateAccessor.update(wrapKey, value, logOffset);
        } else {
            throw new IllegalArgumentException(
                    "Unsupported change type for putToPreWriteBuffer: " + changeType);
        }
    }

    /**
     * Get a executor that executes submitted runnable tasks with preventing any concurrent
     * modification to this tablet.
     *
     * @return An executor that wraps task execution within the lock for all modification to this
     *     tablet.
     */
    public Executor getGuardedExecutor() {
        return runnable -> inWriteLock(kvLock, runnable::run);
    }

    public List<ByteArraySlice> multiGet(List<byte[]> keys) throws IOException {
        return inReadLock(
                kvLock,
                () -> {
                    rocksDBKv.checkIfRocksDBClosed();
                    return toValueBodySlices(rocksDBKv.multiGet(keys));
                });
    }

    /**
     * Multi-get that also sees entries still sitting in the kv pre-write buffer (already appended
     * to the CDC log but not yet flushed to RocksDB by the asynchronous flush).
     *
     * <p>Only for internal server-side reads that must observe their own just-written data, e.g.
     * the re-lookup of lookup-with-insert-if-not-exists. External lookups must keep using {@link
     * #multiGet} so that clients only observe flushed data.
     */
    public List<ByteArraySlice> multiGetFromBufferOrKv(List<byte[]> keys) throws IOException {
        checkState(
                !historicalPartition,
                "multiGetFromBufferOrKv is not supported for historical KV tablet %s",
                tableBucket);
        return inReadLock(
                kvLock,
                () -> {
                    rocksDBKv.checkIfRocksDBClosed();
                    List<ByteArraySlice> values = new ArrayList<>(keys.size());
                    for (byte[] key : keys) {
                        KvPreWriteBuffer.Key lookupKey = kvStateAccessor.encodeKey(key, null);
                        byte[] rawValue = kvStateAccessor.lookup(lookupKey).value();
                        values.add(kvValueLayout.toValueBodySlice(rawValue));
                    }
                    return values;
                });
    }

    /** Looks up one key from the flushed local state for an original historical partition. */
    public KvStateLookupResult lookupHistoricalLocal(String originalPartitionName, byte[] key)
            throws IOException {
        checkState(historicalPartition, "%s is not a historical KV tablet", tableBucket);
        return inReadLock(
                kvLock,
                () -> {
                    rocksDBKv.checkIfRocksDBClosed();
                    byte[] value =
                            rocksDBKv.get(
                                    kvStateAccessor.encodeKey(key, originalPartitionName).get());
                    if (value == null) {
                        return KvStateLookupResult.notFound();
                    }
                    return value.length == 0
                            ? KvStateLookupResult.deleted()
                            : KvStateLookupResult.present(value);
                });
    }

    public List<ByteArraySlice> prefixLookup(byte[] prefixKey) throws IOException {
        return inReadLock(
                kvLock,
                () -> {
                    rocksDBKv.checkIfRocksDBClosed();
                    return toValueBodySlices(rocksDBKv.prefixLookup(prefixKey));
                });
    }

    public List<ByteArraySlice> limitScan(int limit) throws IOException {
        return inReadLock(
                kvLock,
                () -> {
                    rocksDBKv.checkIfRocksDBClosed();
                    return toValueBodySlices(rocksDBKv.limitScan(limit));
                });
    }

    private List<ByteArraySlice> toValueBodySlices(List<byte[]> values) {
        List<ByteArraySlice> valueBodySlices = new ArrayList<>(values.size());
        for (byte[] value : values) {
            valueBodySlices.add(kvValueLayout.toValueBodySlice(value));
        }
        return valueBodySlices;
    }

    /**
     * Opens a new full-scan session under the {@code kvLock} read lock. Returns an empty-bucket
     * result (context = {@code null}, all RocksDB resources released internally) when the bucket
     * has no rows. The returned {@link ScannerContext} is unregistered; the caller owns
     * registration and close.
     *
     * @param limit row-count cap across all batches ({@code ≤ 0} means unlimited)
     * @throws IOException if RocksDB is shutting down
     */
    public OpenScanResult openScan(String scannerId, long limit, long initialAccessTimeMs)
            throws IOException {
        return inReadLock(
                kvLock,
                () -> {
                    rocksDBKv.checkIfRocksDBClosed();
                    ResourceGuard.Lease lease = rocksDBKv.getResourceGuard().acquireResource();
                    Snapshot snapshot = null;
                    ReadOptions readOptions = null;
                    RocksIterator iterator = null;
                    boolean success = false;
                    try {
                        snapshot = rocksDBKv.getDb().getSnapshot();
                        // Capture under kvLock so the offset matches the data visible through
                        // the snapshot.
                        long capturedLogOffset = flushedLogOffset;
                        readOptions = new ReadOptions().setSnapshot(snapshot);
                        iterator =
                                rocksDBKv
                                        .getDb()
                                        .newIterator(
                                                rocksDBKv.getDefaultColumnFamilyHandle(),
                                                readOptions);
                        iterator.seekToFirst();
                        if (!iterator.isValid()) {
                            return new OpenScanResult(null, capturedLogOffset);
                        }
                        ScannerContext context =
                                new ScannerContext(
                                        scannerId,
                                        tableBucket,
                                        rocksDBKv,
                                        iterator,
                                        readOptions,
                                        snapshot,
                                        lease,
                                        limit,
                                        capturedLogOffset,
                                        initialAccessTimeMs,
                                        kvValueLayout);
                        success = true;
                        return new OpenScanResult(context, capturedLogOffset);
                    } finally {
                        if (!success) {
                            IOUtils.closeQuietly(iterator);
                            IOUtils.closeQuietly(readOptions);
                            if (snapshot != null) {
                                try {
                                    rocksDBKv.getDb().releaseSnapshot(snapshot);
                                } catch (Throwable t) {
                                    LOG.warn("Error releasing RocksDB snapshot.", t);
                                }
                                IOUtils.closeQuietly(snapshot);
                            }
                            IOUtils.closeQuietly(lease);
                        }
                    }
                });
    }

    public KvBatchWriter createKvBatchWriter() {
        return rocksDBKv.newWriteBatch(
                writeBatchSize,
                serverMetricGroup.kvFlushCount(),
                serverMetricGroup.kvFlushLatencyHistogram());
    }

    private KvBatchWriter createNoSlowdownKvBatchWriter() {
        return rocksDBKv.newNoSlowdownWriteBatch(
                writeBatchSize,
                serverMetricGroup.kvFlushCount(),
                serverMetricGroup.kvFlushLatencyHistogram());
    }

    public void close() throws Exception {
        close(KvCloseMode.PRESERVE_LOCAL_STATE);
    }

    public void close(KvCloseMode closeMode) throws Exception {
        LOG.info(
                "Close kv tablet {} for table {} with mode {}.",
                tableBucket,
                physicalPath,
                closeMode);
        boolean shouldClose =
                inWriteLock(
                        kvLock,
                        () -> {
                            if (isClosed) {
                                return false;
                            }
                            isClosed = true;
                            // Terminal transition: closing forces IDLE regardless of the current
                            // state, see the FlushState state graph.
                            flushState = FlushState.IDLE;
                            return true;
                        });
        if (shouldClose && closeFlushScheduler) {
            kvFlushScheduler.close();
        }
        if (shouldClose && rocksDBKv != null) {
            // Note: RocksDB metrics lifecycle is managed by TableMetricGroup.
            // Close outside kvLock so an async flush can finish and release its RocksDB lease.
            rocksDBKv.close(closeMode);
        }
    }

    /** Completely delete the kv directory and all contents form the file system with no delay. */
    public void drop() throws Exception {
        inWriteLock(
                kvLock,
                () -> {
                    // first close the kv.
                    close(KvCloseMode.DISCARD_UNPERSISTED_STATE);
                    // then delete the directory.
                    FileUtils.deleteDirectory(kvTabletDir);
                });
    }

    public RocksIncrementalSnapshot createIncrementalSnapshot(
            Map<Long, Collection<KvFileHandleAndLocalPath>> uploadedSstFiles,
            KvSnapshotDataUploader kvSnapshotDataUploader,
            long lastCompletedSnapshotId) {
        return new RocksIncrementalSnapshot(
                uploadedSstFiles,
                rocksDBKv.getDb(),
                rocksDBKv.getResourceGuard(),
                kvSnapshotDataUploader,
                kvTabletDir,
                lastCompletedSnapshotId);
    }

    // only for testing.
    @VisibleForTesting
    KvPreWriteBuffer getKvPreWriteBuffer() {
        return kvPreWriteBuffer;
    }

    // only for testing.
    @VisibleForTesting
    public RocksDBKv getRocksDBKv() {
        return rocksDBKv;
    }

    /** Returns the recent normalized backpressure pressure in {@code [0, 1)}. */
    public float currentPressure() {
        return rocksDBKv.currentPressure();
    }

    /**
     * Applies a flush state transition, enforcing that the current state matches the source state
     * of the transition shown in the {@link FlushState} state graph. Terminal transitions forced by
     * {@link #close()} and {@link #failScheduledFlush(Throwable)} assign the state directly.
     */
    @GuardedBy("kvLock")
    private void transitionFlushState(FlushState expected, FlushState target) {
        checkState(
                flushState == expected,
                "Invalid flush state transition for %s: expected %s but was %s (target %s).",
                tableBucket,
                expected,
                flushState,
                target);
        flushState = target;
    }

    /**
     * Flush scheduling state for one KV tablet.
     *
     * <p>This state tracks scheduler ownership and retry backoff. Whether more data needs to be
     * flushed is determined separately by {@code requestedFlushOffset} and {@code
     * flushedLogOffset}.
     *
     * <pre>
     * Normal scheduling:
     *
     *   +------+    a new target requires flushing    +--------+
     *   | IDLE | -----------------------------------> | QUEUED |
     *   +------+                                      +--------+
     *                                                     |
     *                                              a worker claims
     *                                               the queued task
     *                                                     |
     *                                                     v
     *                                               +---------+
     *                                               | RUNNING |
     *                                               +---------+
     *                                                     |
     *                                            the target is reached
     *                                            or a fatal error occurs
     *                                                     |
     *                                                     v
     *                                                 +------+
     *                                                 | IDLE |
     *                                                 +------+
     *
     * Backpressure retry:
     *
     *                    storage rejection or
     *                    prepared-entry conflict
     *   +---------+ -------------------------------> +-----------------+
     *   | RUNNING |                                  | STORAGE_BLOCKED |
     *   +---------+                                  +-----------------+
     *                                                        |
     *                                                 the retry timer
     *                                                      fires
     *                                                        |
     *                                                        v
     *                                                   +--------+
     *                                                   | QUEUED |
     *                                                   +--------+
     * </pre>
     *
     * <p>A higher flush target received in {@code QUEUED}, {@code RUNNING}, or {@code
     * STORAGE_BLOCKED} only advances {@code requestedFlushOffset}; it does not change the state or
     * enqueue another task. Closing the tablet sets {@code isClosed} and terminates the state
     * machine. All transitions must happen while holding {@code kvLock} and are applied through
     * {@code transitionFlushState}, which enforces the source state of each transition.
     */
    enum FlushState {
        IDLE,
        QUEUED,
        RUNNING,
        STORAGE_BLOCKED
    }
}
