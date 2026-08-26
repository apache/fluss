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
import org.apache.fluss.exception.DeletionDisabledException;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.exception.KvStorageException;
import org.apache.fluss.exception.SchemaNotExistException;
import org.apache.fluss.exception.StorageBackpressureException;
import org.apache.fluss.memory.MemorySegmentPool;
import org.apache.fluss.metadata.ChangelogImage;
import org.apache.fluss.metadata.DeleteBehavior;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.KvRecord;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.KvRecordReadContext;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.PaddingRow;
import org.apache.fluss.row.arrow.ArrowWriterPool;
import org.apache.fluss.row.arrow.ArrowWriterProvider;
import org.apache.fluss.row.encode.ValueDecoder;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.server.kv.autoinc.AutoIncIDRange;
import org.apache.fluss.server.kv.autoinc.AutoIncrementManager;
import org.apache.fluss.server.kv.autoinc.AutoIncrementUpdater;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.PreparedFlush;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.TruncateReason;
import org.apache.fluss.server.kv.rocksdb.RocksDBKv;
import org.apache.fluss.server.kv.rocksdb.RocksDBKvBuilder;
import org.apache.fluss.server.kv.rocksdb.RocksDBResourceContainer;
import org.apache.fluss.server.kv.rocksdb.RocksDBStatistics;
import org.apache.fluss.server.kv.rowmerger.DefaultRowMerger;
import org.apache.fluss.server.kv.rowmerger.RowMerger;
import org.apache.fluss.server.kv.scan.OpenScanResult;
import org.apache.fluss.server.kv.scan.ScannerContext;
import org.apache.fluss.server.kv.snapshot.KvFileHandleAndLocalPath;
import org.apache.fluss.server.kv.snapshot.KvSnapshotDataUploader;
import org.apache.fluss.server.kv.snapshot.RocksIncrementalSnapshot;
import org.apache.fluss.server.kv.snapshot.TabletState;
import org.apache.fluss.server.kv.wal.ArrowWalBuilder;
import org.apache.fluss.server.kv.wal.CompactedWalBuilder;
import org.apache.fluss.server.kv.wal.IndexWalBuilder;
import org.apache.fluss.server.kv.wal.WalBuilder;
import org.apache.fluss.server.log.LogAppendInfo;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.server.utils.FatalErrorHandler;
import org.apache.fluss.server.utils.ResourceGuard;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.BytesUtils;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.IOUtils;

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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.fluss.utils.Preconditions.checkState;
import static org.apache.fluss.utils.concurrent.LockUtils.inLock;
import static org.apache.fluss.utils.concurrent.LockUtils.inReadLock;
import static org.apache.fluss.utils.concurrent.LockUtils.inWriteLock;

/**
 * A KV tablet that presents the committed RocksDB state and coordinates the mutable write pipeline
 * used to produce that state.
 *
 * <p>The tablet uses two locks with deliberately different scopes:
 *
 * <ul>
 *   <li>{@code kvStateLock} protects the RocksDB-visible state and the tablet lifecycle. Ordinary
 *       lookups and puts take its read lock, so a lookup may execute concurrently with a put.
 *       Flush, snapshot and close take its write lock because they must exclude both operations.
 *   <li>{@code mutationLock} serializes the non-thread-safe write pipeline: row merging,
 *       auto-increment allocation, pre-write-buffer mutation, WAL append and rollback. It keeps
 *       put-put ordering without making ordinary lookups wait for that work.
 * </ul>
 *
 * <p>When both locks are needed, the only valid order is {@code kvStateLock -> mutationLock}. Lock
 * upgrading and the reverse order are forbidden. In particular, a put that requests backpressure
 * flushing must release both locks before requesting {@code kvStateLock}'s write lock, and
 * callbacks that may enter Replica lifecycle code must run after both locks are released.
 *
 * <p>A logical flush holds both locks across every native RocksDB write and the corresponding
 * {@code flushedLogOffset}/{@code rowCount} update. Consequently, a lookup can observe either the
 * state before the flush or the completed state, but never an intermediate prefix of a segmented
 * flush. Close holds {@code kvStateLock}'s write lock while closing RocksDB, which makes a
 * point-lookup lease unnecessary. Resources that outlive one lock acquisition, such as scan
 * iterators, must still retain a {@link ResourceGuard.Lease} for their full lifetime.
 */
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

    private final LogTablet logTablet;
    private final ArrowWriterProvider arrowWriterProvider;
    private final MemorySegmentPool memorySegmentPool;

    private final File kvTabletDir;
    private final long writeBatchSize;
    private final RocksDBKv rocksDBKv;
    private final KvPreWriteBuffer kvPreWriteBuffer;
    private final TabletServerMetricGroup serverMetricGroup;
    private final KvFlushScheduler kvFlushScheduler;
    private final boolean closeFlushScheduler;

    /**
     * Guards RocksDB-visible state and the tablet lifecycle. Read holders may access a live RocksDB
     * instance; write holders may atomically publish a flush, take a snapshot or close it.
     */
    private final ReadWriteLock kvStateLock = new ReentrantReadWriteLock();

    /**
     * Serializes merge, auto-increment, pre-write-buffer mutation, WAL append and rollback. If
     * acquired together with {@link #kvStateLock}, this lock must always be acquired second.
     */
    private final Lock mutationLock = new ReentrantLock();

    private final LogFormat logFormat;
    private final KvFormat kvFormat;
    // defines how to merge rows on the same primary key
    private final RowMerger rowMerger;
    // Pre-created DefaultRowMerger for OVERWRITE mode (undo recovery scenarios)
    // This avoids creating a new instance on every putAsLeader call
    private final RowMerger overwriteRowMerger;
    private final ArrowCompressionInfo arrowCompressionInfo;
    private final AutoIncrementManager autoIncrementManager;

    private final SchemaGetter schemaGetter;

    // the changelog image mode for this tablet
    private final ChangelogImage changelogImage;

    // RocksDB statistics accessor for this tablet
    @Nullable private final RocksDBStatistics rocksDBStatistics;

    /**
     * The kv data in pre-write buffer whose log offset is less than the flushedLogOffset has been
     * flushed into kv.
     */
    private volatile long flushedLogOffset = 0;

    @GuardedBy("kvStateLock")
    private FlushState flushState = FlushState.IDLE;

    @GuardedBy("kvStateLock")
    private long requestedFlushOffset = 0;

    @GuardedBy("kvStateLock")
    private int flushRetryAttempts = 0;

    /** Invoked after each flush that made progress; set at construction time by the owner. */
    private volatile @Nullable Runnable flushCompleteListener;

    private volatile @Nullable FatalErrorHandler asyncFatalErrorHandler;

    private volatile long rowCount;

    @GuardedBy("kvStateLock")
    private volatile boolean isClosed = false;

    private KvTablet(
            PhysicalTablePath physicalPath,
            TableBucket tableBucket,
            LogTablet logTablet,
            File kvTabletDir,
            TabletServerMetricGroup serverMetricGroup,
            RocksDBKv rocksDBKv,
            long writeBatchSize,
            LogFormat logFormat,
            BufferAllocator arrowBufferAllocator,
            MemorySegmentPool memorySegmentPool,
            KvFormat kvFormat,
            RowMerger rowMerger,
            ArrowCompressionInfo arrowCompressionInfo,
            SchemaGetter schemaGetter,
            ChangelogImage changelogImage,
            @Nullable RocksDBStatistics rocksDBStatistics,
            KvFlushScheduler kvFlushScheduler,
            boolean closeFlushScheduler,
            @Nullable Runnable flushCompleteListener,
            AutoIncrementManager autoIncrementManager) {
        this.physicalPath = physicalPath;
        this.tableBucket = tableBucket;
        this.logTablet = logTablet;
        this.kvTabletDir = kvTabletDir;
        this.rocksDBKv = rocksDBKv;
        this.writeBatchSize = writeBatchSize;
        this.serverMetricGroup = serverMetricGroup;
        this.kvFlushScheduler = kvFlushScheduler;
        this.closeFlushScheduler = closeFlushScheduler;
        this.kvPreWriteBuffer = new KvPreWriteBuffer(serverMetricGroup);
        this.logFormat = logFormat;
        this.arrowWriterProvider = new ArrowWriterPool(arrowBufferAllocator);
        this.memorySegmentPool = memorySegmentPool;
        this.kvFormat = kvFormat;
        this.rowMerger = rowMerger;
        // Pre-create DefaultRowMerger for OVERWRITE mode to avoid creating new instances
        // on every putAsLeader call. Used for undo recovery scenarios.
        this.overwriteRowMerger = new DefaultRowMerger(kvFormat, DeleteBehavior.ALLOW);
        this.arrowCompressionInfo = arrowCompressionInfo;
        this.schemaGetter = schemaGetter;
        this.changelogImage = changelogImage;
        this.rocksDBStatistics = rocksDBStatistics;
        this.autoIncrementManager = autoIncrementManager;
        this.flushCompleteListener = flushCompleteListener;
        // disable row count for WAL image mode.
        this.rowCount = changelogImage == ChangelogImage.WAL ? ROW_COUNT_DISABLED : 0L;
    }

    /**
     * Creates a kv tablet with a dedicated {@link KvFlushScheduler} that is closed together with
     * the tablet. Production code must use {@link #create(PhysicalTablePath, TableBucket,
     * LogTablet, File, Configuration, TabletServerMetricGroup, BufferAllocator, MemorySegmentPool,
     * KvFormat, RowMerger, ArrowCompressionInfo, SchemaGetter, ChangelogImage, RateLimiter,
     * KvFlushScheduler, Runnable, AutoIncrementManager)} with the shared scheduler owned by {@link
     * KvManager}.
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
                new KvFlushScheduler(serverConf),
                true,
                null,
                autoIncrementManager);
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
            KvFlushScheduler kvFlushScheduler,
            @Nullable Runnable flushCompleteListener,
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
                kvFlushScheduler,
                false,
                flushCompleteListener,
                autoIncrementManager);
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
            KvFlushScheduler kvFlushScheduler,
            boolean closeFlushScheduler,
            @Nullable Runnable flushCompleteListener,
            AutoIncrementManager autoIncrementManager)
            throws IOException {
        RocksDBKv kv = buildRocksDBKv(serverConf, kvTabletDir, sharedRateLimiter);

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
                        kv.getBlockCache());

        return new KvTablet(
                tablePath,
                tableBucket,
                logTablet,
                kvTabletDir,
                serverMetricGroup,
                kv,
                serverConf.get(ConfigOptions.KV_WRITE_BATCH_SIZE).getBytes(),
                logTablet.getLogFormat(),
                arrowBufferAllocator,
                memorySegmentPool,
                kvFormat,
                rowMerger,
                arrowCompressionInfo,
                schemaGetter,
                changelogImage,
                rocksDBStatistics,
                kvFlushScheduler,
                closeFlushScheduler,
                flushCompleteListener,
                autoIncrementManager);
    }

    private static RocksDBKv buildRocksDBKv(
            Configuration configuration, File kvDir, RateLimiter sharedRateLimiter)
            throws IOException {
        // Enable statistics to support RocksDB statistics collection
        RocksDBResourceContainer rocksDBResourceContainer =
                new RocksDBResourceContainer(configuration, kvDir, true, sharedRateLimiter);
        RocksDBKvBuilder rocksDBKvBuilder =
                new RocksDBKvBuilder(
                                kvDir,
                                rocksDBResourceContainer,
                                rocksDBResourceContainer.getColumnOptions())
                        .setFlussL0SlowdownTrigger(
                                configuration.get(
                                        ConfigOptions.KV_BACKPRESSURE_L0_SLOWDOWN_TRIGGER));
        return rocksDBKvBuilder.build();
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
        this.rowCount = rowCount;
    }

    // row_count is volatile, so it's safe to read without lock
    public long getRowCount() {
        if (rowCount == ROW_COUNT_DISABLED) {
            throw new InvalidTableException(
                    String.format(
                            "Row count is disabled for this table '%s'. This usually happens when the table is"
                                    + "created before v0.9 or the changelog image is set to WAL, "
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
     * <p>Note: this method must be called under the kvStateLock to ensure the consistency between
     * the returned state and the log offset.
     */
    @GuardedBy("kvStateLock")
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
        try {
            return inReadLock(
                    kvStateLock,
                    () ->
                            inLock(
                                    mutationLock,
                                    () -> {
                                        checkIfClosed();

                                        long pendingFlushBytesAfterWrite =
                                                kvPreWriteBuffer.pendingFlushBytes()
                                                        + kvRecords.sizeInBytes();

                                        // Write-path admission gate: reject the request if
                                        // accepting this batch would
                                        // push the buffered KV view beyond RocksDB's safe write
                                        // budget.
                                        if (rocksDBKv.wouldExceedFlushBudget(
                                                pendingFlushBytesAfterWrite)) {
                                            throw new StorageBackpressureException(
                                                    String.format(
                                                            "Write rejected for %s: flush budget exceeded "
                                                                    + "(storage pressure or buffer size limit reached). "
                                                                    + "Retry after backoff.",
                                                            tableBucket));
                                        }

                                        SchemaInfo schemaInfo = schemaGetter.getLatestSchemaInfo();
                                        Schema latestSchema = schemaInfo.getSchema();
                                        short latestSchemaId = (short) schemaInfo.getSchemaId();
                                        validateSchemaId(kvRecords.schemaId(), latestSchemaId);

                                        AutoIncrementUpdater currentAutoIncrementUpdater =
                                                autoIncrementManager.getUpdaterForSchema(
                                                        kvFormat, latestSchemaId);

                                        // Validate targetColumns doesn't contain auto-increment
                                        // column
                                        currentAutoIncrementUpdater.validateTargetColumns(
                                                targetColumns);

                                        // Determine the row merger based on mergeMode:
                                        // - DEFAULT: Use the configured merge engine (rowMerger)
                                        // - OVERWRITE: Bypass merge engine, use pre-created
                                        // overwriteRowMerger
                                        //   to directly replace values (for undo recovery
                                        // scenarios)
                                        // We only support ADD COLUMN, so targetColumns is fine to
                                        // be used directly.
                                        RowMerger currentMerger =
                                                (mergeMode == MergeMode.OVERWRITE)
                                                        ? overwriteRowMerger.configureTargetColumns(
                                                                targetColumns,
                                                                latestSchemaId,
                                                                latestSchema)
                                                        : rowMerger.configureTargetColumns(
                                                                targetColumns,
                                                                latestSchemaId,
                                                                latestSchema);

                                        RowType latestRowType = latestSchema.getRowType();
                                        WalBuilder walBuilder =
                                                createWalBuilder(latestSchemaId, latestRowType);
                                        walBuilder.setWriterState(
                                                kvRecords.writerId(), kvRecords.batchSequence());
                                        // we only support ADD COLUMN LAST, so the BinaryRow after
                                        // RowMerger is
                                        // only has fewer ending columns than latest schema, so we
                                        // pad nulls to
                                        // the end of the BinaryRow to get the latest schema row.
                                        PaddingRow latestSchemaRow =
                                                new PaddingRow(latestRowType.getFieldCount());
                                        // get offset to track the offset corresponded to the kv
                                        // record
                                        long logEndOffsetOfPrevBatch =
                                                logTablet.localLogEndOffset();

                                        try {
                                            processKvRecords(
                                                    kvRecords,
                                                    kvRecords.schemaId(),
                                                    currentMerger,
                                                    currentAutoIncrementUpdater,
                                                    walBuilder,
                                                    latestSchemaRow,
                                                    logEndOffsetOfPrevBatch);

                                            // There will be a situation that these batches of
                                            // kvRecordBatch have not
                                            // generated any CDC logs, for example, when client
                                            // attempts to delete
                                            // some non-existent keys or MergeEngineType set to
                                            // FIRST_ROW. In this case,
                                            // we cannot simply return, as doing so would cause a
                                            // OutOfOrderSequenceException problem. Therefore, here
                                            // we will build an
                                            // empty batch with lastLogOffset to 0L as the
                                            // baseLogOffset is 0L. As doing
                                            // that, the logOffsetDelta in logRecordBatch will be
                                            // set to 0L. So, we will
                                            // put a batch into file with recordCount 0 and offset
                                            // plus 1L, it will
                                            // update the batchSequence corresponding to the
                                            // writerId and also increment
                                            // the CDC log offset by 1.
                                            LogAppendInfo logAppendInfo =
                                                    logTablet.appendAsLeader(walBuilder.build());

                                            // if the batch is duplicated, we should truncate the
                                            // kvPreWriteBuffer
                                            // already written.
                                            if (logAppendInfo.duplicated()) {
                                                kvPreWriteBuffer.truncateTo(
                                                        logEndOffsetOfPrevBatch,
                                                        TruncateReason.DUPLICATED);
                                            }
                                            return logAppendInfo;
                                        } catch (Throwable t) {
                                            // While encounter error here, the CDC logs may fail
                                            // writing to disk,
                                            // and the client probably will resend the batch. If we
                                            // do not remove the
                                            // values generated by the erroneous batch from the
                                            // kvPreWriteBuffer, the
                                            // retry-send batch will produce incorrect CDC logs.
                                            // TODO for some errors, the cdc logs may already be
                                            // written to disk, for
                                            //  those errors, we should not truncate the
                                            // kvPreWriteBuffer.
                                            kvPreWriteBuffer.truncateTo(
                                                    logEndOffsetOfPrevBatch, TruncateReason.ERROR);
                                            throw t;
                                        } finally {
                                            // deallocate the memory and arrow writer used by the
                                            // wal builder
                                            walBuilder.deallocate();
                                        }
                                    }));
        } catch (StorageBackpressureException e) {
            // Releasing the read lock before requesting a flush avoids a read-to-write lock
            // upgrade, which ReentrantReadWriteLock does not support.
            inWriteLock(
                    kvStateLock,
                    () ->
                            requestFlushInternal(
                                    Math.max(requestedFlushOffset, logTablet.getHighWatermark())));
            throw e;
        }
    }

    private void validateSchemaId(short schemaIdOfNewData, short latestSchemaId) {
        if (schemaIdOfNewData > latestSchemaId || schemaIdOfNewData < 0) {
            throw new SchemaNotExistException(
                    "Invalid schema id: "
                            + schemaIdOfNewData
                            + ", latest schema id: "
                            + latestSchemaId);
        }
    }

    private void processKvRecords(
            KvRecordBatch kvRecords,
            short schemaIdOfNewData,
            RowMerger currentMerger,
            AutoIncrementUpdater autoIncrementUpdater,
            WalBuilder walBuilder,
            PaddingRow latestSchemaRow,
            long startLogOffset)
            throws Exception {
        long logOffset = startLogOffset;

        // TODO: reuse the read context and decoder
        KvRecordBatch.ReadContext readContext =
                KvRecordReadContext.createReadContext(kvFormat, schemaGetter);
        ValueDecoder valueDecoder = new ValueDecoder(schemaGetter, kvFormat);

        for (KvRecord kvRecord : kvRecords.records(readContext)) {
            byte[] keyBytes = BytesUtils.toArray(kvRecord.getKey());
            KvPreWriteBuffer.Key key = KvPreWriteBuffer.Key.of(keyBytes);
            BinaryRow row = kvRecord.getRow();
            BinaryValue currentValue = row == null ? null : new BinaryValue(schemaIdOfNewData, row);

            if (currentValue == null) {
                logOffset =
                        processDeletion(
                                key,
                                currentMerger,
                                valueDecoder,
                                walBuilder,
                                latestSchemaRow,
                                logOffset);
            } else {
                logOffset =
                        processUpsert(
                                key,
                                currentValue,
                                currentMerger,
                                autoIncrementUpdater,
                                valueDecoder,
                                walBuilder,
                                latestSchemaRow,
                                logOffset);
            }
        }
    }

    private long processDeletion(
            KvPreWriteBuffer.Key key,
            RowMerger currentMerger,
            ValueDecoder valueDecoder,
            WalBuilder walBuilder,
            PaddingRow latestSchemaRow,
            long logOffset)
            throws Exception {
        DeleteBehavior deleteBehavior = currentMerger.deleteBehavior();
        if (deleteBehavior == DeleteBehavior.IGNORE) {
            // skip delete rows if the merger doesn't support yet
            return logOffset;
        } else if (deleteBehavior == DeleteBehavior.DISABLE) {
            throw new DeletionDisabledException(
                    "Delete operations are disabled for this table. "
                            + "The table.delete.behavior is set to 'disable'.");
        }

        byte[] oldValueBytes = getFromBufferOrKv(key);
        if (oldValueBytes == null) {
            LOG.debug(
                    "The specific key can't be found in kv tablet although the kv record is for deletion, "
                            + "ignore it directly as it doesn't exist in the kv tablet yet.");
            return logOffset;
        }

        BinaryValue oldValue = valueDecoder.decodeValue(oldValueBytes);
        BinaryValue newValue = currentMerger.delete(oldValue);

        // if newValue is null, it means the row should be deleted
        if (newValue == null) {
            return applyDelete(key, oldValue, walBuilder, latestSchemaRow, logOffset);
        } else {
            return applyUpdate(key, oldValue, newValue, walBuilder, latestSchemaRow, logOffset);
        }
    }

    private long processUpsert(
            KvPreWriteBuffer.Key key,
            BinaryValue currentValue,
            RowMerger currentMerger,
            AutoIncrementUpdater autoIncrementUpdater,
            ValueDecoder valueDecoder,
            WalBuilder walBuilder,
            PaddingRow latestSchemaRow,
            long logOffset)
            throws Exception {
        // Optimization: IN WAL mode，when using DefaultRowMerger (full update, not partial update)
        // and there is no auto-increment column, we can skip fetching old value for better
        // performance since the result always reflects the new value. In this case, both INSERT and
        // UPDATE will produce UPDATE_AFTER.
        if (changelogImage == ChangelogImage.WAL
                && !autoIncrementUpdater.hasAutoIncrement()
                && currentMerger instanceof DefaultRowMerger) {
            return applyUpdate(key, null, currentValue, walBuilder, latestSchemaRow, logOffset);
        }

        byte[] oldValueBytes = getFromBufferOrKv(key);
        if (oldValueBytes == null) {
            BinaryValue valueToInsert = currentMerger.merge(null, currentValue);
            return applyInsert(
                    key,
                    valueToInsert,
                    walBuilder,
                    latestSchemaRow,
                    logOffset,
                    autoIncrementUpdater);
        }

        BinaryValue oldValue = valueDecoder.decodeValue(oldValueBytes);
        BinaryValue newValue = currentMerger.merge(oldValue, currentValue);

        if (newValue == oldValue) {
            // no actual change, skip this record
            return logOffset;
        }

        return applyUpdate(key, oldValue, newValue, walBuilder, latestSchemaRow, logOffset);
    }

    private long applyDelete(
            KvPreWriteBuffer.Key key,
            BinaryValue oldValue,
            WalBuilder walBuilder,
            PaddingRow latestSchemaRow,
            long logOffset)
            throws Exception {
        walBuilder.append(ChangeType.DELETE, latestSchemaRow.replaceRow(oldValue.row));
        kvPreWriteBuffer.delete(key, logOffset);
        return logOffset + 1;
    }

    private long applyInsert(
            KvPreWriteBuffer.Key key,
            BinaryValue currentValue,
            WalBuilder walBuilder,
            PaddingRow latestSchemaRow,
            long logOffset,
            AutoIncrementUpdater autoIncrementUpdater)
            throws Exception {
        BinaryValue newValue = autoIncrementUpdater.updateAutoIncrementColumns(currentValue);
        walBuilder.append(ChangeType.INSERT, latestSchemaRow.replaceRow(newValue.row));
        kvPreWriteBuffer.insert(key, newValue.encodeValue(), logOffset);
        return logOffset + 1;
    }

    private long applyUpdate(
            KvPreWriteBuffer.Key key,
            BinaryValue oldValue,
            BinaryValue newValue,
            WalBuilder walBuilder,
            PaddingRow latestSchemaRow,
            long logOffset)
            throws Exception {
        if (changelogImage == ChangelogImage.WAL) {
            walBuilder.append(ChangeType.UPDATE_AFTER, latestSchemaRow.replaceRow(newValue.row));
            kvPreWriteBuffer.update(key, newValue.encodeValue(), logOffset);
            return logOffset + 1;
        } else {
            walBuilder.append(ChangeType.UPDATE_BEFORE, latestSchemaRow.replaceRow(oldValue.row));
            walBuilder.append(ChangeType.UPDATE_AFTER, latestSchemaRow.replaceRow(newValue.row));
            kvPreWriteBuffer.update(key, newValue.encodeValue(), logOffset + 1);
            return logOffset + 2;
        }
    }

    private WalBuilder createWalBuilder(int schemaId, RowType rowType) throws Exception {
        switch (logFormat) {
            case INDEXED:
                if (kvFormat == KvFormat.COMPACTED) {
                    // convert from compacted row to indexed row is time cost, and gain
                    // less benefits, currently we won't support compacted as kv format and
                    // indexed as cdc log format.
                    // so in here we throw exception directly
                    throw new IllegalArgumentException(
                            "Primary Key Table with COMPACTED kv format doesn't support INDEXED cdc log format.");
                }
                return new IndexWalBuilder(schemaId, memorySegmentPool);
            case COMPACTED:
                return new CompactedWalBuilder(schemaId, rowType, memorySegmentPool);
            case ARROW:
                return new ArrowWalBuilder(
                        schemaId,
                        arrowWriterProvider.getOrCreateWriter(
                                tableBucket.getTableId(),
                                schemaId,
                                // we don't limit size of the arrow batch, because all the
                                // changelogs should be in a single batch
                                Integer.MAX_VALUE,
                                rowType,
                                arrowCompressionInfo),
                        memorySegmentPool);
            default:
                throw new IllegalArgumentException("Unsupported log format: " + logFormat);
        }
    }

    @VisibleForTesting
    long localLogEndOffset() {
        return logTablet.localLogEndOffset();
    }

    public void requestFlush(long exclusiveUpToLogOffset, FatalErrorHandler fatalErrorHandler) {
        asyncFatalErrorHandler = fatalErrorHandler;
        inWriteLock(kvStateLock, () -> requestFlushInternal(exclusiveUpToLogOffset));
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
        return inReadLock(kvStateLock, () -> flushState);
    }

    @VisibleForTesting
    void setFlushState(FlushState state) {
        inWriteLock(kvStateLock, () -> flushState = state);
    }

    @GuardedBy("kvStateLock")
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
                kvStateLock,
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
            // Keep the complete logical flush hidden from lookups. writePreparedFlush may issue
            // several native RocksDB writes, so releasing kvStateLock between segments would
            // expose a partially applied flush. mutationLock also excludes every pre-write-buffer
            // and auto-increment mutation while the flush state is being committed.
            inWriteLock(kvStateLock, () -> inLock(mutationLock, this::doScheduledFlush));
        } catch (StorageBackpressureException e) {
            delayScheduledFlush(e);
        } catch (Throwable t) {
            failScheduledFlush(t);
        } finally {
            // Publish progress (including the completed prefix of a partially rejected run) so
            // the high watermark can advance. Runs outside kvStateLock because the listener
            // acquires
            // the replica's leaderIsrUpdateLock while the write path acquires
            // leaderIsrUpdateLock -> kvStateLock; invoking it under kvStateLock would invert that
            // order.
            if (flushedLogOffset > flushedOffsetBefore) {
                notifyFlushComplete();
            }
        }
    }

    @GuardedBy("kvStateLock")
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
                kvStateLock,
                () -> {
                    if (isClosed || flushState != FlushState.QUEUED) {
                        return false;
                    }
                    transitionFlushState(FlushState.QUEUED, FlushState.RUNNING);
                    return true;
                });
    }

    @GuardedBy("kvStateLock")
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
            // before releasing kvStateLock; kept as defense in depth.
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
    @GuardedBy("kvStateLock")
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
                        kvBatchWriter.delete(entry.getKey().get());
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
    @GuardedBy("kvStateLock")
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

    @GuardedBy("kvStateLock")
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
                kvStateLock,
                () ->
                        inLock(
                                mutationLock,
                                () -> {
                                    if (!isClosed) {
                                        completeFlushedSegment(preparedFlush);
                                    }
                                    finishScheduledFlush();
                                }));
    }

    @VisibleForTesting
    void abortScheduledFlush(PreparedFlush preparedFlush) {
        inWriteLock(
                kvStateLock,
                () -> inLock(mutationLock, () -> kvPreWriteBuffer.abortFlush(preparedFlush)));
    }

    @VisibleForTesting
    void delayScheduledFlush(StorageBackpressureException e) {
        LOG.debug("KV flush for {} delayed by RocksDB backpressure.", tableBucket, e);
        inWriteLock(
                kvStateLock,
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
    @GuardedBy("kvStateLock")
    private long nextFlushRetryDelayMs() {
        int shift = Math.min(flushRetryAttempts, MAX_FLUSH_RETRY_BACKOFF_SHIFT);
        if (flushRetryAttempts < MAX_FLUSH_RETRY_BACKOFF_SHIFT) {
            flushRetryAttempts++;
        }
        return Math.min(MIN_FLUSH_RETRY_DELAY_MS << shift, MAX_FLUSH_RETRY_DELAY_MS);
    }

    @GuardedBy("kvStateLock")
    private void resetFlushRetryBackoff() {
        flushRetryAttempts = 0;
    }

    private void failScheduledFlush(Throwable t) {
        FatalErrorHandler fatalErrorHandler =
                inWriteLock(
                        kvStateLock,
                        () -> {
                            // Fatal path: force the state machine back to IDLE regardless of the
                            // current state (a concurrent close() may have forced IDLE already).
                            flushState = FlushState.IDLE;
                            return asyncFatalErrorHandler;
                        });
        if (fatalErrorHandler != null) {
            fatalErrorHandler.onFatalError(
                    new KvStorageException("Failed to flush kv pre-write buffer.", t));
        } else {
            LOG.error("Failed to flush kv pre-write buffer for {}.", tableBucket, t);
        }
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
            kvPreWriteBuffer.delete(wrapKey, logOffset);
        } else if (changeType == ChangeType.INSERT) {
            kvPreWriteBuffer.insert(wrapKey, value, logOffset);
        } else if (changeType == ChangeType.UPDATE_AFTER) {
            kvPreWriteBuffer.update(wrapKey, value, logOffset);
        } else {
            throw new IllegalArgumentException(
                    "Unsupported change type for putToPreWriteBuffer: " + changeType);
        }
    }

    /**
     * Returns an executor that runs a task against one stable tablet state.
     *
     * <p>The task runs under {@code kvStateLock.write -> mutationLock}. Snapshot creation uses this
     * boundary so its RocksDB checkpoint, flushed offset, row count and auto-increment range all
     * describe the same state.
     *
     * @return An executor that wraps task execution within the lock for all modification to this
     *     tablet.
     */
    public Executor getGuardedExecutor() {
        return runnable -> inWriteLock(kvStateLock, () -> inLock(mutationLock, runnable::run));
    }

    // get from kv pre-write buffer first, if can't find, get from rocksdb
    private byte[] getFromBufferOrKv(KvPreWriteBuffer.Key key) throws IOException {
        KvPreWriteBuffer.Value value = kvPreWriteBuffer.get(key);
        if (value == null) {
            return rocksDBKv.get(key.get());
        }
        return value.get();
    }

    /**
     * Reads committed values directly from RocksDB.
     *
     * <p>This method intentionally does not acquire {@code mutationLock} and does not inspect the
     * pre-write buffer. It can therefore overlap a put while preserving the existing rule that
     * unflushed WAL records are invisible to ordinary lookup clients. The {@code kvStateLock} read
     * lock prevents concurrent logical flush publication and native RocksDB close.
     */
    public List<byte[]> multiGet(List<byte[]> keys) throws IOException {
        return inReadLock(
                kvStateLock,
                () -> {
                    checkIfClosed();
                    return rocksDBKv.multiGet(keys);
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
    public List<byte[]> multiGetFromBufferOrKv(List<byte[]> keys) throws IOException {
        return inReadLock(
                kvStateLock,
                () ->
                        inLock(
                                mutationLock,
                                () -> {
                                    checkIfClosed();
                                    List<byte[]> values = new ArrayList<>(keys.size());
                                    for (byte[] key : keys) {
                                        values.add(getFromBufferOrKv(KvPreWriteBuffer.Key.of(key)));
                                    }
                                    return values;
                                }));
    }

    public List<byte[]> prefixLookup(byte[] prefixKey) throws IOException {
        return inReadLock(
                kvStateLock,
                () -> {
                    checkIfClosed();
                    return rocksDBKv.prefixLookup(prefixKey);
                });
    }

    public List<byte[]> limitScan(int limit) throws IOException {
        return inReadLock(
                kvStateLock,
                () -> {
                    checkIfClosed();
                    return rocksDBKv.limitScan(limit);
                });
    }

    /**
     * Opens a new full-scan session under the {@code kvStateLock} read lock. Returns an
     * empty-bucket result (context = {@code null}, all RocksDB resources released internally) when
     * the bucket has no rows. The returned {@link ScannerContext} is unregistered; the caller owns
     * registration and close.
     *
     * @param limit row-count cap across all batches ({@code ≤ 0} means unlimited)
     * @throws IOException if RocksDB is shutting down
     */
    public OpenScanResult openScan(String scannerId, long limit, long initialAccessTimeMs)
            throws IOException {
        return inReadLock(
                kvStateLock,
                () -> {
                    checkIfClosed();
                    ResourceGuard.Lease lease = rocksDBKv.getResourceGuard().acquireResource();
                    Snapshot snapshot = null;
                    ReadOptions readOptions = null;
                    RocksIterator iterator = null;
                    boolean success = false;
                    try {
                        snapshot = rocksDBKv.getDb().getSnapshot();
                        // Capture under kvStateLock so the offset matches the data visible through
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
                                        initialAccessTimeMs);
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
                        kvStateLock,
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
            // isClosed rejects operations entering during scheduler shutdown. The write lock here
            // then waits for all point lookups and puts that entered before closure, and remains
            // held throughout native close. Long-lived scanners are protected separately by their
            // ResourceGuard leases. RocksDB metrics lifecycle is managed by TableMetricGroup.
            inWriteLock(kvStateLock, () -> rocksDBKv.close(closeMode));
        }
    }

    /** Completely delete the kv directory and all contents form the file system with no delay. */
    public void drop() throws Exception {
        close(KvCloseMode.DISCARD_UNPERSISTED_STATE);
        FileUtils.deleteDirectory(kvTabletDir);
    }

    @GuardedBy("kvStateLock")
    private void checkIfClosed() {
        if (isClosed) {
            throw new FlussRuntimeException("The KV tablet " + tableBucket + " is already closed");
        }
        rocksDBKv.checkIfRocksDBClosed();
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
    @GuardedBy("kvStateLock")
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
     * machine. All transitions must happen while holding {@code kvStateLock} and are applied
     * through {@code transitionFlushState}, which enforces the source state of each transition.
     */
    enum FlushState {
        IDLE,
        QUEUED,
        RUNNING,
        STORAGE_BLOCKED
    }
}
