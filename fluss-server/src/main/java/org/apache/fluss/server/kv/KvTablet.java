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
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.exception.KvStorageException;
import org.apache.fluss.exception.SchemaNotExistException;
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
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.TruncateReason;
import org.apache.fluss.server.kv.rocksdb.RocksDBKv;
import org.apache.fluss.server.kv.rocksdb.RocksDBKvBuilder;
import org.apache.fluss.server.kv.rocksdb.RocksDBResourceContainer;
import org.apache.fluss.server.kv.rocksdb.RocksDBStatistics;
import org.apache.fluss.server.kv.rowmerger.DefaultRowMerger;
import org.apache.fluss.server.kv.rowmerger.RowMerger;
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
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.BytesUtils;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.clock.Clock;

import org.rocksdb.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.fluss.utils.concurrent.LockUtils.inReadLock;
import static org.apache.fluss.utils.concurrent.LockUtils.inWriteLock;

/** A kv tablet which presents a unified view of kv storage. */
@ThreadSafe
public final class KvTablet {
    private static final Logger LOG = LoggerFactory.getLogger(KvTablet.class);
    public static final long ROW_COUNT_DISABLED = -1;

    /**
     * A guard that prevents the underlying RocksDB from being released while held. Use with
     * try-with-resources. In eager mode, this is a no-op. In lazy mode, it pins the RocksDB
     * instance and ensures it is open before returning.
     */
    public static final class Guard implements AutoCloseable {
        private final @Nullable KvTablet tablet;
        private boolean released;

        /** No-op guard for eager mode. */
        static final Guard NOOP = new Guard(null);

        Guard(@Nullable KvTablet tablet) {
            this.tablet = tablet;
        }

        @Override
        public void close() {
            if (!released && tablet != null) {
                released = true;
                tablet.releasePin();
            }
        }
    }

    private final PhysicalTablePath physicalPath;
    private final TableBucket tableBucket;

    /** Package-private for {@link KvTabletLazyLifecycle} access (log end offset during reopen). */
    final LogTablet logTablet;

    /** Package-private for {@link KvTabletLazyLifecycle} access (gauge updates). */
    final @Nullable TabletServerMetricGroup serverMetricGroup;

    // A lock that guards all modifications to the kv.
    private final ReadWriteLock kvLock = new ReentrantReadWriteLock();

    // ---- Immutable holder for all RocksDB-related fields ----
    // In EAGER mode all fields are final (JIT can cache in registers).
    // In LAZY mode a single volatile write publishes the entire state atomically.

    private volatile @Nullable RocksDBState rocksDBState;

    /**
     * The kv data in pre-write buffer whose log offset is less than the flushedLogOffset has been
     * flushed into kv. Package-private volatile for cross-thread access by {@link
     * KvTabletLazyLifecycle} during release (caching metadata).
     */
    volatile long flushedLogOffset = 0;

    /** Current row count. Package-private volatile for {@link KvTabletLazyLifecycle} access. */
    volatile long rowCount;

    @GuardedBy("kvLock")
    private volatile boolean isClosed = false;

    // ---- Lazy lifecycle (null in EAGER mode) ----

    private final @Nullable KvTabletLazyLifecycle lifecycle;

    /** Full constructor for eager mode (RocksDB is immediately available). */
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
            AutoIncrementManager autoIncrementManager) {
        this.physicalPath = physicalPath;
        this.tableBucket = tableBucket;
        this.logTablet = logTablet;
        this.serverMetricGroup = serverMetricGroup;
        // Pre-create DefaultRowMerger for OVERWRITE mode to avoid creating new instances
        // on every putAsLeader call. Used for undo recovery scenarios.
        RowMerger overwriteRowMerger = new DefaultRowMerger(kvFormat, DeleteBehavior.ALLOW);
        ArrowWriterProvider arrowWriterProvider = new ArrowWriterPool(arrowBufferAllocator);
        KvPreWriteBuffer kvPreWriteBuffer =
                new KvPreWriteBuffer(
                        rocksDBKv.newWriteBatch(
                                writeBatchSize,
                                serverMetricGroup.kvFlushCount(),
                                serverMetricGroup.kvFlushLatencyHistogram()),
                        serverMetricGroup);
        this.rocksDBState =
                new RocksDBState(
                        kvTabletDir,
                        rocksDBKv,
                        writeBatchSize,
                        kvPreWriteBuffer,
                        logFormat,
                        kvFormat,
                        arrowWriterProvider,
                        memorySegmentPool,
                        rowMerger,
                        overwriteRowMerger,
                        arrowCompressionInfo,
                        schemaGetter,
                        changelogImage,
                        rocksDBStatistics,
                        autoIncrementManager);
        // disable row count for WAL image mode.
        this.rowCount = changelogImage == ChangelogImage.WAL ? ROW_COUNT_DISABLED : 0L;
        this.lifecycle = null;
    }

    /**
     * Lazy sentinel constructor: creates a KvTablet in LAZY state without RocksDB. The RocksDB
     * fields are populated later via {@link #installRocksDB(KvTablet)} when ensureOpen() triggers.
     */
    private KvTablet(
            PhysicalTablePath physicalPath,
            TableBucket tableBucket,
            LogTablet logTablet,
            @Nullable TabletServerMetricGroup serverMetricGroup) {
        this.physicalPath = physicalPath;
        this.tableBucket = tableBucket;
        this.logTablet = logTablet;
        this.serverMetricGroup = serverMetricGroup;
        this.lifecycle = new KvTabletLazyLifecycle(this);
    }

    /**
     * Create a lazy sentinel KvTablet that starts in LAZY state without RocksDB. Use {@link
     * KvTabletLazyLifecycle#configureLazyOpen} and callback setters on the lifecycle to configure
     * before use.
     */
    public static KvTablet createLazySentinel(
            PhysicalTablePath physicalPath,
            TableBucket tableBucket,
            LogTablet logTablet,
            @Nullable TabletServerMetricGroup serverMetricGroup) {
        return new KvTablet(physicalPath, tableBucket, logTablet, serverMetricGroup);
    }

    /** Returns the lifecycle manager, or null if this tablet is in EAGER mode. */
    @Nullable
    public KvTabletLazyLifecycle getLifecycle() {
        return lifecycle;
    }

    /**
     * Install RocksDB state from a fully-constructed KvTablet into this lazy sentinel. Called by
     * commitOpenResult() after a successful open. The source tablet is consumed (its RocksDB
     * reference is transferred, not copied).
     */
    void installRocksDB(KvTablet source) {
        this.rocksDBState = source.rocksDBState;
        this.rowCount = source.rowCount;
        this.flushedLogOffset = source.flushedLogOffset;
        this.isClosed = false;
        source.rocksDBState = null;
        source.isClosed = true;
    }

    /**
     * Detach RocksDB state from this sentinel (during release). Nulls out RocksDB-related fields so
     * the sentinel returns to a lightweight state.
     */
    void detachRocksDB() {
        RocksDBState s = this.rocksDBState;
        if (s != null) {
            closeRocksDBState(s, "detach");
        }
        this.rocksDBState = null;
        this.isClosed = true;
    }

    /**
     * Returns the current RocksDB state, throwing if not available. This provides a clear error
     * message when RocksDB is not open (e.g. lazy mode without guard).
     */
    private RocksDBState requireRocksDBState() {
        RocksDBState s = this.rocksDBState;
        if (s == null) {
            throw new KvStorageException(
                    "RocksDB is not open for "
                            + tableBucket
                            + ". Ensure a Guard is acquired before performing KV operations.");
        }
        return s;
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
                        rocksDBResourceContainer.getColumnOptions());
        return rocksDBKvBuilder.build();
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }

    public TablePath getTablePath() {
        return physicalPath.getTablePath();
    }

    public long getAutoIncrementCacheSize() {
        RocksDBState s = this.rocksDBState;
        return s != null ? s.autoIncrementManager.getAutoIncrementCacheSize() : 0;
    }

    public void updateAutoIncrementIDRange(AutoIncIDRange newRange) {
        RocksDBState s = this.rocksDBState;
        if (s != null) {
            s.autoIncrementManager.updateIDRange(newRange);
        }
    }

    @Nullable
    public String getPartitionName() {
        return physicalPath.getPartitionName();
    }

    public File getKvTabletDir() {
        RocksDBState s = this.rocksDBState;
        return s != null ? s.kvTabletDir : null;
    }

    /**
     * Get RocksDB statistics accessor for this tablet.
     *
     * @return the RocksDB statistics accessor, or null if not available
     */
    @Nullable
    public RocksDBStatistics getRocksDBStatistics() {
        RocksDBState s = this.rocksDBState;
        return s != null ? s.rocksDBStatistics : null;
    }

    /** Returns the log offset up to which data has been flushed from pre-write buffer into KV. */
    public long getFlushedLogOffset() {
        return flushedLogOffset;
    }

    void setFlushedLogOffset(long flushedLogOffset) {
        this.flushedLogOffset = flushedLogOffset;
    }

    void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }

    // row_count is volatile, so it's safe to read without lock
    public long getRowCount() {
        // In LAZY state, RocksDB is not open — return cached value
        if (lifecycle != null && lifecycle.needsCachedRowCount()) {
            long cached = lifecycle.getCachedRowCount();
            if (cached == ROW_COUNT_DISABLED) {
                throw new InvalidTableException(
                        String.format(
                                "Row count is disabled for this table '%s'. "
                                        + "This usually happens when the table is "
                                        + "created before v0.9 or the changelog image is set to WAL.",
                                getTablePath()));
            }
            return cached;
        }
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
     * <p>Note: this method must be called under the kvLock to ensure the consistency between the
     * returned state and the log offset.
     */
    @GuardedBy("kvLock")
    public TabletState getTabletState() {
        RocksDBState s = this.rocksDBState;
        return new TabletState(
                flushedLogOffset,
                rowCount == ROW_COUNT_DISABLED ? null : rowCount,
                s != null ? s.autoIncrementManager.getCurrentIDRanges() : null);
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
        RocksDBState s = requireRocksDBState();
        return inWriteLock(
                kvLock,
                () -> {
                    s.rocksDBKv.checkIfRocksDBClosed();

                    SchemaInfo schemaInfo = s.schemaGetter.getLatestSchemaInfo();
                    Schema latestSchema = schemaInfo.getSchema();
                    short latestSchemaId = (short) schemaInfo.getSchemaId();
                    validateSchemaId(kvRecords.schemaId(), latestSchemaId);

                    AutoIncrementUpdater currentAutoIncrementUpdater =
                            s.autoIncrementManager.getUpdaterForSchema(s.kvFormat, latestSchemaId);

                    // Validate targetColumns doesn't contain auto-increment column
                    currentAutoIncrementUpdater.validateTargetColumns(targetColumns);

                    // Determine the row merger based on mergeMode:
                    // - DEFAULT: Use the configured merge engine (rowMerger)
                    // - OVERWRITE: Bypass merge engine, use pre-created overwriteRowMerger
                    //   to directly replace values (for undo recovery scenarios)
                    // We only support ADD COLUMN, so targetColumns is fine to be used directly.
                    RowMerger currentMerger =
                            (mergeMode == MergeMode.OVERWRITE)
                                    ? s.overwriteRowMerger.configureTargetColumns(
                                            targetColumns, latestSchemaId, latestSchema)
                                    : s.rowMerger.configureTargetColumns(
                                            targetColumns, latestSchemaId, latestSchema);

                    RowType latestRowType = latestSchema.getRowType();
                    WalBuilder walBuilder = createWalBuilder(s, latestSchemaId, latestRowType);
                    walBuilder.setWriterState(kvRecords.writerId(), kvRecords.batchSequence());
                    // we only support ADD COLUMN LAST, so the BinaryRow after RowMerger is
                    // only has fewer ending columns than latest schema, so we pad nulls to
                    // the end of the BinaryRow to get the latest schema row.
                    PaddingRow latestSchemaRow = new PaddingRow(latestRowType.getFieldCount());
                    // get offset to track the offset corresponded to the kv record
                    long logEndOffsetOfPrevBatch = logTablet.localLogEndOffset();

                    try {
                        processKvRecords(
                                s,
                                kvRecords,
                                kvRecords.schemaId(),
                                currentMerger,
                                currentAutoIncrementUpdater,
                                walBuilder,
                                latestSchemaRow,
                                logEndOffsetOfPrevBatch);

                        // There will be a situation that these batches of kvRecordBatch have not
                        // generated any CDC logs, for example, when client attempts to delete
                        // some non-existent keys or MergeEngineType set to FIRST_ROW. In this case,
                        // we cannot simply return, as doing so would cause a
                        // OutOfOrderSequenceException problem. Therefore, here we will build an
                        // empty batch with lastLogOffset to 0L as the baseLogOffset is 0L. As doing
                        // that, the logOffsetDelta in logRecordBatch will be set to 0L. So, we will
                        // put a batch into file with recordCount 0 and offset plus 1L, it will
                        // update the batchSequence corresponding to the writerId and also increment
                        // the CDC log offset by 1.
                        LogAppendInfo logAppendInfo = logTablet.appendAsLeader(walBuilder.build());

                        // if the batch is duplicated, we should truncate the kvPreWriteBuffer
                        // already written.
                        if (logAppendInfo.duplicated()) {
                            s.kvPreWriteBuffer.truncateTo(
                                    logEndOffsetOfPrevBatch, TruncateReason.DUPLICATED);
                        }
                        return logAppendInfo;
                    } catch (Throwable t) {
                        // While encounter error here, the CDC logs may fail writing to disk,
                        // and the client probably will resend the batch. If we do not remove the
                        // values generated by the erroneous batch from the kvPreWriteBuffer, the
                        // retry-send batch will produce incorrect CDC logs.
                        // TODO for some errors, the cdc logs may already be written to disk, for
                        //  those errors, we should not truncate the kvPreWriteBuffer.
                        s.kvPreWriteBuffer.truncateTo(
                                logEndOffsetOfPrevBatch, TruncateReason.ERROR);
                        throw t;
                    } finally {
                        // deallocate the memory and arrow writer used by the wal builder
                        walBuilder.deallocate();
                    }
                });
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
            RocksDBState s,
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
                KvRecordReadContext.createReadContext(s.kvFormat, s.schemaGetter);
        ValueDecoder valueDecoder = new ValueDecoder(s.schemaGetter, s.kvFormat);

        for (KvRecord kvRecord : kvRecords.records(readContext)) {
            byte[] keyBytes = BytesUtils.toArray(kvRecord.getKey());
            KvPreWriteBuffer.Key key = KvPreWriteBuffer.Key.of(keyBytes);
            BinaryRow row = kvRecord.getRow();
            BinaryValue currentValue = row == null ? null : new BinaryValue(schemaIdOfNewData, row);

            if (currentValue == null) {
                logOffset =
                        processDeletion(
                                s,
                                key,
                                currentMerger,
                                valueDecoder,
                                walBuilder,
                                latestSchemaRow,
                                logOffset);
            } else {
                logOffset =
                        processUpsert(
                                s,
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
            RocksDBState s,
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

        byte[] oldValueBytes = getFromBufferOrKv(s, key);
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
            return applyDelete(s, key, oldValue, walBuilder, latestSchemaRow, logOffset);
        } else {
            return applyUpdate(s, key, oldValue, newValue, walBuilder, latestSchemaRow, logOffset);
        }
    }

    private long processUpsert(
            RocksDBState s,
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
        if (s.changelogImage == ChangelogImage.WAL
                && !autoIncrementUpdater.hasAutoIncrement()
                && currentMerger instanceof DefaultRowMerger) {
            return applyUpdate(s, key, null, currentValue, walBuilder, latestSchemaRow, logOffset);
        }

        byte[] oldValueBytes = getFromBufferOrKv(s, key);
        if (oldValueBytes == null) {
            return applyInsert(
                    s,
                    key,
                    currentValue,
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

        return applyUpdate(s, key, oldValue, newValue, walBuilder, latestSchemaRow, logOffset);
    }

    private long applyDelete(
            RocksDBState s,
            KvPreWriteBuffer.Key key,
            BinaryValue oldValue,
            WalBuilder walBuilder,
            PaddingRow latestSchemaRow,
            long logOffset)
            throws Exception {
        walBuilder.append(ChangeType.DELETE, latestSchemaRow.replaceRow(oldValue.row));
        s.kvPreWriteBuffer.delete(key, logOffset);
        return logOffset + 1;
    }

    private long applyInsert(
            RocksDBState s,
            KvPreWriteBuffer.Key key,
            BinaryValue currentValue,
            WalBuilder walBuilder,
            PaddingRow latestSchemaRow,
            long logOffset,
            AutoIncrementUpdater autoIncrementUpdater)
            throws Exception {
        BinaryValue newValue = autoIncrementUpdater.updateAutoIncrementColumns(currentValue);
        walBuilder.append(ChangeType.INSERT, latestSchemaRow.replaceRow(newValue.row));
        s.kvPreWriteBuffer.insert(key, newValue.encodeValue(), logOffset);
        return logOffset + 1;
    }

    private long applyUpdate(
            RocksDBState s,
            KvPreWriteBuffer.Key key,
            BinaryValue oldValue,
            BinaryValue newValue,
            WalBuilder walBuilder,
            PaddingRow latestSchemaRow,
            long logOffset)
            throws Exception {
        if (s.changelogImage == ChangelogImage.WAL) {
            walBuilder.append(ChangeType.UPDATE_AFTER, latestSchemaRow.replaceRow(newValue.row));
            s.kvPreWriteBuffer.update(key, newValue.encodeValue(), logOffset);
            return logOffset + 1;
        } else {
            walBuilder.append(ChangeType.UPDATE_BEFORE, latestSchemaRow.replaceRow(oldValue.row));
            walBuilder.append(ChangeType.UPDATE_AFTER, latestSchemaRow.replaceRow(newValue.row));
            s.kvPreWriteBuffer.update(key, newValue.encodeValue(), logOffset + 1);
            return logOffset + 2;
        }
    }

    private WalBuilder createWalBuilder(RocksDBState s, int schemaId, RowType rowType)
            throws Exception {
        switch (s.logFormat) {
            case INDEXED:
                if (s.kvFormat == KvFormat.COMPACTED) {
                    // convert from compacted row to indexed row is time cost, and gain
                    // less benefits, currently we won't support compacted as kv format and
                    // indexed as cdc log format.
                    // so in here we throw exception directly
                    throw new IllegalArgumentException(
                            "Primary Key Table with COMPACTED kv format doesn't support INDEXED cdc log format.");
                }
                return new IndexWalBuilder(schemaId, s.memorySegmentPool);
            case COMPACTED:
                return new CompactedWalBuilder(schemaId, rowType, s.memorySegmentPool);
            case ARROW:
                return new ArrowWalBuilder(
                        schemaId,
                        s.arrowWriterProvider.getOrCreateWriter(
                                tableBucket.getTableId(),
                                schemaId,
                                // we don't limit size of the arrow batch, because all the
                                // changelogs should be in a single batch
                                Integer.MAX_VALUE,
                                rowType,
                                s.arrowCompressionInfo),
                        s.memorySegmentPool);
            default:
                throw new IllegalArgumentException("Unsupported log format: " + s.logFormat);
        }
    }

    public void flush(long exclusiveUpToLogOffset, FatalErrorHandler fatalErrorHandler) {
        Guard guard = tryAcquireExistingGuard();
        if (guard == null) {
            return;
        }
        try (Guard ignored = guard) {
            RocksDBState s = this.rocksDBState;
            if (s == null) {
                return;
            }
            // todo: need to introduce a backpressure mechanism
            // to avoid too much records in kvPreWriteBuffer
            inWriteLock(
                    kvLock,
                    () -> {
                        // when kv manager is closed which means kv tablet is already closed,
                        // but the tablet server may still handle fetch log request from follower
                        // as the tablet rpc service is closed asynchronously, then update the
                        // watermark and then flush the pre-write buffer.

                        // In such case, if the tablet is already closed, we won't flush pre-write
                        // buffer, just warning it.
                        if (isClosed) {
                            LOG.warn(
                                    "The kv tablet for {} is already closed, ignore flushing kv pre-write buffer.",
                                    tableBucket);
                        } else {
                            try {
                                int rowCountDiff = s.kvPreWriteBuffer.flush(exclusiveUpToLogOffset);
                                flushedLogOffset = exclusiveUpToLogOffset;
                                if (rowCount != ROW_COUNT_DISABLED) {
                                    // row count is enabled, we update the row count after flush.
                                    long currentRowCount = rowCount;
                                    rowCount = currentRowCount + rowCountDiff;
                                }
                            } catch (Throwable t) {
                                fatalErrorHandler.onFatalError(
                                        new KvStorageException(
                                                "Failed to flush kv pre-write buffer."));
                            }
                        }
                    });
        }
    }

    /** put key,value,logOffset into pre-write buffer directly. */
    void putToPreWriteBuffer(
            ChangeType changeType, byte[] key, @Nullable byte[] value, long logOffset) {
        RocksDBState s = this.rocksDBState;
        KvPreWriteBuffer.Key wrapKey = KvPreWriteBuffer.Key.of(key);
        if (changeType == ChangeType.DELETE && value == null) {
            s.kvPreWriteBuffer.delete(wrapKey, logOffset);
        } else if (changeType == ChangeType.INSERT) {
            s.kvPreWriteBuffer.insert(wrapKey, value, logOffset);
        } else if (changeType == ChangeType.UPDATE_AFTER) {
            s.kvPreWriteBuffer.update(wrapKey, value, logOffset);
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
        return runnable -> {
            Guard guard = tryAcquireExistingGuard();
            if (guard == null) {
                return;
            }
            try (Guard ignored = guard) {
                inWriteLock(kvLock, runnable::run);
            }
        };
    }

    // get from kv pre-write buffer first, if can't find, get from rocksdb
    private byte[] getFromBufferOrKv(RocksDBState s, KvPreWriteBuffer.Key key) throws IOException {
        KvPreWriteBuffer.Value value = s.kvPreWriteBuffer.get(key);
        if (value == null) {
            return s.rocksDBKv.get(key.get());
        }
        return value.get();
    }

    public List<byte[]> multiGet(List<byte[]> keys) throws IOException {
        RocksDBState s = requireRocksDBState();
        return inReadLock(
                kvLock,
                () -> {
                    s.rocksDBKv.checkIfRocksDBClosed();
                    return s.rocksDBKv.multiGet(keys);
                });
    }

    public List<byte[]> prefixLookup(byte[] prefixKey) throws IOException {
        RocksDBState s = requireRocksDBState();
        return inReadLock(
                kvLock,
                () -> {
                    s.rocksDBKv.checkIfRocksDBClosed();
                    return s.rocksDBKv.prefixLookup(prefixKey);
                });
    }

    public List<byte[]> limitScan(int limit) throws IOException {
        RocksDBState s = requireRocksDBState();
        return inReadLock(
                kvLock,
                () -> {
                    s.rocksDBKv.checkIfRocksDBClosed();
                    return s.rocksDBKv.limitScan(limit);
                });
    }

    public KvBatchWriter createKvBatchWriter() {
        RocksDBState s = requireRocksDBState();
        return s.rocksDBKv.newWriteBatch(
                s.writeBatchSize,
                serverMetricGroup.kvFlushCount(),
                serverMetricGroup.kvFlushLatencyHistogram());
    }

    public void close() throws Exception {
        LOG.info("close kv tablet {} for table {}.", tableBucket, physicalPath);
        if (lifecycle != null) {
            lifecycle.closeKvLazy();
            return;
        }
        inWriteLock(
                kvLock,
                () -> {
                    if (isClosed) {
                        return;
                    }
                    // Note: RocksDB metrics lifecycle is managed by TableMetricGroup
                    // No need to close it here
                    RocksDBState s = this.rocksDBState;
                    if (s != null) {
                        closeRocksDBState(s, "close");
                    }
                    isClosed = true;
                });
    }

    /** Completely delete the kv directory and all contents form the file system with no delay. */
    public void drop() throws Exception {
        inWriteLock(
                kvLock,
                () -> {
                    // first close the kv.
                    close();
                    // then delete the directory.
                    RocksDBState s = this.rocksDBState;
                    if (s != null) {
                        FileUtils.deleteDirectory(s.kvTabletDir);
                    }
                });
    }

    /**
     * Delete the local tablet directory. Prefers {@code kvTabletDir} if available (OPEN state),
     * falls back to lifecycle's tabletDirSupplier (LAZY/FAILED state).
     */
    void deleteLocalDirectory() {
        RocksDBState s = this.rocksDBState;
        File dir = s != null ? s.kvTabletDir : null;
        if (dir == null && lifecycle != null) {
            dir = lifecycle.getTabletDir();
        }
        if (dir != null) {
            FileUtils.deleteDirectoryQuietly(dir);
            LOG.info(
                    "Deleted local data directory for bucket {} at {}.",
                    tableBucket,
                    dir.getAbsolutePath());
        }
    }

    public RocksIncrementalSnapshot createIncrementalSnapshot(
            Map<Long, Collection<KvFileHandleAndLocalPath>> uploadedSstFiles,
            KvSnapshotDataUploader kvSnapshotDataUploader,
            long lastCompletedSnapshotId) {
        RocksDBState s = this.rocksDBState;
        if (s == null) {
            throw new IllegalStateException(
                    "Cannot create incremental snapshot for lazy tablet "
                            + tableBucket
                            + " while closed.");
        }
        return new RocksIncrementalSnapshot(
                uploadedSstFiles,
                s.rocksDBKv.getDb(),
                s.rocksDBKv.getResourceGuard(),
                kvSnapshotDataUploader,
                s.kvTabletDir,
                lastCompletedSnapshotId);
    }

    // only for testing.
    @VisibleForTesting
    KvPreWriteBuffer getKvPreWriteBuffer() {
        RocksDBState s = this.rocksDBState;
        return s != null ? s.kvPreWriteBuffer : null;
    }

    // only for testing.
    @VisibleForTesting
    public RocksDBKv getRocksDBKv() {
        RocksDBState s = this.rocksDBState;
        return s != null ? s.rocksDBKv : null;
    }

    // ========================================================================
    //  Lazy lifecycle delegation
    // ========================================================================

    /** Returns true if this tablet is in lazy mode (as opposed to eager/traditional mode). */
    public boolean isLazyMode() {
        return lifecycle != null;
    }

    /** Returns true if this tablet is in lazy mode and currently OPEN (RocksDB loaded). */
    public boolean isLazyOpen() {
        return lifecycle != null && lifecycle.isOpen();
    }

    /** Returns the current lazy state. */
    public KvTabletLazyLifecycle.LazyState getLazyState() {
        return lifecycle != null ? lifecycle.getLazyState() : null;
    }

    /**
     * Acquire a guard that prevents RocksDB from being released while held. In eager mode, returns
     * a no-op guard immediately. In lazy mode, ensures RocksDB is open (blocking if necessary) and
     * pins it.
     *
     * <p>Must be called OUTSIDE any Replica-level locks (e.g. leaderIsrUpdateLock) to avoid
     * blocking leader transitions during slow opens.
     */
    public Guard acquireGuard() {
        return lifecycle != null ? lifecycle.acquireGuard() : Guard.NOOP;
    }

    /**
     * Try to acquire a guard only if RocksDB is already open.
     *
     * <p>Returns {@code null} for a lazy tablet that is currently LAZY/FAILED/RELEASING/CLOSED.
     */
    @Nullable
    Guard tryAcquireExistingGuard() {
        return lifecycle != null ? lifecycle.tryAcquireExistingGuard() : Guard.NOOP;
    }

    /** Release a pin. Called by Guard.close(). */
    void releasePin() {
        if (lifecycle != null) {
            lifecycle.releasePin();
        }
    }

    /** Pre-check for idle release eligibility. */
    public boolean canRelease(long closeIdleIntervalMs, long nowMs) {
        return lifecycle != null && lifecycle.canRelease(closeIdleIntervalMs, nowMs);
    }

    public boolean releaseKv() {
        return lifecycle != null && lifecycle.releaseKv();
    }

    public void dropKvLazy() {
        if (lifecycle != null) {
            lifecycle.dropKvLazy();
        }
    }

    // ---- Lazy state queries ----

    public long getCachedRowCount() {
        return lifecycle != null ? lifecycle.getCachedRowCount() : 0;
    }

    public long getCachedFlushedLogOffset() {
        return lifecycle != null ? lifecycle.getCachedFlushedLogOffset() : 0;
    }

    public long getLastAccessTimestamp() {
        return lifecycle != null ? lifecycle.getLastAccessTimestamp() : 0;
    }

    /**
     * Returns the number of active pins preventing RocksDB release.
     *
     * <p>This is primarily for testing and monitoring purposes. Exposing internal concurrency state
     * may lead to misuse in production code.
     */
    @VisibleForTesting
    public int getActivePins() {
        return lifecycle != null ? lifecycle.getActivePins() : 0;
    }

    // ---- Test helpers ----

    @VisibleForTesting
    void setLazyStateForTesting(KvTabletLazyLifecycle.LazyState state) {
        if (lifecycle != null) {
            lifecycle.setLazyStateForTesting(state);
        }
    }

    @VisibleForTesting
    void setLastAccessTimestampForTesting(long timestamp) {
        if (lifecycle != null) {
            lifecycle.setLastAccessTimestampForTesting(timestamp);
        }
    }

    @VisibleForTesting
    void setClockForTesting(Clock clock) {
        if (lifecycle != null) {
            lifecycle.setClockForTesting(clock);
        }
    }

    private void closeRocksDBState(RocksDBState state, String operation) {
        try {
            state.kvPreWriteBuffer.close();
        } catch (Exception e) {
            LOG.warn(
                    "Failed to close kv pre-write buffer during {} for {}",
                    operation,
                    tableBucket,
                    e);
        }
        try {
            state.arrowWriterProvider.close();
        } catch (Exception e) {
            LOG.warn(
                    "Failed to close arrow writer pool during {} for {}",
                    operation,
                    tableBucket,
                    e);
        }
        try {
            state.rocksDBKv.close();
        } catch (Exception e) {
            LOG.warn("Failed to close RocksDB during {} for {}", operation, tableBucket, e);
        }
    }

    // ---- Immutable RocksDB state holder ----

    /**
     * Immutable holder for all RocksDB-related fields. All fields are {@code final}, so in EAGER
     * mode the JIT can cache them in registers. In LAZY mode, a single volatile write of the holder
     * reference publishes all fields atomically (instead of 15+ individual volatile writes).
     */
    static final class RocksDBState {
        final File kvTabletDir;
        final RocksDBKv rocksDBKv;
        final long writeBatchSize;
        final KvPreWriteBuffer kvPreWriteBuffer;
        final LogFormat logFormat;
        final KvFormat kvFormat;
        final ArrowWriterProvider arrowWriterProvider;
        final MemorySegmentPool memorySegmentPool;
        final RowMerger rowMerger;
        final RowMerger overwriteRowMerger;
        final ArrowCompressionInfo arrowCompressionInfo;
        final SchemaGetter schemaGetter;
        final ChangelogImage changelogImage;
        final @Nullable RocksDBStatistics rocksDBStatistics;
        final AutoIncrementManager autoIncrementManager;

        RocksDBState(
                File kvTabletDir,
                RocksDBKv rocksDBKv,
                long writeBatchSize,
                KvPreWriteBuffer kvPreWriteBuffer,
                LogFormat logFormat,
                KvFormat kvFormat,
                ArrowWriterProvider arrowWriterProvider,
                MemorySegmentPool memorySegmentPool,
                RowMerger rowMerger,
                RowMerger overwriteRowMerger,
                ArrowCompressionInfo arrowCompressionInfo,
                SchemaGetter schemaGetter,
                ChangelogImage changelogImage,
                @Nullable RocksDBStatistics rocksDBStatistics,
                AutoIncrementManager autoIncrementManager) {
            this.kvTabletDir = kvTabletDir;
            this.rocksDBKv = rocksDBKv;
            this.writeBatchSize = writeBatchSize;
            this.kvPreWriteBuffer = kvPreWriteBuffer;
            this.logFormat = logFormat;
            this.kvFormat = kvFormat;
            this.arrowWriterProvider = arrowWriterProvider;
            this.memorySegmentPool = memorySegmentPool;
            this.rowMerger = rowMerger;
            this.overwriteRowMerger = overwriteRowMerger;
            this.arrowCompressionInfo = arrowCompressionInfo;
            this.schemaGetter = schemaGetter;
            this.changelogImage = changelogImage;
            this.rocksDBStatistics = rocksDBStatistics;
            this.autoIncrementManager = autoIncrementManager;
        }
    }
}
