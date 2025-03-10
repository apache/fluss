/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.kv;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.compression.ArrowCompressionInfo;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.KvStorageException;
import com.alibaba.fluss.memory.MemorySegmentPool;
import com.alibaba.fluss.metadata.KvFormat;
import com.alibaba.fluss.metadata.LogFormat;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.metrics.MeterView;
import com.alibaba.fluss.metrics.MetricNames;
import com.alibaba.fluss.metrics.groups.MetricGroup;
import com.alibaba.fluss.record.KvRecord;
import com.alibaba.fluss.record.KvRecordBatch;
import com.alibaba.fluss.record.KvRecordReadContext;
import com.alibaba.fluss.record.RowKind;
import com.alibaba.fluss.row.BinaryRow;
import com.alibaba.fluss.row.arrow.ArrowWriterPool;
import com.alibaba.fluss.row.arrow.ArrowWriterProvider;
import com.alibaba.fluss.row.encode.ValueDecoder;
import com.alibaba.fluss.row.encode.ValueEncoder;
import com.alibaba.fluss.server.kv.prewrite.KvPreWriteBuffer;
import com.alibaba.fluss.server.kv.prewrite.KvPreWriteBuffer.TruncateReason;
import com.alibaba.fluss.server.kv.rocksdb.RocksDBKv;
import com.alibaba.fluss.server.kv.rocksdb.RocksDBKvBuilder;
import com.alibaba.fluss.server.kv.rocksdb.RocksDBResourceContainer;
import com.alibaba.fluss.server.kv.rowmerger.RowMerger;
import com.alibaba.fluss.server.kv.snapshot.KvFileHandleAndLocalPath;
import com.alibaba.fluss.server.kv.snapshot.KvSnapshotDataUploader;
import com.alibaba.fluss.server.kv.snapshot.RocksIncrementalSnapshot;
import com.alibaba.fluss.server.kv.wal.ArrowWalBuilder;
import com.alibaba.fluss.server.kv.wal.IndexWalBuilder;
import com.alibaba.fluss.server.kv.wal.WalBuilder;
import com.alibaba.fluss.server.log.LogAppendInfo;
import com.alibaba.fluss.server.log.LogTablet;
import com.alibaba.fluss.server.metrics.group.BucketMetricGroup;
import com.alibaba.fluss.server.utils.FatalErrorHandler;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.BytesUtils;
import com.alibaba.fluss.utils.FileUtils;
import com.alibaba.fluss.utils.FlussPaths;
import com.alibaba.fluss.utils.types.Tuple2;

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

import static com.alibaba.fluss.utils.concurrent.LockUtils.inReadLock;
import static com.alibaba.fluss.utils.concurrent.LockUtils.inWriteLock;

/** A kv tablet which presents a unified view of kv storage. */
@ThreadSafe
public final class KvTablet {
    private static final Logger LOG = LoggerFactory.getLogger(KvTablet.class);

    private final PhysicalTablePath physicalPath;
    private final TableBucket tableBucket;

    private final LogTablet logTablet;
    private final ArrowWriterProvider arrowWriterProvider;
    private final MemorySegmentPool memorySegmentPool;

    private final File kvTabletDir;
    private final long writeBatchSize;
    private final RocksDBKv rocksDBKv;
    private final KvPreWriteBuffer kvPreWriteBuffer;

    // A lock that guards all modifications to the kv.
    private final ReadWriteLock kvLock = new ReentrantReadWriteLock();
    private final LogFormat logFormat;
    private final KvFormat kvFormat;
    private final Schema schema;
    // defines how to merge rows on the same primary key
    private final RowMerger rowMerger;
    private final ArrowCompressionInfo arrowCompressionInfo;

    /**
     * The kv data in pre-write buffer whose log offset is less than the flushedLogOffset has been
     * flushed into kv.
     */
    private volatile long flushedLogOffset = 0;

    @GuardedBy("kvLock")
    private volatile boolean isClosed = false;

    private KvTablet(
            PhysicalTablePath physicalPath,
            TableBucket tableBucket,
            LogTablet logTablet,
            File kvTabletDir,
            RocksDBKv rocksDBKv,
            long writeBatchSize,
            LogFormat logFormat,
            BufferAllocator arrowBufferAllocator,
            MemorySegmentPool memorySegmentPool,
            KvFormat kvFormat,
            Schema schema,
            RowMerger rowMerger,
            ArrowCompressionInfo arrowCompressionInfo) {
        this.physicalPath = physicalPath;
        this.tableBucket = tableBucket;
        this.logTablet = logTablet;
        this.kvTabletDir = kvTabletDir;
        this.rocksDBKv = rocksDBKv;
        this.writeBatchSize = writeBatchSize;
        this.kvPreWriteBuffer = new KvPreWriteBuffer(createKvBatchWriter());
        this.logFormat = logFormat;
        this.arrowWriterProvider = new ArrowWriterPool(arrowBufferAllocator);
        this.memorySegmentPool = memorySegmentPool;
        this.kvFormat = kvFormat;
        this.schema = schema;
        this.rowMerger = rowMerger;
        this.arrowCompressionInfo = arrowCompressionInfo;
    }

    public static KvTablet create(
            LogTablet logTablet,
            File kvTabletDir,
            Configuration serverConf,
            BufferAllocator arrowBufferAllocator,
            MemorySegmentPool memorySegmentPool,
            KvFormat kvFormat,
            Schema schema,
            RowMerger rowMerger,
            ArrowCompressionInfo arrowCompressionInfo)
            throws IOException {
        Tuple2<PhysicalTablePath, TableBucket> tablePathAndBucket =
                FlussPaths.parseTabletDir(kvTabletDir);
        return create(
                tablePathAndBucket.f0,
                tablePathAndBucket.f1,
                logTablet,
                kvTabletDir,
                serverConf,
                arrowBufferAllocator,
                memorySegmentPool,
                kvFormat,
                schema,
                rowMerger,
                arrowCompressionInfo);
    }

    public static KvTablet create(
            PhysicalTablePath tablePath,
            TableBucket tableBucket,
            LogTablet logTablet,
            File kvTabletDir,
            Configuration serverConf,
            BufferAllocator arrowBufferAllocator,
            MemorySegmentPool memorySegmentPool,
            KvFormat kvFormat,
            Schema schema,
            RowMerger rowMerger,
            ArrowCompressionInfo arrowCompressionInfo)
            throws IOException {
        RocksDBKv kv = buildRocksDBKv(serverConf, kvTabletDir);
        return new KvTablet(
                tablePath,
                tableBucket,
                logTablet,
                kvTabletDir,
                kv,
                serverConf.get(ConfigOptions.KV_WRITE_BATCH_SIZE).getBytes(),
                logTablet.getLogFormat(),
                arrowBufferAllocator,
                memorySegmentPool,
                kvFormat,
                schema,
                rowMerger,
                arrowCompressionInfo);
    }

    private static RocksDBKv buildRocksDBKv(Configuration configuration, File kvDir)
            throws IOException {
        RocksDBResourceContainer rocksDBResourceContainer =
                new RocksDBResourceContainer(configuration, kvDir);
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

    @Nullable
    public String getPartitionName() {
        return physicalPath.getPartitionName();
    }

    public File getKvTabletDir() {
        return kvTabletDir;
    }

    void setFlushedLogOffset(long flushedLogOffset) {
        this.flushedLogOffset = flushedLogOffset;
    }

    public long getFlushedLogOffset() {
        return flushedLogOffset;
    }

    public void registerMetrics(BucketMetricGroup bucketMetricGroup) {
        MetricGroup metricGroup = bucketMetricGroup.addGroup("kv");

        // about pre-write buffer.
        metricGroup.meter(
                MetricNames.KV_PRE_WRITE_BUFFER_FLUSH_RATE,
                new MeterView(kvPreWriteBuffer.getFlushCount()));
        metricGroup.histogram(
                MetricNames.KV_PRE_WRITE_BUFFER_FLUSH_LATENCY_MS,
                kvPreWriteBuffer.getFlushLatencyHistogram());
        metricGroup.meter(
                MetricNames.KV_PRE_WRITE_BUFFER_TRUNCATE_AS_DUPLICATED_RATE,
                new MeterView(kvPreWriteBuffer.getTruncateAsDuplicatedCount()));
        metricGroup.meter(
                MetricNames.KV_PRE_WRITE_BUFFER_TRUNCATE_AS_ERROR_RATE,
                new MeterView(kvPreWriteBuffer.getTruncateAsErrorCount()));
    }

    /**
     * Put the KvRecordBatch into the kv storage, and return the appended wal log info.
     *
     * @param kvRecords the kv records to put into
     * @param targetColumns the target columns to put, null if put all columns
     */
    public LogAppendInfo putAsLeader(KvRecordBatch kvRecords, @Nullable int[] targetColumns)
            throws Exception {
        return inWriteLock(
                kvLock,
                () -> {
                    rocksDBKv.checkIfRocksDBClosed();
                    short schemaId = kvRecords.schemaId();
                    RowMerger currentMerger = rowMerger.configureTargetColumns(targetColumns);
                    RowType rowType = schema.getRowType();
                    WalBuilder walBuilder = createWalBuilder(schemaId, rowType);
                    walBuilder.setWriterState(kvRecords.writerId(), kvRecords.batchSequence());
                    // get offset to track the offset corresponded to the kv record
                    long logEndOffsetOfPrevBatch = logTablet.localLogEndOffset();
                    DataType[] fieldTypes = rowType.getChildren().toArray(new DataType[0]);
                    try {
                        long logOffset = logEndOffsetOfPrevBatch;

                        // TODO: reuse the read context and decoder
                        KvRecordBatch.ReadContext readContext =
                                KvRecordReadContext.createReadContext(kvFormat, fieldTypes);
                        ValueDecoder valueDecoder =
                                new ValueDecoder(readContext.getRowDecoder(schemaId));
                        for (KvRecord kvRecord : kvRecords.records(readContext)) {
                            byte[] keyBytes = BytesUtils.toArray(kvRecord.getKey());
                            KvPreWriteBuffer.Key key = KvPreWriteBuffer.Key.of(keyBytes);
                            if (kvRecord.getRow() == null) {
                                if (!rowMerger.supportsDelete()) {
                                    // skip delete rows if the merger doesn't support yet
                                    continue;
                                }
                                // it's for deletion
                                byte[] oldValue = getFromBufferOrKv(key);
                                if (oldValue == null) {
                                    // there might be large amount of such deletion, so we don't log
                                    LOG.debug(
                                            "The specific key can't be found in kv tablet although the kv record is for deletion, "
                                                    + "ignore it directly as it doesn't exist in the kv tablet yet.");
                                } else {
                                    BinaryRow oldRow = valueDecoder.decodeValue(oldValue).row;
                                    BinaryRow newRow = currentMerger.delete(oldRow);
                                    // if newRow is null, it means the row should be deleted
                                    if (newRow == null) {
                                        walBuilder.append(RowKind.DELETE, oldRow);
                                        kvPreWriteBuffer.delete(key, logOffset++);
                                    } else {
                                        // otherwise, it's a partial update, should produce -U,+U
                                        walBuilder.append(RowKind.UPDATE_BEFORE, oldRow);
                                        walBuilder.append(RowKind.UPDATE_AFTER, newRow);
                                        kvPreWriteBuffer.put(
                                                key,
                                                ValueEncoder.encodeValue(schemaId, newRow),
                                                logOffset + 1);
                                        logOffset += 2;
                                    }
                                }
                            } else {
                                // upsert operation
                                byte[] oldValue = getFromBufferOrKv(key);
                                // it's update
                                if (oldValue != null) {
                                    BinaryRow oldRow = valueDecoder.decodeValue(oldValue).row;
                                    BinaryRow newRow =
                                            currentMerger.merge(oldRow, kvRecord.getRow());
                                    if (newRow == oldRow) {
                                        // newRow is the same to oldRow, means nothing
                                        // happens (no update/delete), and input should be ignored
                                        continue;
                                    }
                                    walBuilder.append(RowKind.UPDATE_BEFORE, oldRow);
                                    walBuilder.append(RowKind.UPDATE_AFTER, newRow);
                                    // logOffset is for -U, logOffset + 1 is for +U, we need to use
                                    // the log offset for +U
                                    kvPreWriteBuffer.put(
                                            key,
                                            ValueEncoder.encodeValue(schemaId, newRow),
                                            logOffset + 1);
                                    logOffset += 2;
                                } else {
                                    // it's insert
                                    // TODO: we should add guarantees that all non-specified columns
                                    //  of the input row are set to null.
                                    BinaryRow newRow = kvRecord.getRow();
                                    walBuilder.append(RowKind.INSERT, newRow);
                                    kvPreWriteBuffer.put(
                                            key,
                                            ValueEncoder.encodeValue(schemaId, newRow),
                                            logOffset++);
                                }
                            }
                        }

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
                            kvPreWriteBuffer.truncateTo(
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
                        kvPreWriteBuffer.truncateTo(logEndOffsetOfPrevBatch, TruncateReason.ERROR);
                        throw t;
                    } finally {
                        // deallocate the memory and arrow writer used by the wal builder
                        walBuilder.deallocate();
                    }
                });
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

    public void flush(long exclusiveUpToLogOffset, FatalErrorHandler fatalErrorHandler) {
        // todo: need to introduce a backpressure mechanism
        // to avoid too much records in kvPreWriteBuffer
        inWriteLock(
                kvLock,
                () -> {
                    // when kv manager is closed which means kv tablet is already closed,
                    // but the tablet server may still handle fetch log request from follower
                    // as the tablet rpc service is closed asynchronously, then update the watermark
                    // and then flush the pre-write buffer.

                    // In such case, if the tablet is already closed, we won't flush pre-write
                    // buffer, just warning it.
                    if (isClosed) {
                        LOG.warn(
                                "The kv tablet for {} is already closed, ignore flushing kv pre-write buffer.",
                                tableBucket);
                    } else {
                        try {
                            kvPreWriteBuffer.flush(exclusiveUpToLogOffset);
                            flushedLogOffset = exclusiveUpToLogOffset;
                        } catch (Throwable t) {
                            fatalErrorHandler.onFatalError(
                                    new KvStorageException("Failed to flush kv pre-write buffer."));
                        }
                    }
                });
    }

    /** put key,value,logOffset into pre-write buffer directly. */
    void putToPreWriteBuffer(byte[] key, @Nullable byte[] value, long logOffset) {
        KvPreWriteBuffer.Key wrapKey = KvPreWriteBuffer.Key.of(key);
        if (value == null) {
            kvPreWriteBuffer.delete(wrapKey, logOffset);
        } else {
            kvPreWriteBuffer.put(wrapKey, value, logOffset);
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

    // get from kv pre-write buffer first, if can't find, get from rocksdb
    private byte[] getFromBufferOrKv(KvPreWriteBuffer.Key key) throws IOException {
        KvPreWriteBuffer.Value value = kvPreWriteBuffer.get(key);
        if (value == null) {
            return rocksDBKv.get(key.get());
        }
        return value.get();
    }

    public List<byte[]> multiGet(List<byte[]> keys) throws IOException {
        return inReadLock(
                kvLock,
                () -> {
                    rocksDBKv.checkIfRocksDBClosed();
                    return rocksDBKv.multiGet(keys);
                });
    }

    public List<byte[]> prefixLookup(byte[] prefixKey) throws IOException {
        return inReadLock(
                kvLock,
                () -> {
                    rocksDBKv.checkIfRocksDBClosed();
                    return rocksDBKv.prefixLookup(prefixKey);
                });
    }

    public List<byte[]> limitScan(int limit) throws IOException {
        return inReadLock(
                kvLock,
                () -> {
                    rocksDBKv.checkIfRocksDBClosed();
                    return rocksDBKv.limitScan(limit);
                });
    }

    public KvBatchWriter createKvBatchWriter() {
        return rocksDBKv.newWriteBatch(writeBatchSize);
    }

    public void close() throws Exception {
        LOG.info("close kv tablet {} for table {}.", tableBucket, physicalPath);
        inWriteLock(
                kvLock,
                () -> {
                    if (isClosed) {
                        return;
                    }
                    if (rocksDBKv != null) {
                        rocksDBKv.close();
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
}
