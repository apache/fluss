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
import org.apache.fluss.exception.KvStorageException;
import org.apache.fluss.exception.SchemaNotExistException;
import org.apache.fluss.memory.MemorySegmentPool;
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
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.TruncateReason;
import org.apache.fluss.server.kv.rocksdb.RocksDBKv;
import org.apache.fluss.server.kv.rocksdb.RocksDBKvBuilder;
import org.apache.fluss.server.kv.rocksdb.RocksDBResourceContainer;
import org.apache.fluss.server.kv.rowmerger.RowMerger;
import org.apache.fluss.server.kv.snapshot.KvFileHandleAndLocalPath;
import org.apache.fluss.server.kv.snapshot.KvSnapshotDataUploader;
import org.apache.fluss.server.kv.snapshot.RocksIncrementalSnapshot;
import org.apache.fluss.server.kv.wal.ArrowWalBuilder;
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
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.types.Tuple2;

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
    // defines how to merge rows on the same primary key
    private final RowMerger rowMerger;
    private final ArrowCompressionInfo arrowCompressionInfo;

    private final SchemaGetter schemaGetter;

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
            TabletServerMetricGroup serverMetricGroup,
            RocksDBKv rocksDBKv,
            long writeBatchSize,
            LogFormat logFormat,
            BufferAllocator arrowBufferAllocator,
            MemorySegmentPool memorySegmentPool,
            KvFormat kvFormat,
            RowMerger rowMerger,
            ArrowCompressionInfo arrowCompressionInfo,
            SchemaGetter schemaGetter) {
        this.physicalPath = physicalPath;
        this.tableBucket = tableBucket;
        this.logTablet = logTablet;
        this.kvTabletDir = kvTabletDir;
        this.rocksDBKv = rocksDBKv;
        this.writeBatchSize = writeBatchSize;
        this.kvPreWriteBuffer = new KvPreWriteBuffer(createKvBatchWriter(), serverMetricGroup);
        this.logFormat = logFormat;
        this.arrowWriterProvider = new ArrowWriterPool(arrowBufferAllocator);
        this.memorySegmentPool = memorySegmentPool;
        this.kvFormat = kvFormat;
        this.rowMerger = rowMerger;
        this.arrowCompressionInfo = arrowCompressionInfo;
        this.schemaGetter = schemaGetter;
    }

    public static KvTablet create(
            LogTablet logTablet,
            File kvTabletDir,
            Configuration serverConf,
            TabletServerMetricGroup serverMetricGroup,
            BufferAllocator arrowBufferAllocator,
            MemorySegmentPool memorySegmentPool,
            KvFormat kvFormat,
            RowMerger rowMerger,
            ArrowCompressionInfo arrowCompressionInfo,
            SchemaGetter schemaGetter)
            throws IOException {
        Tuple2<PhysicalTablePath, TableBucket> tablePathAndBucket =
                FlussPaths.parseTabletDir(kvTabletDir);
        return create(
                tablePathAndBucket.f0,
                tablePathAndBucket.f1,
                logTablet,
                kvTabletDir,
                serverConf,
                serverMetricGroup,
                arrowBufferAllocator,
                memorySegmentPool,
                kvFormat,
                rowMerger,
                arrowCompressionInfo,
                schemaGetter);
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
            SchemaGetter schemaGetter)
            throws IOException {
        RocksDBKv kv = buildRocksDBKv(serverConf, kvTabletDir);
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
                schemaGetter);
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
     */
    public LogAppendInfo putAsLeader(KvRecordBatch kvRecords, @Nullable int[] targetColumns)
            throws Exception {
        return inWriteLock(
                kvLock,
                () -> {
                    rocksDBKv.checkIfRocksDBClosed();

                    SchemaInfo schemaInfo = schemaGetter.getLatestSchemaInfo();
                    Schema latestSchema = schemaInfo.getSchema();
                    short latestSchemaId = (short) schemaInfo.getSchemaId();
                    short schemaIdOfNewData = kvRecords.schemaId();
                    if (schemaIdOfNewData > latestSchemaId || schemaIdOfNewData < 0) {
                        // TODO: we may need to support retriable exception here
                        throw new SchemaNotExistException(
                                "Invalid schema id: "
                                        + schemaIdOfNewData
                                        + ", latest schema id: "
                                        + latestSchemaId);
                    }

                    // we only support ADD COLUMN, so targetColumns is fine to be used directly
                    RowMerger currentMerger =
                            rowMerger.configureTargetColumns(
                                    targetColumns, latestSchemaId, latestSchema);
                    RowType latestRowType = latestSchema.getRowType();
                    WalBuilder walBuilder = createWalBuilder(latestSchemaId, latestRowType);
                    walBuilder.setWriterState(kvRecords.writerId(), kvRecords.batchSequence());
                    // we only support ADD COLUMN LAST, so the BinaryRow after RowMerger is
                    // only has fewer ending columns than latest schema, so we pad nulls to
                    // the end of the BinaryRow to get the latest schema row.
                    PaddingRow latestSchemaRow = new PaddingRow(latestRowType.getFieldCount());
                    // get offset to track the offset corresponded to the kv record
                    long logEndOffsetOfPrevBatch = logTablet.localLogEndOffset();
                    try {
                        long logOffset = logEndOffsetOfPrevBatch;

                        // TODO: reuse the read context and decoder
                        KvRecordBatch.ReadContext readContext =
                                KvRecordReadContext.createReadContext(kvFormat, schemaGetter);
                        ValueDecoder valueDecoder = new ValueDecoder(schemaGetter, kvFormat);
                        for (KvRecord kvRecord : kvRecords.records(readContext)) {
                            byte[] keyBytes = BytesUtils.toArray(kvRecord.getKey());
                            KvPreWriteBuffer.Key key = KvPreWriteBuffer.Key.of(keyBytes);
                            BinaryRow row = kvRecord.getRow();
                            BinaryValue currentValue =
                                    row == null ? null : new BinaryValue(schemaIdOfNewData, row);
                            if (currentValue == null) {
                                DeleteBehavior deleteBehavior = currentMerger.deleteBehavior();
                                if (deleteBehavior == DeleteBehavior.IGNORE) {
                                    // skip delete rows if the merger doesn't support yet
                                    continue;
                                } else if (deleteBehavior == DeleteBehavior.DISABLE) {
                                    throw new DeletionDisabledException(
                                            "Delete operations are disabled for this table. "
                                                    + "The table.delete.behavior is set to 'disable'.");
                                }
                                // it's for deletion
                                byte[] oldValueBytes = getFromBufferOrKv(key);

                                if (oldValueBytes == null) {
                                    // there might be large amount of such deletion, so we don't log
                                    LOG.debug(
                                            "The specific key can't be found in kv tablet although the kv record is for deletion, "
                                                    + "ignore it directly as it doesn't exist in the kv tablet yet.");
                                } else {
                                    BinaryValue oldValue = valueDecoder.decodeValue(oldValueBytes);
                                    BinaryValue newValue = currentMerger.delete(oldValue);
                                    // if newRow is null, it means the row should be deleted
                                    if (newValue == null) {
                                        walBuilder.append(
                                                ChangeType.DELETE,
                                                latestSchemaRow.replaceRow(oldValue.row));
                                        kvPreWriteBuffer.delete(key, logOffset++);
                                    } else {
                                        // otherwise, it's a partial update, should produce -U,+U
                                        walBuilder.append(
                                                ChangeType.UPDATE_BEFORE,
                                                latestSchemaRow.replaceRow(oldValue.row));
                                        walBuilder.append(
                                                ChangeType.UPDATE_AFTER,
                                                latestSchemaRow.replaceRow(newValue.row));
                                        kvPreWriteBuffer.put(
                                                key, newValue.encodeValue(), logOffset + 1);
                                        logOffset += 2;
                                    }
                                }
                            } else {
                                // upsert operation
                                byte[] oldValueBytes = getFromBufferOrKv(key);
                                // it's update
                                if (oldValueBytes != null) {
                                    BinaryValue oldValue = valueDecoder.decodeValue(oldValueBytes);
                                    BinaryValue newValue =
                                            currentMerger.merge(oldValue, currentValue);
                                    if (newValue == oldValue) {
                                        // newValue is the same to oldValue, means nothing
                                        // happens (no update/delete), and input should be ignored
                                        continue;
                                    }

                                    walBuilder.append(
                                            ChangeType.UPDATE_BEFORE,
                                            latestSchemaRow.replaceRow(oldValue.row));
                                    walBuilder.append(
                                            ChangeType.UPDATE_AFTER,
                                            latestSchemaRow.replaceRow(newValue.row));
                                    // logOffset is for -U, logOffset + 1 is for +U, we need to use
                                    // the log offset for +U
                                    kvPreWriteBuffer.put(
                                            key, newValue.encodeValue(), logOffset + 1);
                                    logOffset += 2;
                                } else {
                                    // it's insert
                                    // TODO: we should add guarantees that all non-specified columns
                                    //  of the input row are set to null.
                                    walBuilder.append(
                                            ChangeType.INSERT,
                                            latestSchemaRow.replaceRow(currentValue.row));
                                    kvPreWriteBuffer.put(
                                            key, currentValue.encodeValue(), logOffset++);
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

    // only for testing.
    @VisibleForTesting
    public RocksDBKv getRocksDBKv() {
        return rocksDBKv;
    }
}
