/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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

package org.apache.fluss.server.index;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.CorruptRecordException;
import org.apache.fluss.exception.KvStorageException;
import org.apache.fluss.exception.StaleMetadataException;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.PartitionTombstone;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.KvRecord;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.KvRecordReadContext;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.decode.KeyDecoder;
import org.apache.fluss.row.decode.RowDecoder;
import org.apache.fluss.server.kv.KvTablet;
import org.apache.fluss.server.log.LogAppendInfo;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.utils.ByteArraySlice;
import org.apache.fluss.utils.BytesUtils;
import org.apache.fluss.utils.IndexTableUtils;

import io.github.fluss_contrib.rocksdb.AbstractCompactionFilter;
import io.github.fluss_contrib.rocksdb.AbstractCompactionFilterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.ToLongFunction;

import static org.apache.fluss.record.LogRecordBatchFormat.NO_BATCH_SEQUENCE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_WRITER_ID;
import static org.apache.fluss.utils.IndexTableUtils.DATA_RECORD_KIND;
import static org.apache.fluss.utils.IndexTableUtils.PROGRESS_RECORD_KIND;
import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Configures the storage behavior of a system-managed index table replica. */
@Internal
public final class IndexTableStorage {

    private static final Logger LOG = LoggerFactory.getLogger(IndexTableStorage.class);

    private final TableInfo tableInfo;
    private final TableBucket tableBucket;
    private final TabletServerMetadataCache metadataCache;
    private final KvRecordBatch.ReadContext readContext;
    private final KeyDecoder keyDecoder;
    private final int partitionIdPosition;
    private final int partitionIdKeyPosition;
    private final int recordKindPosition;
    private final int sourceProgressPosition;

    @Nullable private TombstonedPartitionDiscriminator tombstoneDiscriminator;

    /** Creates the storage policy for an Index Table replica. */
    public IndexTableStorage(
            TableInfo tableInfo,
            TableBucket tableBucket,
            TabletServerMetadataCache metadataCache,
            SchemaGetter schemaGetter) {
        this.tableInfo = tableInfo;
        this.tableBucket = tableBucket;
        this.metadataCache = metadataCache;
        Schema schema = tableInfo.getSchema();
        this.readContext =
                KvRecordReadContext.createReadContext(
                        tableInfo.getTableConfig().getKvFormat(), schemaGetter);
        this.partitionIdPosition =
                schema.getColumnNames().indexOf(IndexTableUtils.PARTITION_ID_SYSTEM_COLUMN);
        this.recordKindPosition =
                schema.getColumnNames().indexOf(IndexTableUtils.RECORD_KIND_SYSTEM_COLUMN);
        this.sourceProgressPosition =
                schema.getColumnNames().indexOf(IndexTableUtils.SOURCE_PROGRESS_SYSTEM_COLUMN);
        List<String> primaryKeys =
                schema.getPrimaryKey()
                        .map(Schema.PrimaryKey::getColumnNames)
                        .orElse(Collections.emptyList());
        this.partitionIdKeyPosition =
                primaryKeys.indexOf(IndexTableUtils.PARTITION_ID_SYSTEM_COLUMN);
        this.keyDecoder =
                tableInfo.isIndexTable()
                        ? KeyDecoder.ofPrimaryKeyDecoder(
                                schema.getRowType(),
                                primaryKeys,
                                tableInfo
                                        .getTableConfig()
                                        .getKvFormatVersion()
                                        .orElse(ConfigOptions.KV_FORMAT_VERSION_2)
                                        .shortValue(),
                                tableInfo.getTableConfig().getDataLakeFormat().orElse(null),
                                tableInfo.isDefaultBucketKey())
                        : null;
    }

    /**
     * Applies one source window under the target tablet lock. The progress row must be the final
     * record of the ordinary KV batch, so a partial RocksDB flush can never expose progress ahead
     * of its data mutations.
     */
    @Nullable
    public LogAppendInfo putIndex(
            KvTablet kvTablet,
            TableBucket sourceBucket,
            long sourceEndOffset,
            byte[] progressKey,
            KvRecordBatch records,
            Callable<LogAppendInfo> append)
            throws Exception {
        AtomicReference<LogAppendInfo> result = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        kvTablet.getGuardedExecutor()
                .execute(
                        () -> {
                            try {
                                result.set(
                                        putIndexLocked(
                                                kvTablet,
                                                sourceBucket,
                                                sourceEndOffset,
                                                progressKey,
                                                records,
                                                append));
                            } catch (Throwable t) {
                                failure.set(t);
                            }
                        });
        Throwable error = failure.get();
        if (error instanceof Exception) {
            throw (Exception) error;
        }
        if (error instanceof Error) {
            throw (Error) error;
        }
        return result.get();
    }

    @Nullable
    private LogAppendInfo putIndexLocked(
            KvTablet kvTablet,
            TableBucket sourceBucket,
            long sourceEndOffset,
            byte[] progressKey,
            KvRecordBatch records,
            Callable<LogAppendInfo> append)
            throws Exception {
        validateSource(sourceBucket);
        if (sourceIsTombstoned(sourceBucket)) {
            return null;
        }
        validateBatch(sourceBucket, sourceEndOffset, progressKey, records);

        long durableProgress =
                readProgress(kvTablet.multiGet(Collections.singletonList(progressKey)));
        if (durableProgress >= sourceEndOffset) {
            return null;
        }
        long visibleProgress =
                readProgress(
                        kvTablet.multiGetFromBufferOrKv(Collections.singletonList(progressKey)));
        if (visibleProgress >= sourceEndOffset) {
            throw new KvStorageException(
                    "Index progress "
                            + visibleProgress
                            + " for source "
                            + sourceBucket
                            + " is awaiting durable flush on "
                            + tableBucket);
        }
        return append.call();
    }

    private void validateSource(TableBucket sourceBucket) {
        checkArgument(tableInfo.isIndexTable(), "%s is not an Index Table", tableBucket);
        checkArgument(
                tableInfo.getMainTableId().getAsLong() == sourceBucket.getTableId(),
                "Index Table %s is not owned by source table %s",
                tableBucket,
                sourceBucket.getTableId());
        checkArgument(
                (partitionIdPosition >= 0) == (sourceBucket.getPartitionId() != null),
                "Source partition identity does not match Index Table schema");
    }

    private boolean sourceIsTombstoned(TableBucket sourceBucket) {
        if (sourceBucket.getPartitionId() == null) {
            return false;
        }
        PartitionTombstone tombstone =
                metadataCache
                        .getInitializedPartitionTombstone(sourceBucket.getTableId())
                        .orElseThrow(
                                () ->
                                        new StaleMetadataException(
                                                "Partition tombstone baseline is not initialized for "
                                                        + sourceBucket.getTableId()));
        return tombstone.isTombstoned(sourceBucket.getPartitionId());
    }

    private void validateBatch(
            TableBucket sourceBucket,
            long sourceEndOffset,
            byte[] progressKey,
            KvRecordBatch records) {
        checkArgument(sourceEndOffset >= 0, "sourceEndOffset must not be negative");
        records.ensureValid();
        if (records.magic() != KvRecordBatch.CURRENT_KV_MAGIC_VALUE
                || records.writerId() != NO_WRITER_ID
                || records.batchSequence() != NO_BATCH_SEQUENCE) {
            throw new CorruptRecordException(
                    "PutIndex accepts only ordinary KV batches without writer state");
        }

        int recordIndex = 0;
        for (KvRecord record : records.records(readContext)) {
            boolean last = recordIndex == records.getRecordCount() - 1;
            byte[] key = BytesUtils.toArray(record.getKey());
            InternalRow physicalKey = keyDecoder.decodeKey(key);
            byte expectedKind = last ? PROGRESS_RECORD_KIND : DATA_RECORD_KIND;
            if (physicalKey.getByte(0) != expectedKind) {
                throw new CorruptRecordException("PutIndex progress must be the final record");
            }
            validatePartition(sourceBucket, physicalKey, partitionIdKeyPosition);

            BinaryRow value = record.getRow();
            if (last) {
                if (!Arrays.equals(key, progressKey) || value == null) {
                    throw new CorruptRecordException("Invalid PutIndex progress record");
                }
                if (value.getByte(recordKindPosition) != PROGRESS_RECORD_KIND
                        || value.isNullAt(sourceProgressPosition)
                        || value.getLong(sourceProgressPosition) != sourceEndOffset) {
                    throw new CorruptRecordException(
                            "PutIndex progress value does not match request");
                }
                validatePartition(sourceBucket, value, partitionIdPosition);
            } else if (value != null) {
                if (value.getByte(recordKindPosition) != DATA_RECORD_KIND) {
                    throw new CorruptRecordException("Invalid PutIndex data record kind");
                }
                validatePartition(sourceBucket, value, partitionIdPosition);
            }
            recordIndex++;
        }
        if (recordIndex == 0 || recordIndex != records.getRecordCount()) {
            throw new CorruptRecordException("PutIndex batch has an invalid record count");
        }
    }

    private static void validatePartition(
            TableBucket sourceBucket, InternalRow row, int partitionPosition) {
        if (sourceBucket.getPartitionId() != null
                && row.getLong(partitionPosition) != sourceBucket.getPartitionId()) {
            throw new CorruptRecordException(
                    "PutIndex record partition does not match its source bucket");
        }
    }

    private long readProgress(List<ByteArraySlice> values) {
        ByteArraySlice value = values.get(0);
        if (value == null) {
            return -1L;
        }
        if (value.length() < Short.BYTES) {
            throw new CorruptRecordException("Stored Index Table progress value is truncated");
        }
        RowDecoder decoder = readContext.getRowDecoder(tableInfo.getSchemaId());
        InternalRow row =
                decoder.decode(
                        MemorySegment.wrap(value.array()),
                        value.offset() + Short.BYTES,
                        value.length() - Short.BYTES);
        if (row.getByte(recordKindPosition) != PROGRESS_RECORD_KIND
                || row.isNullAt(sourceProgressPosition)) {
            throw new CorruptRecordException("Stored Index Table progress row is invalid");
        }
        return row.getLong(sourceProgressPosition);
    }

    /** Prepares compaction and read filtering before the leader KV tablet is opened. */
    public void prepareForLeader() {
        tombstoneDiscriminator =
                TombstonedPartitionDiscriminator.forIndexTable(tableInfo, metadataCache);
    }

    /** Clears state retained only while this replica is leader. */
    public void clearLeaderState() {
        tombstoneDiscriminator = null;
    }

    /** Creates the native partition-tombstone compaction filter when required. */
    @Nullable
    public AbstractCompactionFilterFactory<? extends AbstractCompactionFilter<?>>
            createCompactionFilterFactory() {
        TombstonedPartitionDiscriminator discriminator = tombstoneDiscriminator;
        return discriminator == null ? null : discriminator.createCompactionFilterFactory();
    }

    /** Creates the value-tag extractor used by tagged Index Table values. */
    @Nullable
    public ToLongFunction<BinaryRow> createTagExtractor() {
        TombstonedPartitionDiscriminator discriminator = tombstoneDiscriminator;
        return discriminator == null ? null : discriminator.createTagExtractor();
    }

    /** Installs the partition-tombstone filter after the leader KV tablet is opened. */
    public void installValueFilter(KvTablet kvTablet) {
        TombstonedPartitionDiscriminator discriminator = tombstoneDiscriminator;
        if (discriminator == null) {
            return;
        }
        kvTablet.setValueFilter(discriminator::isTombstoned);
        LOG.info("Index Table partition-tombstone filter installed for {}.", tableBucket);
    }
}
