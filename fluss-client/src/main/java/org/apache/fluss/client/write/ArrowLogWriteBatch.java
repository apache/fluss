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

package org.apache.fluss.client.write;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.ConfigOptions.BatchDeduplicationMode;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.memory.AbstractPagedOutputView;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecordBatchStatisticsCollector;
import org.apache.fluss.record.MemoryLogRecordsArrowBuilder;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.arrow.ArrowWriter;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.rpc.messages.ProduceLogRequest;
import org.apache.fluss.utils.ByteArrayWrapper;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * A batch of log records managed in ARROW format that is or will be sent to server by {@link
 * ProduceLogRequest}.
 *
 * <p>This class is not thread safe and external synchronization must be used when modifying it.
 */
@NotThreadSafe
@Internal
public class ArrowLogWriteBatch extends WriteBatch {
    private final MemoryLogRecordsArrowBuilder recordsBuilder;
    private final AbstractPagedOutputView outputView;
    private final BatchDeduplicationMode deduplicationMode;
    @Nullable private KeyEncoder keyEncoder;
    @Nullable private byte[] previousKey;
    @Nullable private Set<ByteArrayWrapper> seenKeys;

    public ArrowLogWriteBatch(
            long tableId,
            int bucketId,
            int bucketCount,
            PhysicalTablePath physicalTablePath,
            int schemaId,
            ArrowWriter arrowWriter,
            AbstractPagedOutputView outputView,
            boolean isHistoricalPartition,
            long createdMs,
            @Nullable LogRecordBatchStatisticsCollector statisticsCollector,
            BatchDeduplicationMode deduplicationMode) {
        super(
                tableId,
                bucketId,
                bucketCount,
                physicalTablePath,
                schemaId,
                WriteFormat.ARROW_LOG,
                isHistoricalPartition,
                createdMs);
        this.outputView = outputView;
        this.deduplicationMode = deduplicationMode;
        this.recordsBuilder =
                MemoryLogRecordsArrowBuilder.builder(
                        schemaId, arrowWriter, outputView, true, statisticsCollector);
    }

    @Override
    public boolean isLogBatch() {
        return true;
    }

    @Override
    boolean tryAppendRecord(WriteRecord writeRecord, WriteCallback callback) throws Exception {
        InternalRow row = writeRecord.getRow();
        checkArgument(
                writeRecord.getTargetColumns() == null,
                "target columns must be null for log record");
        checkArgument(writeRecord.getKey() == null, "key must be null for log record");
        checkNotNull(row, "row must not be null for log record");
        checkNotNull(callback, "write callback must be not null");
        if (recordsBuilder.isClosed() || recordsBuilder.isFull()) {
            return false;
        } else {
            if (deduplicationMode == BatchDeduplicationMode.NONE) {
                recordsBuilder.append(ChangeType.APPEND_ONLY, row);
                recordCount++;
            } else {
                appendDistinct(writeRecord, row);
            }
            callbacks.add(callback);
            return true;
        }
    }

    private void appendDistinct(WriteRecord writeRecord, InternalRow row) throws Exception {
        if (keyEncoder == null) {
            TableInfo table = writeRecord.getTableInfo();
            List<String> keyColumns = new ArrayList<>(table.getDeduplicationKeys());
            keyColumns.removeAll(table.getPartitionKeys());
            keyEncoder =
                    KeyEncoder.ofPrimaryKeyEncoder(
                            table.getRowType(),
                            keyColumns,
                            table.getTableConfig(),
                            table.isDefaultBucketKey());
        }
        byte[] key = keyEncoder.encodeKey(row);
        if (deduplicationMode == BatchDeduplicationMode.ADJACENT) {
            if (Arrays.equals(previousKey, key)) {
                return;
            }
            recordsBuilder.append(ChangeType.APPEND_ONLY, row);
            previousKey = key;
        } else {
            if (seenKeys == null) {
                seenKeys = new HashSet<>();
            }
            ByteArrayWrapper wrapped = new ByteArrayWrapper(key);
            if (seenKeys.contains(wrapped)) {
                return;
            }
            recordsBuilder.append(ChangeType.APPEND_ONLY, row);
            seenKeys.add(wrapped);
        }
        recordCount++;
    }

    @Override
    public BytesView build() {
        try {
            BytesView records = recordsBuilder.build();
            releaseDeduplicationKeys();
            return records;
        } catch (IOException e) {
            throw new FlussRuntimeException("Failed to build memory log records.", e);
        }
    }

    private void releaseDeduplicationKeys() {
        previousKey = null;
        seenKeys = null;
        keyEncoder = null;
    }

    @Override
    public void close() throws Exception {
        recordsBuilder.close();
        releaseDeduplicationKeys();
        reopened = false;
    }

    @Override
    public boolean isClosed() {
        return recordsBuilder.isClosed();
    }

    @Override
    public int estimatedSizeInBytes() {
        return recordsBuilder.estimatedSizeInBytes();
    }

    @Override
    public List<MemorySegment> pooledMemorySegments() {
        return outputView.allocatedPooledSegments();
    }

    @Override
    public void setWriterState(long writerId, int batchSequence) {
        recordsBuilder.setWriterState(writerId, batchSequence);
    }

    @Override
    public void resetWriterState(long writerId, int batchSequence) {
        super.resetWriterState(writerId, batchSequence);
        recordsBuilder.setWriterState(writerId, batchSequence);
    }

    @Override
    public long writerId() {
        return recordsBuilder.writerId();
    }

    @Override
    public int batchSequence() {
        return recordsBuilder.batchSequence();
    }

    @Override
    public void abortRecordAppends() {
        recordsBuilder.abort();
        releaseDeduplicationKeys();
    }
}
