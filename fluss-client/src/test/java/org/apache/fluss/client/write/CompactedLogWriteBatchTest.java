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

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.memory.PreAllocatedPagedOutputView;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.CompactedLogRecord;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.record.LogRecordBatch.CURRENT_LOG_MAGIC_VALUE;
import static org.apache.fluss.record.LogRecordBatchFormat.recordBatchHeaderSize;
import static org.apache.fluss.record.TestData.DATA1_PHYSICAL_TABLE_PATH;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO;
import static org.apache.fluss.record.TestData.TEST_SCHEMA_GETTER;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.apache.fluss.testutils.DataTestUtils.indexedRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link CompactedLogWriteBatch}. */
public class CompactedLogWriteBatchTest {
    private CompactedRow row;
    private int estimatedSizeInBytes;

    @BeforeEach
    void setup() {
        row = compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a"});
        estimatedSizeInBytes = CompactedLogRecord.sizeOf(row);
    }

    @Test
    void testTryAppendWithWriteLimit() throws Exception {
        int bucketId = 0;
        int writeLimit = 100;
        CompactedLogWriteBatch logProducerBatch =
                createLogWriteBatch(
                        new TableBucket(DATA1_TABLE_ID, bucketId),
                        writeLimit,
                        MemorySegment.allocateHeapMemory(writeLimit));

        int maxRecordsPerBatch =
                (writeLimit - recordBatchHeaderSize(CURRENT_LOG_MAGIC_VALUE))
                        / estimatedSizeInBytes;
        for (int i = 0; i < maxRecordsPerBatch; i++) {
            boolean appendResult =
                    logProducerBatch.tryAppend(createWriteRecord(), newWriteCallback());
            assertThat(appendResult).isTrue();
        }

        // batch full.
        boolean appendResult = logProducerBatch.tryAppend(createWriteRecord(), newWriteCallback());
        assertThat(appendResult).isFalse();
    }

    @Test
    void testToBytes() throws Exception {
        int bucketId = 0;
        CompactedLogWriteBatch logProducerBatch =
                createLogWriteBatch(new TableBucket(DATA1_TABLE_ID, bucketId));
        boolean appendResult = logProducerBatch.tryAppend(createWriteRecord(), newWriteCallback());
        assertThat(appendResult).isTrue();

        logProducerBatch.close();
        BytesView bytesView = logProducerBatch.build();
        MemoryLogRecords logRecords = MemoryLogRecords.pointToBytesView(bytesView);
        Iterator<LogRecordBatch> iterator = logRecords.batches().iterator();
        assertDefaultLogRecordBatchEquals(iterator.next());
        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    void testCompleteTwice() throws Exception {
        int bucketId = 0;
        CompactedLogWriteBatch logWriteBatch =
                createLogWriteBatch(new TableBucket(DATA1_TABLE_ID, bucketId));
        boolean appendResult = logWriteBatch.tryAppend(createWriteRecord(), newWriteCallback());
        assertThat(appendResult).isTrue();

        assertThat(logWriteBatch.complete()).isTrue();
        assertThatThrownBy(logWriteBatch::complete)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "A SUCCEEDED batch must not attempt another state change to SUCCEEDED");
    }

    @Test
    void testFailedTwice() throws Exception {
        int bucketId = 0;
        CompactedLogWriteBatch logWriteBatch =
                createLogWriteBatch(new TableBucket(DATA1_TABLE_ID, bucketId));
        boolean appendResult = logWriteBatch.tryAppend(createWriteRecord(), newWriteCallback());
        assertThat(appendResult).isTrue();

        assertThat(logWriteBatch.completeExceptionally(new IllegalStateException("test failed.")))
                .isTrue();
        // FAILED --> FAILED transitions are ignored.
        assertThat(logWriteBatch.completeExceptionally(new IllegalStateException("test failed.")))
                .isFalse();
    }

    @Test
    void testClose() throws Exception {
        int bucketId = 0;
        CompactedLogWriteBatch logProducerBatch =
                createLogWriteBatch(new TableBucket(DATA1_TABLE_ID, bucketId));
        boolean appendResult = logProducerBatch.tryAppend(createWriteRecord(), newWriteCallback());
        assertThat(appendResult).isTrue();

        logProducerBatch.close();
        assertThat(logProducerBatch.isClosed()).isTrue();

        appendResult = logProducerBatch.tryAppend(createWriteRecord(), newWriteCallback());
        assertThat(appendResult).isFalse();
    }

    @Test
    void testTryAppendRejectsIncompatibleBatchIdentity() throws Exception {
        int bucketId = 0;
        CompactedLogWriteBatch logProducerBatch =
                createLogWriteBatch(new TableBucket(DATA1_TABLE_ID, bucketId));

        assertThat(logProducerBatch.tryAppend(createWriteRecord(), newWriteCallback())).isTrue();
        assertThat(
                        logProducerBatch.tryAppend(
                                createWriteRecord(
                                        withTableIdAndSchemaId(
                                                DATA1_TABLE_ID,
                                                DATA1_TABLE_INFO.getSchemaId() + 1)),
                                newWriteCallback()))
                .isFalse();
        assertThat(
                        logProducerBatch.tryAppend(
                                createWriteRecord(
                                        withTableIdAndSchemaId(
                                                DATA1_TABLE_ID + 1,
                                                DATA1_TABLE_INFO.getSchemaId())),
                                newWriteCallback()))
                .isFalse();
        assertThat(
                        logProducerBatch.tryAppend(
                                WriteRecord.forIndexedAppend(
                                        DATA1_TABLE_INFO,
                                        DATA1_PHYSICAL_TABLE_PATH,
                                        indexedRow(DATA1_ROW_TYPE, new Object[] {1, "a"}),
                                        null),
                                newWriteCallback()))
                .isFalse();
        assertThat(logProducerBatch.getRecordCount()).isEqualTo(1);
    }

    @Test
    void testBatchAborted() throws Exception {
        int bucketId = 0;
        int writeLimit = 10240;
        CompactedLogWriteBatch logProducerBatch =
                createLogWriteBatch(
                        new TableBucket(DATA1_TABLE_ID, bucketId),
                        writeLimit,
                        MemorySegment.allocateHeapMemory(writeLimit));

        int recordCount = 5;
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < recordCount; i++) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            logProducerBatch.tryAppend(
                    createWriteRecord(),
                    (bucket, offset, exception) -> {
                        if (exception != null) {
                            future.completeExceptionally(exception);
                        } else {
                            future.complete(null);
                        }
                    });
            futures.add(future);
        }

        logProducerBatch.abortRecordAppends();
        logProducerBatch.abort(new RuntimeException("close with record batch abort"));

        // first try to append.
        assertThatThrownBy(
                        () -> logProducerBatch.tryAppend(createWriteRecord(), newWriteCallback()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "Tried to append a record, but MemoryLogRecordsCompactedBuilder has already been aborted");

        // try to build.
        assertThatThrownBy(logProducerBatch::build)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Attempting to build an aborted record batch");

        // verify record append future is completed with exception.
        for (CompletableFuture<Void> future : futures) {
            assertThatThrownBy(future::join)
                    .rootCause()
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("close with record batch abort");
        }
    }

    private WriteRecord createWriteRecord() {
        return createWriteRecord(DATA1_TABLE_INFO);
    }

    private WriteRecord createWriteRecord(TableInfo tableInfo) {
        return WriteRecord.forCompactedAppend(tableInfo, DATA1_PHYSICAL_TABLE_PATH, row, null);
    }

    private CompactedLogWriteBatch createLogWriteBatch(TableBucket tb) throws Exception {
        return createLogWriteBatch(tb, Integer.MAX_VALUE, MemorySegment.allocateHeapMemory(1000));
    }

    private CompactedLogWriteBatch createLogWriteBatch(
            TableBucket tb, int writeLimit, MemorySegment memorySegment) {
        return new CompactedLogWriteBatch(
                tb.getTableId(),
                tb.getBucket(),
                DATA1_PHYSICAL_TABLE_PATH,
                DATA1_TABLE_INFO.getSchemaId(),
                writeLimit,
                new PreAllocatedPagedOutputView(Collections.singletonList(memorySegment)),
                false,
                System.currentTimeMillis());
    }

    private TableInfo withTableIdAndSchemaId(long tableId, int schemaId) {
        return new TableInfo(
                DATA1_TABLE_INFO.getTablePath(),
                tableId,
                schemaId,
                DATA1_TABLE_INFO.getSchema(),
                DATA1_TABLE_INFO.getBucketKeys(),
                DATA1_TABLE_INFO.getPartitionKeys(),
                DATA1_TABLE_INFO.getNumBuckets(),
                DATA1_TABLE_INFO.getProperties(),
                DATA1_TABLE_INFO.getCustomProperties(),
                DATA1_TABLE_INFO.getRemoteDataDir(),
                DATA1_TABLE_INFO.getComment().orElse(null),
                DATA1_TABLE_INFO.getCreatedTime(),
                DATA1_TABLE_INFO.getModifiedTime());
    }

    private void assertDefaultLogRecordBatchEquals(LogRecordBatch recordBatch) {
        assertThat(recordBatch.getRecordCount()).isEqualTo(1);
        assertThat(recordBatch.baseLogOffset()).isEqualTo(0L);
        assertThat(recordBatch.schemaId()).isEqualTo((short) DATA1_TABLE_INFO.getSchemaId());
        try (LogRecordReadContext readContext =
                        LogRecordReadContext.createCompactedRowReadContext(
                                DATA1_ROW_TYPE,
                                DATA1_TABLE_INFO.getSchemaId(),
                                TEST_SCHEMA_GETTER);
                CloseableIterator<LogRecord> iterator = recordBatch.records(readContext)) {
            assertThat(iterator.hasNext()).isTrue();
            LogRecord record = iterator.next();
            assertThat(record.getChangeType()).isEqualTo(ChangeType.APPEND_ONLY);
            assertThat(record.getRow()).isEqualTo(row);
            assertThat(iterator.hasNext()).isFalse();
        }
    }

    private WriteCallback newWriteCallback() {
        return (bucket, offset, exception) -> {
            if (exception != null) {
                throw new RuntimeException(exception);
            }
        };
    }
}
