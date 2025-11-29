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
import org.apache.fluss.memory.TestingMemorySegmentPool;
import org.apache.fluss.memory.UnmanagedPagedOutputView;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.arrow.ArrowWriter;
import org.apache.fluss.row.arrow.ArrowWriterPool;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.utils.CloseableIterator;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.LogRecordReadContext.createArrowReadContext;
import static org.apache.fluss.record.TestData.DATA1_PHYSICAL_TABLE_PATH;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link ArrowLogWriteBatch}. */
public class ArrowLogWriteBatchTest {

    private BufferAllocator allocator;
    private ArrowWriterPool writerProvider;

    @BeforeEach
    void setup() {
        allocator = new RootAllocator(Long.MAX_VALUE);
        writerProvider = new ArrowWriterPool(allocator);
    }

    @AfterEach
    void teardown() {
        allocator.close();
        writerProvider.close();
    }

    @Test
    void testAppend() throws Exception {
        int bucketId = 0;
        int maxSizeInBytes = 1024;
        ArrowLogWriteBatch arrowLogWriteBatch =
                createArrowLogWriteBatch(new TableBucket(DATA1_TABLE_ID, bucketId), maxSizeInBytes);
        int count = 0;
        while (arrowLogWriteBatch.tryAppend(
                createWriteRecord(row(count, "a" + count)), newWriteCallback())) {
            count++;
        }

        // batch full.
        boolean appendResult =
                arrowLogWriteBatch.tryAppend(createWriteRecord(row(1, "a")), newWriteCallback());
        assertThat(appendResult).isFalse();

        // close this batch.
        arrowLogWriteBatch.close();
        BytesView bytesView = arrowLogWriteBatch.build();
        MemoryLogRecords records = MemoryLogRecords.pointToBytesView(bytesView);
        LogRecordBatch batch = records.batches().iterator().next();
        assertThat(batch.getRecordCount()).isEqualTo(count);
        try (LogRecordReadContext readContext =
                        createArrowReadContext(DATA1_ROW_TYPE, DATA1_TABLE_INFO.getSchemaId());
                CloseableIterator<LogRecord> recordsIter = batch.records(readContext)) {
            int readCount = 0;
            while (recordsIter.hasNext()) {
                LogRecord record = recordsIter.next();
                assertThat(record.getRow().getInt(0)).isEqualTo(readCount);
                assertThat(record.getRow().getString(1).toString()).isEqualTo("a" + readCount);
                readCount++;
            }
            assertThat(readCount).isEqualTo(count);
        }
    }

    @Test
    void testAppendWithPreAllocatedMemorySegments() throws Exception {
        int bucketId = 0;
        int maxSizeInBytes = 1024;
        int pageSize = 128;
        TestingMemorySegmentPool memoryPool = new TestingMemorySegmentPool(pageSize);
        List<MemorySegment> memorySegmentList = new ArrayList<>();
        for (int i = 0; i < maxSizeInBytes / pageSize; i++) {
            memorySegmentList.add(memoryPool.nextSegment());
        }

        TableBucket tb = new TableBucket(DATA1_TABLE_ID, bucketId);
        ArrowLogWriteBatch arrowLogWriteBatch =
                new ArrowLogWriteBatch(
                        tb.getBucket(),
                        DATA1_PHYSICAL_TABLE_PATH,
                        DATA1_TABLE_INFO.getSchemaId(),
                        writerProvider.getOrCreateWriter(
                                tb.getTableId(),
                                DATA1_TABLE_INFO.getSchemaId(),
                                maxSizeInBytes,
                                DATA1_ROW_TYPE,
                                DEFAULT_COMPRESSION),
                        new PreAllocatedPagedOutputView(memorySegmentList),
                        System.currentTimeMillis());
        assertThat(arrowLogWriteBatch.pooledMemorySegments()).isEqualTo(memorySegmentList);

        int count = 0;
        while (arrowLogWriteBatch.tryAppend(
                createWriteRecord(row(count, "a" + count)), newWriteCallback())) {
            count++;
        }

        // batch full.
        boolean appendResult =
                arrowLogWriteBatch.tryAppend(createWriteRecord(row(1, "a")), newWriteCallback());
        assertThat(appendResult).isFalse();

        // close this batch.
        arrowLogWriteBatch.close();
        BytesView bytesView = arrowLogWriteBatch.build();
        MemoryLogRecords records = MemoryLogRecords.pointToBytesView(bytesView);
        LogRecordBatch batch = records.batches().iterator().next();
        assertThat(batch.getRecordCount()).isEqualTo(count);
        try (LogRecordReadContext readContext =
                        createArrowReadContext(DATA1_ROW_TYPE, DATA1_TABLE_INFO.getSchemaId());
                CloseableIterator<LogRecord> recordsIter = batch.records(readContext)) {
            int readCount = 0;
            while (recordsIter.hasNext()) {
                LogRecord record = recordsIter.next();
                assertThat(record.getRow().getInt(0)).isEqualTo(readCount);
                assertThat(record.getRow().getString(1).toString()).isEqualTo("a" + readCount);
                readCount++;
            }
            assertThat(readCount).isEqualTo(count);
        }
    }

    @Test
    void testArrowCompressionRatioEstimated() throws Exception {
        int bucketId = 0;
        int maxSizeInBytes = 1024 * 10;
        int pageSize = 512;
        TestingMemorySegmentPool memoryPool = new TestingMemorySegmentPool(pageSize);
        List<MemorySegment> memorySegmentList = new ArrayList<>();
        for (int i = 0; i < maxSizeInBytes / pageSize; i++) {
            memorySegmentList.add(memoryPool.nextSegment());
        }

        TableBucket tb = new TableBucket(DATA1_TABLE_ID, bucketId);

        // The compression rate increases slowly, with an increment of only 0.005
        // (COMPRESSION_RATIO_IMPROVING_STEP#COMPRESSION_RATIO_IMPROVING_STEP) each time. Therefore,
        // the loop runs 100 times, and theoretically, the final number of input records will be
        // much greater than at the beginning.
        float previousRatio = -1.0f;
        float currentRatio = 1.0f;
        int lastBytesInSize = 0;
        // exit the loop until compression ratio is converged
        while (previousRatio != currentRatio) {
            ArrowWriter arrowWriter =
                    writerProvider.getOrCreateWriter(
                            tb.getTableId(),
                            DATA1_TABLE_INFO.getSchemaId(),
                            maxSizeInBytes,
                            DATA1_ROW_TYPE,
                            DEFAULT_COMPRESSION);

            ArrowLogWriteBatch arrowLogWriteBatch =
                    new ArrowLogWriteBatch(
                            tb.getBucket(),
                            DATA1_PHYSICAL_TABLE_PATH,
                            DATA1_TABLE_INFO.getSchemaId(),
                            arrowWriter,
                            new PreAllocatedPagedOutputView(memorySegmentList),
                            System.currentTimeMillis());

            int recordCount = 0;
            while (arrowLogWriteBatch.tryAppend(
                    createWriteRecord(row(recordCount, RandomStringUtils.random(100))),
                    newWriteCallback())) {
                recordCount++;
            }

            // batch full.
            boolean appendResult =
                    arrowLogWriteBatch.tryAppend(
                            createWriteRecord(row(1, "a")), newWriteCallback());
            assertThat(appendResult).isFalse();

            // close this batch and recycle the writer.
            arrowLogWriteBatch.close();
            BytesView built = arrowLogWriteBatch.build();
            lastBytesInSize = built.getBytesLength();

            previousRatio = currentRatio;
            currentRatio = arrowWriter.getCompressionRatioEstimator().estimation();
        }

        // when the compression ratio is converged, the memory buffer should be fully used.
        assertThat(lastBytesInSize)
                .isGreaterThan((int) (maxSizeInBytes * ArrowWriter.BUFFER_USAGE_RATIO))
                .isLessThan(maxSizeInBytes);
        assertThat(currentRatio).isLessThan(1.0f);
    }

    @Test
    void testBatchAborted() throws Exception {
        int bucketId = 0;
        int maxSizeInBytes = 10240;
        ArrowLogWriteBatch arrowLogWriteBatch =
                createArrowLogWriteBatch(new TableBucket(DATA1_TABLE_ID, bucketId), maxSizeInBytes);
        int recordCount = 5;
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < recordCount; i++) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            arrowLogWriteBatch.tryAppend(
                    createWriteRecord(row(i, "a" + i)),
                    exception -> {
                        if (exception != null) {
                            future.completeExceptionally(exception);
                        } else {
                            future.complete(null);
                        }
                    });
            futures.add(future);
        }

        assertThat(writerProvider.freeWriters()).isEmpty();
        arrowLogWriteBatch.abortRecordAppends();
        arrowLogWriteBatch.abort(new RuntimeException("close with record batch abort"));

        // first try to append.
        assertThatThrownBy(
                        () ->
                                arrowLogWriteBatch.tryAppend(
                                        createWriteRecord(row(1, "a")), newWriteCallback()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "Tried to append a record, but MemoryLogRecordsArrowBuilder has already been aborted");

        // try to build.
        assertThatThrownBy(arrowLogWriteBatch::build)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Attempting to build an aborted record batch");

        // verify arrow writer have recycled.
        assertThat(writerProvider.freeWriters()).hasSize(1);
        assertThat(writerProvider.freeWriters().get("150001-1-ZSTD-3")).isNotNull();

        // verify record append future is completed with exception.
        for (CompletableFuture<Void> future : futures) {
            assertThatThrownBy(future::join)
                    .rootCause()
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("close with record batch abort");
        }
    }

    private WriteRecord createWriteRecord(GenericRow row) {
        return WriteRecord.forArrowAppend(DATA1_TABLE_INFO, DATA1_PHYSICAL_TABLE_PATH, row, null);
    }

    private ArrowLogWriteBatch createArrowLogWriteBatch(TableBucket tb, int maxSizeInBytes) {
        return new ArrowLogWriteBatch(
                tb.getBucket(),
                DATA1_PHYSICAL_TABLE_PATH,
                DATA1_TABLE_INFO.getSchemaId(),
                writerProvider.getOrCreateWriter(
                        tb.getTableId(),
                        DATA1_TABLE_INFO.getSchemaId(),
                        maxSizeInBytes,
                        DATA1_ROW_TYPE,
                        DEFAULT_COMPRESSION),
                new UnmanagedPagedOutputView(128),
                System.currentTimeMillis());
    }

    private WriteCallback newWriteCallback() {
        return exception -> {
            if (exception != null) {
                throw new RuntimeException(exception);
            }
        };
    }
}
