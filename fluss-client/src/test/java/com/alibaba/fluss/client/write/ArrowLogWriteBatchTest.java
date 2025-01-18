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

package com.alibaba.fluss.client.write;

import com.alibaba.fluss.compression.ArrowCompressionInfo;
import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.memory.PreAllocatedPagedOutputView;
import com.alibaba.fluss.memory.TestingMemorySegmentPool;
import com.alibaba.fluss.memory.UnmanagedPagedOutputView;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.record.LogRecordBatch;
import com.alibaba.fluss.record.LogRecordReadContext;
import com.alibaba.fluss.record.MemoryLogRecords;
import com.alibaba.fluss.record.bytesview.BytesView;
import com.alibaba.fluss.row.arrow.ArrowWriterPool;
import com.alibaba.fluss.row.indexed.IndexedRow;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import com.alibaba.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.fluss.record.LogRecordReadContext.createArrowReadContext;
import static com.alibaba.fluss.record.TestData.DATA1_PHYSICAL_TABLE_PATH;
import static com.alibaba.fluss.record.TestData.DATA1_ROW_TYPE;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_ID;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_INFO;
import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

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
                createWriteRecord(row(DATA1_ROW_TYPE, new Object[] {count, "a" + count})),
                newWriteCallback())) {
            count++;
        }

        // batch full.
        boolean appendResult =
                arrowLogWriteBatch.tryAppend(
                        createWriteRecord(row(DATA1_ROW_TYPE, new Object[] {1, "a"})),
                        newWriteCallback());
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
                        tb,
                        DATA1_PHYSICAL_TABLE_PATH,
                        DATA1_TABLE_INFO.getSchemaId(),
                        writerProvider.getOrCreateWriter(
                                tb.getTableId(),
                                DATA1_TABLE_INFO.getSchemaId(),
                                maxSizeInBytes,
                                DATA1_ROW_TYPE,
                                ArrowCompressionInfo.NO_COMPRESSION),
                        new PreAllocatedPagedOutputView(memorySegmentList));
        assertThat(arrowLogWriteBatch.pooledMemorySegments()).isEqualTo(memorySegmentList);

        int count = 0;
        while (arrowLogWriteBatch.tryAppend(
                createWriteRecord(row(DATA1_ROW_TYPE, new Object[] {count, "a" + count})),
                newWriteCallback())) {
            count++;
        }

        // batch full.
        boolean appendResult =
                arrowLogWriteBatch.tryAppend(
                        createWriteRecord(row(DATA1_ROW_TYPE, new Object[] {1, "a"})),
                        newWriteCallback());
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

    private WriteRecord createWriteRecord(IndexedRow row) {
        return new WriteRecord(DATA1_PHYSICAL_TABLE_PATH, WriteKind.APPEND, row, null);
    }

    private ArrowLogWriteBatch createArrowLogWriteBatch(TableBucket tb, int maxSizeInBytes) {
        return new ArrowLogWriteBatch(
                tb,
                DATA1_PHYSICAL_TABLE_PATH,
                DATA1_TABLE_INFO.getSchemaId(),
                writerProvider.getOrCreateWriter(
                        tb.getTableId(),
                        DATA1_TABLE_INFO.getSchemaId(),
                        maxSizeInBytes,
                        DATA1_ROW_TYPE,
                        ArrowCompressionInfo.NO_COMPRESSION),
                new UnmanagedPagedOutputView(128));
    }

    private WriteCallback newWriteCallback() {
        return exception -> {
            if (exception != null) {
                throw new RuntimeException(exception);
            }
        };
    }
}
