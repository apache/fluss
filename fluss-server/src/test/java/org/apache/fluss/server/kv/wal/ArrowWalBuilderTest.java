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

package org.apache.fluss.server.kv.wal;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.memory.LazyMemorySegmentPool;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.memory.MemorySegmentPool;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.row.arrow.ArrowWriterPool;
import org.apache.fluss.row.arrow.ArrowWriterProvider;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID_PK;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.testutils.DataTestUtils.assertLogRecordsEqualsWithRowKind;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ArrowWalBuilder}. */
class ArrowWalBuilderTest {
    private BufferAllocator allocator;
    private ArrowWriterProvider arrowWriterProvider;
    private Configuration conf;

    @BeforeEach
    void setUp() {
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        this.arrowWriterProvider = new ArrowWriterPool(allocator);
        this.conf = new Configuration();
    }

    @AfterEach
    void tearDown() throws Exception {
        arrowWriterProvider.close();
        allocator.close();
    }

    @Test
    void testArrowWalBuilderCrossSeveralMemoryPages() throws Exception {
        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE, MemorySize.parse("2kb"));
        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_PAGE_SIZE, MemorySize.parse("128b"));
        conf.set(ConfigOptions.CLIENT_WRITER_BATCH_SIZE, MemorySize.parse("128b"));

        int bucketId = 0;
        TableBucket tb = new TableBucket(DATA1_TABLE_ID_PK, bucketId);
        LazyMemorySegmentPool memorySegmentPool =
                LazyMemorySegmentPool.createWriterBufferPool(conf);
        WalBuilder walBuilder = createWalBuilder(tb, 1024, memorySegmentPool);

        List<Tuple2<ChangeType, Object[]>> expectedResult = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            ChangeType changeType = ChangeType.INSERT;
            Object[] objects = {i, "v" + i};
            walBuilder.append(changeType, row(objects));
            expectedResult.add(Tuple2.of(changeType, objects));
        }

        // consume log records before walBuilder deallocate memory. it's safe.
        MemoryLogRecords logRecords = walBuilder.build();
        long totalPages = memorySegmentPool.totalSize() / memorySegmentPool.pageSize();
        assertThat(logRecords.batches().iterator().next().isValid()).isTrue();
        // allocate multiple pages
        assertThat(totalPages - memorySegmentPool.freePages()).isGreaterThan(1);
        assertLogRecordsEqualsWithRowKind(DATA1_ROW_TYPE, logRecords, expectedResult);

        // consume log records after walBuilder deallocate memory. Even the content in memory
        // segment pool is changed, the log records is still valid. Because the memory of logRecords
        // is deeply copied.
        walBuilder.deallocate();
        assertThat(memorySegmentPool.freePages()).isEqualTo(totalPages);
        assertThat(logRecords.batches().iterator().next().isValid()).isTrue();
        // change some bytes of memory segment pool.
        for (MemorySegment memorySegment : memorySegmentPool.getAllCachePages()) {
            memorySegment.put(50, (byte) 4);
        }
        assertThat(logRecords.batches().iterator().next().isValid()).isTrue();
        assertLogRecordsEqualsWithRowKind(DATA1_ROW_TYPE, logRecords, expectedResult);
    }

    @Test
    void testArrowWalBuilderWithinOneMemoryPage() throws Exception {
        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE, MemorySize.parse("4kb"));
        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_PAGE_SIZE, MemorySize.parse("1kb"));
        conf.set(ConfigOptions.CLIENT_WRITER_BATCH_SIZE, MemorySize.parse("1kb"));

        int bucketId = 0;
        TableBucket tb = new TableBucket(DATA1_TABLE_ID_PK, bucketId);
        LazyMemorySegmentPool memorySegmentPool =
                LazyMemorySegmentPool.createWriterBufferPool(conf);
        WalBuilder walBuilder = createWalBuilder(tb, 1024, memorySegmentPool);

        List<Tuple2<ChangeType, Object[]>> expectedResult = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            ChangeType changeType = ChangeType.INSERT;
            Object[] objects = {i, "v" + i};
            walBuilder.append(changeType, row(objects));
            expectedResult.add(Tuple2.of(changeType, objects));
        }

        // consume log records before walBuilder deallocate memory. it's safe.
        MemoryLogRecords logRecords = walBuilder.build();
        long totalPages = memorySegmentPool.totalSize() / memorySegmentPool.pageSize();
        assertThat(logRecords.batches().iterator().next().isValid()).isTrue();
        // allocate one page
        assertThat(totalPages - memorySegmentPool.freePages()).isEqualTo(1);
        assertLogRecordsEqualsWithRowKind(DATA1_ROW_TYPE, logRecords, expectedResult);

        // consume log records after walBuilder deallocate memory. While the content in memory
        // segment pool is changed, the log records will be invalid. Because the memory of
        // logRecords use the same byteBuffer with the segment in memory segment pool.
        walBuilder.deallocate();
        assertThat(memorySegmentPool.freePages()).isEqualTo(totalPages);
        assertThat(logRecords.batches().iterator().next().isValid()).isTrue();
        // change some bytes of memory segment pool.
        for (MemorySegment memorySegment : memorySegmentPool.getAllCachePages()) {
            memorySegment.put(50, (byte) 4);
        }
        assertThat(logRecords.batches().iterator().next().isValid()).isFalse();
    }

    private WalBuilder createWalBuilder(
            TableBucket tb, int maxSizeInBytes, MemorySegmentPool memorySegmentPool)
            throws IOException {
        return new ArrowWalBuilder(
                DEFAULT_SCHEMA_ID,
                arrowWriterProvider.getOrCreateWriter(
                        tb.getTableId(),
                        DEFAULT_SCHEMA_ID,
                        maxSizeInBytes,
                        DATA1_ROW_TYPE,
                        DEFAULT_COMPRESSION),
                memorySegmentPool);
    }
}
