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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.memory.LazyMemorySegmentPool;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.memory.MemorySegmentPool;
import org.apache.fluss.memory.PreAllocatedPagedOutputView;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.DefaultKvRecord;
import org.apache.fluss.record.DefaultKvRecordBatch;
import org.apache.fluss.record.KvRecord;
import org.apache.fluss.record.KvRecordReadContext;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.types.DataType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.apache.fluss.utils.BytesUtils.toArray;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link KvWriteBatch}. */
class KvWriteBatchTest {
    private BinaryRow row;
    private byte[] key;
    private int estimatedSizeInBytes;
    private MemorySegmentPool memoryPool;

    @BeforeEach
    void setup() {
        row = compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a"});
        int[] pkIndex = DATA1_SCHEMA_PK.getPrimaryKeyIndexes();
        key = new CompactedKeyEncoder(DATA1_ROW_TYPE, pkIndex).encodeKey(row);
        estimatedSizeInBytes = DefaultKvRecord.sizeOf(key, row);
        Configuration config = new Configuration();
        config.setString(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE.key(), "5kb");
        config.setString(ConfigOptions.CLIENT_WRITER_BUFFER_PAGE_SIZE.key(), "256b");
        config.setString(ConfigOptions.CLIENT_WRITER_BATCH_SIZE.key(), "1kb");
        memoryPool = LazyMemorySegmentPool.createWriterBufferPool(config);
    }

    @Test
    void testTryAppendWithWriteLimit() throws Exception {
        int writeLimit = 100;
        KvWriteBatch kvProducerBatch =
                createKvWriteBatch(
                        new TableBucket(DATA1_TABLE_ID_PK, 0),
                        writeLimit,
                        MemorySegment.allocateHeapMemory(writeLimit));

        for (int i = 0;
                i
                        < (writeLimit - DefaultKvRecordBatch.RECORD_BATCH_HEADER_SIZE)
                                / estimatedSizeInBytes;
                i++) {
            boolean appendResult =
                    kvProducerBatch.tryAppend(createWriteRecord(), newWriteCallback());

            assertThat(appendResult).isTrue();
        }

        // batch full.
        boolean appendResult = kvProducerBatch.tryAppend(createWriteRecord(), newWriteCallback());
        assertThat(appendResult).isFalse();
    }

    @Test
    void testToBytes() throws Exception {
        KvWriteBatch kvProducerBatch = createKvWriteBatch(new TableBucket(DATA1_TABLE_ID_PK, 0));

        boolean appendResult = kvProducerBatch.tryAppend(createWriteRecord(), newWriteCallback());
        assertThat(appendResult).isTrue();
        DefaultKvRecordBatch kvRecords =
                DefaultKvRecordBatch.pointToBytesView(kvProducerBatch.build());
        assertDefaultKvRecordBatchEquals(kvRecords);
    }

    @Test
    void testCompleteTwice() throws Exception {
        KvWriteBatch kvWriteBatch = createKvWriteBatch(new TableBucket(DATA1_TABLE_ID_PK, 0));

        boolean appendResult = kvWriteBatch.tryAppend(createWriteRecord(), newWriteCallback());
        assertThat(appendResult).isTrue();

        assertThat(kvWriteBatch.complete()).isTrue();
        assertThatThrownBy(kvWriteBatch::complete)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "A SUCCEEDED batch must not attempt another state change to SUCCEEDED");
    }

    @Test
    void testFailedTwice() throws Exception {
        KvWriteBatch kvWriteBatch = createKvWriteBatch(new TableBucket(DATA1_TABLE_ID_PK, 0));

        boolean appendResult = kvWriteBatch.tryAppend(createWriteRecord(), newWriteCallback());
        assertThat(appendResult).isTrue();

        assertThat(kvWriteBatch.completeExceptionally(new IllegalStateException("test failed.")))
                .isTrue();
        // FAILED --> FAILED transitions are ignored.
        assertThat(kvWriteBatch.completeExceptionally(new IllegalStateException("test failed.")))
                .isFalse();
    }

    @Test
    void testClose() throws Exception {
        KvWriteBatch kvProducerBatch = createKvWriteBatch(new TableBucket(DATA1_TABLE_ID_PK, 0));
        boolean appendResult = kvProducerBatch.tryAppend(createWriteRecord(), newWriteCallback());
        assertThat(appendResult).isTrue();

        kvProducerBatch.close();
        assertThat(kvProducerBatch.isClosed()).isTrue();

        appendResult = kvProducerBatch.tryAppend(createWriteRecord(), newWriteCallback());
        assertThat(appendResult).isFalse();
    }

    @Test
    void testBatchAborted() throws Exception {
        int writeLimit = 10240;
        KvWriteBatch kvProducerBatch =
                createKvWriteBatch(
                        new TableBucket(DATA1_TABLE_ID_PK, 0),
                        writeLimit,
                        MemorySegment.allocateHeapMemory(writeLimit));

        int recordCount = 5;
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < recordCount; i++) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            kvProducerBatch.tryAppend(
                    createWriteRecord(),
                    exception -> {
                        if (exception != null) {
                            future.completeExceptionally(exception);
                        } else {
                            future.complete(null);
                        }
                    });
            futures.add(future);
        }

        kvProducerBatch.abortRecordAppends();
        kvProducerBatch.abort(new RuntimeException("close with record batch abort"));

        // first try to append.
        assertThatThrownBy(() -> kvProducerBatch.tryAppend(createWriteRecord(), newWriteCallback()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "Tried to append a record, but KvRecordBatchBuilder has already been aborted");

        // try to build.
        assertThatThrownBy(kvProducerBatch::build)
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

    protected WriteRecord createWriteRecord() {
        return WriteRecord.forUpsert(
                DATA1_TABLE_INFO_PK,
                PhysicalTablePath.of(DATA1_TABLE_PATH_PK),
                row,
                key,
                key,
                null);
    }

    private KvWriteBatch createKvWriteBatch(TableBucket tb) throws Exception {
        return createKvWriteBatch(tb, Integer.MAX_VALUE, memoryPool.nextSegment());
    }

    private KvWriteBatch createKvWriteBatch(
            TableBucket tb, int writeLimit, MemorySegment memorySegment) throws Exception {
        PreAllocatedPagedOutputView outputView =
                new PreAllocatedPagedOutputView(Collections.singletonList(memorySegment));
        return new KvWriteBatch(
                tb.getBucket(),
                PhysicalTablePath.of(DATA1_TABLE_PATH_PK),
                DATA1_TABLE_INFO_PK.getSchemaId(),
                KvFormat.COMPACTED,
                writeLimit,
                outputView,
                null,
                System.currentTimeMillis());
    }

    private WriteCallback newWriteCallback() {
        return exception -> {
            if (exception != null) {
                throw new RuntimeException(exception);
            }
        };
    }

    private void assertDefaultKvRecordBatchEquals(DefaultKvRecordBatch recordBatch) {
        assertThat(recordBatch.getRecordCount()).isEqualTo(1);

        DataType[] dataTypes = DATA1_ROW_TYPE.getChildren().toArray(new DataType[0]);
        Iterator<KvRecord> iterator =
                recordBatch
                        .records(
                                KvRecordReadContext.createReadContext(
                                        KvFormat.COMPACTED,
                                        new TestingSchemaGetter(1, DATA1_SCHEMA)))
                        .iterator();
        assertThat(iterator.hasNext()).isTrue();
        KvRecord kvRecord = iterator.next();
        assertThat(toArray(kvRecord.getKey())).isEqualTo(key);
        assertThat(kvRecord.getRow()).isEqualTo(row);
    }
}
