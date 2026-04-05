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
import org.apache.fluss.record.MutationType;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.types.DataType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.client.write.WriteBatch.AppendResult;
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
        // Use a write limit that fits exactly N records with no leftover space
        int headerSize = DefaultKvRecordBatch.RECORD_BATCH_HEADER_SIZE;
        int numRecords = 5;
        int writeLimit = headerSize + numRecords * estimatedSizeInBytes;
        KvWriteBatch kvProducerBatch =
                createKvWriteBatch(
                        new TableBucket(DATA1_TABLE_ID_PK, 0),
                        writeLimit,
                        MemorySegment.allocateHeapMemory(writeLimit));

        for (int i = 0; i < numRecords; i++) {
            AppendResult appendResult =
                    kvProducerBatch.tryAppend(createWriteRecord(), newWriteCallback());

            assertThat(appendResult).isEqualTo(AppendResult.APPENDED);
        }

        // batch full.
        AppendResult appendResult =
                kvProducerBatch.tryAppend(createWriteRecord(), newWriteCallback());
        assertThat(appendResult).isEqualTo(AppendResult.BATCH_FULL);
    }

    @Test
    void testToBytes() throws Exception {
        KvWriteBatch kvProducerBatch = createKvWriteBatch(new TableBucket(DATA1_TABLE_ID_PK, 0));

        assertThat(kvProducerBatch.tryAppend(createWriteRecord(), newWriteCallback()))
                .isEqualTo(AppendResult.APPENDED);
        DefaultKvRecordBatch kvRecords =
                DefaultKvRecordBatch.pointToBytesView(kvProducerBatch.build());
        assertDefaultKvRecordBatchEquals(kvRecords);
    }

    @Test
    void testCompleteTwice() throws Exception {
        KvWriteBatch kvWriteBatch = createKvWriteBatch(new TableBucket(DATA1_TABLE_ID_PK, 0));

        assertThat(kvWriteBatch.tryAppend(createWriteRecord(), newWriteCallback()))
                .isEqualTo(AppendResult.APPENDED);

        assertThat(kvWriteBatch.complete()).isTrue();
        assertThatThrownBy(kvWriteBatch::complete)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "A SUCCEEDED batch must not attempt another state change to SUCCEEDED");
    }

    @Test
    void testFailedTwice() throws Exception {
        KvWriteBatch kvWriteBatch = createKvWriteBatch(new TableBucket(DATA1_TABLE_ID_PK, 0));

        assertThat(kvWriteBatch.tryAppend(createWriteRecord(), newWriteCallback()))
                .isEqualTo(AppendResult.APPENDED);

        assertThat(kvWriteBatch.completeExceptionally(new IllegalStateException("test failed.")))
                .isTrue();
        // FAILED --> FAILED transitions are ignored.
        assertThat(kvWriteBatch.completeExceptionally(new IllegalStateException("test failed.")))
                .isFalse();
    }

    @Test
    void testClose() throws Exception {
        KvWriteBatch kvProducerBatch = createKvWriteBatch(new TableBucket(DATA1_TABLE_ID_PK, 0));
        assertThat(kvProducerBatch.tryAppend(createWriteRecord(), newWriteCallback()))
                .isEqualTo(AppendResult.APPENDED);

        kvProducerBatch.close();
        assertThat(kvProducerBatch.isClosed()).isTrue();

        assertThat(kvProducerBatch.tryAppend(createWriteRecord(), newWriteCallback()))
                .isEqualTo(AppendResult.BATCH_FULL);
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
                    (bucket, offset, exception) -> {
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
                WriteFormat.COMPACTED_KV,
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
                MergeMode.DEFAULT,
                false,
                System.currentTimeMillis());
    }

    private WriteCallback newWriteCallback() {
        return (bucket, offset, exception) -> {
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

    // ==================== MergeMode Tests ====================

    @Test
    void testMergeModeConsistencyValidation() throws Exception {
        // Create batch with DEFAULT mode
        KvWriteBatch defaultBatch =
                createKvWriteBatchWithMergeMode(
                        new TableBucket(DATA1_TABLE_ID_PK, 0), MergeMode.DEFAULT);

        // Append record with DEFAULT mode should succeed
        WriteRecord defaultRecord = createWriteRecordWithMergeMode(MergeMode.DEFAULT);
        assertThat(defaultBatch.tryAppend(defaultRecord, newWriteCallback()))
                .isEqualTo(AppendResult.APPENDED);

        // Append record with OVERWRITE mode should fail
        WriteRecord overwriteRecord = createWriteRecordWithMergeMode(MergeMode.OVERWRITE);
        assertThatThrownBy(() -> defaultBatch.tryAppend(overwriteRecord, newWriteCallback()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "Cannot mix records with different mergeMode in the same batch")
                .hasMessageContaining("Batch mergeMode: DEFAULT")
                .hasMessageContaining("Record mergeMode: OVERWRITE");
    }

    @Test
    void testOverwriteModeBatch() throws Exception {
        // Create batch with OVERWRITE mode
        KvWriteBatch overwriteBatch =
                createKvWriteBatchWithMergeMode(
                        new TableBucket(DATA1_TABLE_ID_PK, 0), MergeMode.OVERWRITE);

        // Verify batch has correct mergeMode
        assertThat(overwriteBatch.getMergeMode()).isEqualTo(MergeMode.OVERWRITE);

        // Append record with OVERWRITE mode should succeed
        WriteRecord overwriteRecord = createWriteRecordWithMergeMode(MergeMode.OVERWRITE);
        assertThat(overwriteBatch.tryAppend(overwriteRecord, newWriteCallback()))
                .isEqualTo(AppendResult.APPENDED);

        // Append record with DEFAULT mode should fail
        WriteRecord defaultRecord = createWriteRecordWithMergeMode(MergeMode.DEFAULT);
        assertThatThrownBy(() -> overwriteBatch.tryAppend(defaultRecord, newWriteCallback()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "Cannot mix records with different mergeMode in the same batch")
                .hasMessageContaining("Batch mergeMode: OVERWRITE")
                .hasMessageContaining("Record mergeMode: DEFAULT");
    }

    @Test
    void testDefaultMergeModeIsDefault() throws Exception {
        KvWriteBatch batch = createKvWriteBatch(new TableBucket(DATA1_TABLE_ID_PK, 0));
        assertThat(batch.getMergeMode()).isEqualTo(MergeMode.DEFAULT);
    }

    private KvWriteBatch createKvWriteBatchWithMergeMode(TableBucket tb, MergeMode mergeMode)
            throws Exception {
        PreAllocatedPagedOutputView outputView =
                new PreAllocatedPagedOutputView(
                        Collections.singletonList(memoryPool.nextSegment()));
        return new KvWriteBatch(
                tb.getBucket(),
                PhysicalTablePath.of(DATA1_TABLE_PATH_PK),
                DATA1_TABLE_INFO_PK.getSchemaId(),
                KvFormat.COMPACTED,
                Integer.MAX_VALUE,
                outputView,
                null,
                mergeMode,
                false,
                System.currentTimeMillis());
    }

    private WriteRecord createWriteRecordWithMergeMode(MergeMode mergeMode) {
        return WriteRecord.forUpsert(
                DATA1_TABLE_INFO_PK,
                PhysicalTablePath.of(DATA1_TABLE_PATH_PK),
                row,
                key,
                key,
                WriteFormat.COMPACTED_KV,
                null,
                mergeMode);
    }

    // ==================== V2 Format Tests ====================

    @Test
    void testV0BatchRejectsRetract() throws Exception {
        KvWriteBatch v0Batch = createKvWriteBatch(new TableBucket(DATA1_TABLE_ID_PK, 0));

        // UPSERT should succeed on V0 batch
        WriteRecord upsertRecord = createWriteRecord();
        assertThat(v0Batch.tryAppend(upsertRecord, newWriteCallback()))
                .isEqualTo(AppendResult.APPENDED);

        // RETRACT should be rejected on V0 batch
        WriteRecord retractRecord =
                WriteRecord.forRetract(
                        DATA1_TABLE_INFO_PK,
                        PhysicalTablePath.of(DATA1_TABLE_PATH_PK),
                        row,
                        key,
                        key,
                        WriteFormat.COMPACTED_KV,
                        null,
                        MergeMode.DEFAULT);
        assertThat(v0Batch.tryAppend(retractRecord, newWriteCallback()))
                .isEqualTo(AppendResult.FORMAT_MISMATCH);

        // Only the UPSERT record should be in the batch
        assertThat(v0Batch.getRecordCount()).isEqualTo(1);
    }

    @Test
    void testV2BatchAcceptsMixed() throws Exception {
        KvWriteBatch v2Batch = createV2KvWriteBatch(new TableBucket(DATA1_TABLE_ID_PK, 0));

        // RETRACT should succeed on V2 batch
        WriteRecord retractRecord =
                WriteRecord.forRetract(
                        DATA1_TABLE_INFO_PK,
                        PhysicalTablePath.of(DATA1_TABLE_PATH_PK),
                        row,
                        key,
                        key,
                        WriteFormat.COMPACTED_KV,
                        null,
                        MergeMode.DEFAULT);
        assertThat(v2Batch.tryAppend(retractRecord, newWriteCallback()))
                .isEqualTo(AppendResult.APPENDED);

        // UPSERT should also succeed on V2 batch
        WriteRecord upsertRecord = createWriteRecord();
        assertThat(v2Batch.tryAppend(upsertRecord, newWriteCallback()))
                .isEqualTo(AppendResult.APPENDED);

        // Both records should be in the batch
        assertThat(v2Batch.getRecordCount()).isEqualTo(2);

        // Verify mutation types survive serialization
        DefaultKvRecordBatch kvRecords = DefaultKvRecordBatch.pointToBytesView(v2Batch.build());
        KvRecordReadContext v2ReadContext =
                KvRecordReadContext.createReadContext(
                        KvFormat.COMPACTED, new TestingSchemaGetter(1, DATA1_SCHEMA));
        Iterator<KvRecord> iterator = kvRecords.records(v2ReadContext).iterator();
        assertThat(iterator.next().getMutationType()).isEqualTo(MutationType.RETRACT);
        assertThat(iterator.next().getMutationType()).isEqualTo(MutationType.UPSERT);
    }

    @Test
    void testV2FormatRetractRecordRoundTrip() throws Exception {
        KvWriteBatch v2Batch = createV2KvWriteBatch(new TableBucket(DATA1_TABLE_ID_PK, 0));

        // Append a RETRACT record
        WriteRecord retractRecord =
                WriteRecord.forRetract(
                        DATA1_TABLE_INFO_PK,
                        PhysicalTablePath.of(DATA1_TABLE_PATH_PK),
                        row,
                        key,
                        key,
                        WriteFormat.COMPACTED_KV,
                        null,
                        MergeMode.DEFAULT);
        assertThat(retractRecord.getMutationType()).isEqualTo(MutationType.RETRACT);
        assertThat(v2Batch.tryAppend(retractRecord, newWriteCallback()))
                .isEqualTo(AppendResult.APPENDED);

        // Build and read back with V2 ReadContext
        DefaultKvRecordBatch kvRecords = DefaultKvRecordBatch.pointToBytesView(v2Batch.build());
        assertThat(kvRecords.getRecordCount()).isEqualTo(1);

        KvRecordReadContext v2ReadContext =
                KvRecordReadContext.createReadContext(
                        KvFormat.COMPACTED, new TestingSchemaGetter(1, DATA1_SCHEMA));
        Iterator<KvRecord> iterator = kvRecords.records(v2ReadContext).iterator();
        assertThat(iterator.hasNext()).isTrue();
        KvRecord kvRecord = iterator.next();
        assertThat(toArray(kvRecord.getKey())).isEqualTo(key);
        assertThat(kvRecord.getRow()).isEqualTo(row);
        assertThat(kvRecord.getMutationType()).isEqualTo(MutationType.RETRACT);
    }

    @Test
    void testV2FormatUpsertRecordRoundTrip() throws Exception {
        KvWriteBatch v2Batch = createV2KvWriteBatch(new TableBucket(DATA1_TABLE_ID_PK, 0));

        // Append an UPSERT record
        WriteRecord upsertRecord = createWriteRecord();
        assertThat(v2Batch.tryAppend(upsertRecord, newWriteCallback()))
                .isEqualTo(AppendResult.APPENDED);

        // Build and read back with V2 ReadContext
        DefaultKvRecordBatch kvRecords = DefaultKvRecordBatch.pointToBytesView(v2Batch.build());
        assertThat(kvRecords.getRecordCount()).isEqualTo(1);

        KvRecordReadContext v2ReadContext =
                KvRecordReadContext.createReadContext(
                        KvFormat.COMPACTED, new TestingSchemaGetter(1, DATA1_SCHEMA));
        Iterator<KvRecord> iterator = kvRecords.records(v2ReadContext).iterator();
        assertThat(iterator.hasNext()).isTrue();
        KvRecord kvRecord = iterator.next();
        assertThat(toArray(kvRecord.getKey())).isEqualTo(key);
        assertThat(kvRecord.getRow()).isEqualTo(row);
        assertThat(kvRecord.getMutationType()).isEqualTo(MutationType.UPSERT);
    }

    @Test
    void testV2FormatMixedUpsertAndRetract() throws Exception {
        KvWriteBatch v2Batch = createV2KvWriteBatch(new TableBucket(DATA1_TABLE_ID_PK, 0));

        // Append an UPSERT record
        WriteRecord upsertRecord = createWriteRecord();
        assertThat(v2Batch.tryAppend(upsertRecord, newWriteCallback()))
                .isEqualTo(AppendResult.APPENDED);

        // Append a RETRACT record
        WriteRecord retractRecord =
                WriteRecord.forRetract(
                        DATA1_TABLE_INFO_PK,
                        PhysicalTablePath.of(DATA1_TABLE_PATH_PK),
                        row,
                        key,
                        key,
                        WriteFormat.COMPACTED_KV,
                        null,
                        MergeMode.DEFAULT);
        assertThat(v2Batch.tryAppend(retractRecord, newWriteCallback()))
                .isEqualTo(AppendResult.APPENDED);
        assertThat(v2Batch.getRecordCount()).isEqualTo(2);

        // Build and read back with V2 ReadContext
        DefaultKvRecordBatch kvRecords = DefaultKvRecordBatch.pointToBytesView(v2Batch.build());
        assertThat(kvRecords.getRecordCount()).isEqualTo(2);

        KvRecordReadContext v2ReadContext =
                KvRecordReadContext.createReadContext(
                        KvFormat.COMPACTED, new TestingSchemaGetter(1, DATA1_SCHEMA));
        Iterator<KvRecord> iterator = kvRecords.records(v2ReadContext).iterator();

        // First record: UPSERT
        assertThat(iterator.hasNext()).isTrue();
        KvRecord first = iterator.next();
        assertThat(first.getMutationType()).isEqualTo(MutationType.UPSERT);

        // Second record: RETRACT
        assertThat(iterator.hasNext()).isTrue();
        KvRecord second = iterator.next();
        assertThat(second.getMutationType()).isEqualTo(MutationType.RETRACT);
    }

    private KvWriteBatch createV2KvWriteBatch(TableBucket tb) throws Exception {
        PreAllocatedPagedOutputView outputView =
                new PreAllocatedPagedOutputView(
                        Collections.singletonList(memoryPool.nextSegment()));
        return new KvWriteBatch(
                tb.getBucket(),
                PhysicalTablePath.of(DATA1_TABLE_PATH_PK),
                DATA1_TABLE_INFO_PK.getSchemaId(),
                KvFormat.COMPACTED,
                Integer.MAX_VALUE,
                outputView,
                null,
                MergeMode.DEFAULT,
                true,
                System.currentTimeMillis());
    }
}
