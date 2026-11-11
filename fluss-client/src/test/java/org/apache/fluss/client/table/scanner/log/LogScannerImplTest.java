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

package org.apache.fluss.client.table.scanner.log;

import org.apache.fluss.client.metadata.TestingClientSchemaGetter;
import org.apache.fluss.client.metadata.TestingMetadataUpdater;
import org.apache.fluss.client.table.scanner.RemoteFileDownloader;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.LogDecodeException;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;
import org.apache.fluss.rpc.messages.FetchLogRequest;
import org.apache.fluss.rpc.messages.FetchLogResponse;
import org.apache.fluss.rpc.metrics.TestingClientMetricGroup;
import org.apache.fluss.server.entity.FetchReqInfo;
import org.apache.fluss.server.tablet.TestTabletServerGateway;
import org.apache.fluss.utils.crc.Crc32C;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.client.metadata.TestingMetadataUpdater.NODE1;
import static org.apache.fluss.client.metadata.TestingMetadataUpdater.NODE2;
import static org.apache.fluss.client.metadata.TestingMetadataUpdater.NODE3;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V0;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V1;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V2;
import static org.apache.fluss.record.LogRecordBatchFormat.MAGIC_OFFSET;
import static org.apache.fluss.record.LogRecordBatchFormat.crcOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.recordBatchHeaderSize;
import static org.apache.fluss.record.LogRecordBatchFormat.schemaIdOffset;
import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getFetchLogData;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeFetchLogResponse;
import static org.apache.fluss.testutils.DataTestUtils.createRecordsWithoutBaseLogOffset;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Test for {@link LogScannerImpl}. */
class LogScannerImplTest {

    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(30);

    private final TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1, LOG_MAGIC_VALUE_V2})
    void testSubscribeAtNextOffsetSkipsUndecodableBatch(byte magic) throws Exception {
        // Offsets 0-9 decode, 10-19 form one undecodable batch, 20-29 follow it.
        TreeMap<Long, MemoryLogRecords> logFromOffset = new TreeMap<>();
        logFromOffset.put(0L, concat(batch(0L, magic), undecodableBatch(batch(10L, magic))));
        logFromOffset.put(20L, batch(20L, magic));
        Configuration conf = new Configuration();
        TestingMetadataUpdater metadataUpdater =
                new TestingMetadataUpdater(
                        TestingMetadataUpdater.COORDINATOR,
                        Arrays.asList(NODE1, NODE2, NODE3),
                        Collections.singletonMap(DATA1_TABLE_PATH, DATA1_TABLE_INFO),
                        Collections.singletonMap(
                                1, new FixedLogTabletServerGateway(logFromOffset, 30L)),
                        conf);

        try (RemoteFileDownloader downloader = new RemoteFileDownloader(1);
                LogScannerImpl scanner =
                        new LogScannerImpl(
                                conf,
                                DATA1_TABLE_INFO,
                                metadataUpdater,
                                TestingClientMetricGroup.newInstance(),
                                downloader,
                                null,
                                new TestingClientSchemaGetter(
                                        DATA1_TABLE_PATH,
                                        new SchemaInfo(DATA1_SCHEMA, 0),
                                        metadataUpdater,
                                        conf),
                                null)) {
            scanner.subscribe(tb.getBucket(), 0L);
            assertThat(pollOffsets(scanner, 10)).containsExactlyElementsOf(range(0, 10));

            LogDecodeException failure = null;
            for (int i = 0; i < 3; i++) {
                Throwable thrown = catchThrowable(() -> scanner.poll(POLL_TIMEOUT));
                assertThat(thrown).isInstanceOf(LogDecodeException.class);
                failure = (LogDecodeException) thrown;
                assertThat(failure.getTableBucket()).isEqualTo(tb);
                assertThat(failure.getOffset()).isEqualTo(10L);
                assertThat(failure.getNextOffset()).isEqualTo(OptionalLong.of(20L));
            }

            scanner.subscribe(tb.getBucket(), failure.getNextOffset().getAsLong());
            assertThat(pollOffsets(scanner, 10)).containsExactlyElementsOf(range(20, 30));
        }
    }

    private static MemoryLogRecords batch(long baseOffset, byte magic) throws Exception {
        return createRecordsWithoutBaseLogOffset(
                DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, baseOffset, -1L, magic, DATA1, LogFormat.ARROW);
    }

    /** Garbles the records of a single batch but keeps its CRC valid, so its end is trusted. */
    private static MemoryLogRecords undecodableBatch(MemoryLogRecords batch) {
        byte[] bytes = toBytes(batch);
        byte magic = bytes[MAGIC_OFFSET];
        Arrays.fill(bytes, recordBatchHeaderSize(magic), bytes.length, (byte) 0xff);
        int crcStart = schemaIdOffset(magic);
        long crc = Crc32C.compute(bytes, crcStart, bytes.length - crcStart);
        MemorySegment.wrap(bytes).putInt(crcOffset(magic), (int) crc);
        return MemoryLogRecords.pointToBytes(bytes);
    }

    private static MemoryLogRecords concat(MemoryLogRecords first, MemoryLogRecords second) {
        byte[] head = toBytes(first);
        byte[] tail = toBytes(second);
        byte[] bytes = Arrays.copyOf(head, head.length + tail.length);
        System.arraycopy(tail, 0, bytes, head.length, tail.length);
        return MemoryLogRecords.pointToBytes(bytes);
    }

    private static byte[] toBytes(MemoryLogRecords records) {
        byte[] bytes = new byte[records.sizeInBytes()];
        records.getMemorySegment().get(records.getPosition(), bytes, 0, bytes.length);
        return bytes;
    }

    /** Polls until {@code count} records arrived, as a poll may return none or only some. */
    private List<Long> pollOffsets(LogScannerImpl scanner, int count) {
        List<Long> offsets = new ArrayList<>();
        retry(
                POLL_TIMEOUT,
                () -> {
                    for (ScanRecord record : scanner.poll(Duration.ofMillis(100)).records(tb)) {
                        offsets.add(record.logOffset());
                    }
                    assertThat(offsets).hasSizeGreaterThanOrEqualTo(count);
                });
        return offsets;
    }

    private static List<Long> range(long from, long to) {
        List<Long> range = new ArrayList<>();
        for (long offset = from; offset < to; offset++) {
            range.add(offset);
        }
        return range;
    }

    /**
     * Serves fixed log data: for a fetch offset below the high watermark, the entry with the
     * greatest start offset at or below it; nothing at or above the high watermark.
     */
    private static class FixedLogTabletServerGateway extends TestTabletServerGateway {
        private final TreeMap<Long, MemoryLogRecords> logFromOffset;
        private final long highWatermark;

        FixedLogTabletServerGateway(
                TreeMap<Long, MemoryLogRecords> logFromOffset, long highWatermark) {
            super(false, Collections.emptySet());
            this.logFromOffset = logFromOffset;
            this.highWatermark = highWatermark;
        }

        @Override
        public CompletableFuture<FetchLogResponse> fetchLog(FetchLogRequest request) {
            Map<TableBucket, FetchLogResultForBucket> resultForBucketMap = new HashMap<>();
            for (Map.Entry<TableBucket, FetchReqInfo> entry : getFetchLogData(request).entrySet()) {
                long fetchOffset = entry.getValue().getFetchOffset();
                MemoryLogRecords records =
                        fetchOffset < highWatermark
                                ? logFromOffset.floorEntry(fetchOffset).getValue()
                                : MemoryLogRecords.pointToBytes(new byte[0]);
                resultForBucketMap.put(
                        entry.getKey(),
                        FetchLogResultForBucket.records(
                                entry.getKey(), records, highWatermark, -1L, -1L));
            }
            return CompletableFuture.completedFuture(makeFetchLogResponse(resultForBucketMap));
        }
    }
}
