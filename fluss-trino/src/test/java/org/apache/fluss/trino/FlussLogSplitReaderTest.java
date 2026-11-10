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

package org.apache.fluss.trino;

import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.Scan;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.DataTypes;

import io.trino.spi.TrinoException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.apache.fluss.trino.FlussSplitReader.PollResult.AVAILABLE;
import static org.apache.fluss.trino.FlussSplitReader.PollResult.FINISHED;
import static org.apache.fluss.trino.FlussSplitReader.PollResult.YIELD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Verifies real log boundaries and controlled scanner progress and failure contracts. */
final class FlussLogSplitReaderTest extends FlussTrinoTestBase {
    private static final Duration TIMEOUT = Duration.ofMillis(100);
    private Table table;
    private FlussBucketHandle bucket;

    @BeforeEach
    void setUp() throws Exception {
        table = null;
        TablePath path = TablePath.of("fluss", "log_reader");
        TableInfo info =
                createTable(
                        path,
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("id", DataTypes.BIGINT())
                                                .build())
                                .distributedBy(1)
                                .build());
        bucket = new FlussBucketHandle(info.getTableId(), Optional.empty(), 0);
        table = catalogTable(path);
    }

    @AfterEach
    void closeTable() throws Exception {
        if (table != null) {
            table.close();
        }
    }

    @Test
    void testEmptyRangeDoesNotCreateScanner() throws Exception {
        Table unused =
                new TestingScanTable(table) {
                    @Override
                    public Scan newScan() {
                        throw new AssertionError("An empty range must not acquire a scanner");
                    }
                };
        try (FlussLogSplitReader reader =
                new FlussLogSplitReader(unused, bucket, new FlussLogRange(1000, 1000))) {
            assertThat(reader.isFinished()).isTrue();
            assertThat(reader.poll(TIMEOUT)).isEqualTo(FINISHED);
            assertThat(reader.hasNext()).isFalse();
        }
    }

    @Test
    void testStopsAtExclusiveBoundaryAndCountsOnlyConsumedBytes() throws Exception {
        useDataTable();
        AppendWriter writer = table.newAppend().createWriter();
        for (long i = 0; i < 12; i++) {
            writer.append(GenericRow.of(i));
        }
        writer.flush();
        try (FlussLogSplitReader reader = create(5, 10)) {
            awaitRecords(reader);
            assertThat(reader.getRetainedSizeInBytes()).isPositive();
            assertThat(reader.getCompletedBytes()).isZero();
            assertThatThrownBy(() -> reader.poll(TIMEOUT))
                    .isInstanceOf(IllegalStateException.class);
            List<Long> values = new ArrayList<>();
            while (!reader.isFinished()) {
                while (reader.hasNext()) {
                    values.add(reader.next().getLong(0));
                }
                if (!reader.isFinished()) {
                    awaitRecords(reader);
                }
            }
            assertThat(values).containsExactly(5L, 6L, 7L, 8L, 9L);
            assertThat(reader.getCompletedBytes()).isPositive();
            long bytes = reader.getCompletedBytes();
            assertThat(reader.getRetainedSizeInBytes()).isZero();
            assertThat(reader.poll(TIMEOUT)).isEqualTo(FINISHED);
            assertThat(reader.getCompletedBytes()).isEqualTo(bytes);
        }
    }

    @Test
    void testSparseBatchIgnoresRecordsAtAndBeyondStop() throws Exception {
        useScanner(new TestingLogScanner(records(5, 9, 10, 11)));
        try (FlussLogSplitReader reader = create(5, 10)) {
            assertThat(reader.poll(TIMEOUT)).isEqualTo(AVAILABLE);
            assertThat(reader.next().getLong(0)).isEqualTo(5);
            assertThat(reader.next().getLong(0)).isEqualTo(9);
            assertThat(reader.isFinished()).isTrue();
            assertThat(reader.hasNext()).isFalse();
            assertThat(reader.getCompletedBytes()).isEqualTo(16);
            assertThat(reader.getRetainedSizeInBytes()).isZero();
        }
    }

    @Test
    void testSmallBatchYieldsUntilProgressReachesStop() throws Exception {
        TestingLogScanner scanner =
                new TestingLogScanner(
                        records(5),
                        ScanRecords.EMPTY,
                        new ScanRecords(
                                Collections.emptyMap(),
                                Collections.singletonMap(bucket.toTableBucket(), 10L)));
        useScanner(scanner);
        try (FlussLogSplitReader reader = create(5, 10)) {
            assertThat(reader.poll(TIMEOUT)).isEqualTo(AVAILABLE);
            reader.next();
            assertThat(reader.isFinished()).isFalse();
            assertThat(reader.getCompletedBytes()).isEqualTo(8);
            assertThat(reader.poll(TIMEOUT)).isEqualTo(YIELD);
            assertThat(reader.poll(TIMEOUT)).isEqualTo(FINISHED);
            assertThat(reader.getCompletedBytes()).isEqualTo(8);
        }
        assertThat(scanner.closes).isEqualTo(1);
    }

    @Test
    void testRecordBeforeStartFails() throws Exception {
        useScanner(new TestingLogScanner(records(4)));
        try (FlussLogSplitReader reader = create(5, 10)) {
            assertThatThrownBy(() -> reader.poll(TIMEOUT))
                    .isInstanceOf(TrinoException.class)
                    .hasMessageContaining("precedes");
        }
    }

    @Test
    void testPartitionedSubscription() throws Exception {
        TablePath path = TablePath.of("fluss", "partitioned_log_reader");
        TableInfo info =
                createTable(
                        path,
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("id", DataTypes.BIGINT())
                                                .column("region", DataTypes.INT())
                                                .build())
                                .partitionedBy("region")
                                .distributedBy(1)
                                .build());
        admin.createPartition(
                        path, new PartitionSpec(Collections.singletonMap("region", "7")), false)
                .get(30, TimeUnit.SECONDS);
        long partitionId =
                admin.listPartitionInfos(path).get(30, TimeUnit.SECONDS).get(0).getPartitionId();
        CLUSTER.waitAndGetLeader(new TableBucket(info.getTableId(), partitionId, 0));
        try (Table partitioned = dataConnection().getTable(path)) {
            partitioned
                    .newAppend()
                    .createWriter()
                    .append(GenericRow.of(5L, 7))
                    .get(30, TimeUnit.SECONDS);
            FlussBucketHandle partition =
                    new FlussBucketHandle(info.getTableId(), Optional.of(partitionId), 0);
            try (FlussLogSplitReader reader =
                    new FlussLogSplitReader(partitioned, partition, new FlussLogRange(0, 1))) {
                awaitRecords(reader);
                assertThat(reader.next().getLong(0)).isEqualTo(5);
                assertThat(reader.isFinished()).isTrue();
            }
        }
    }

    @Test
    void testSubscribeFailurePreservesCleanupFailure() throws Exception {
        RuntimeException failure = new IllegalStateException("subscribe failed");
        IOException cleanup = new IOException("close failed");
        TestingLogScanner scanner =
                new TestingLogScanner() {
                    @Override
                    public void subscribe(int requestedBucket, long offset) {
                        assertThat(requestedBucket).isEqualTo(bucket.getBucketId());
                        assertThat(offset).isEqualTo(5);
                        throw failure;
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        throw cleanup;
                    }
                };
        useScanner(scanner);
        assertThatThrownBy(() -> create(5, 10)).isSameAs(failure);
        assertThat(failure.getSuppressed()).containsExactly(cleanup);
        assertThat(scanner.closes).isEqualTo(1);
    }

    @Test
    void testPollFailureHasReadErrorContext() throws Exception {
        RuntimeException failure = new IllegalStateException("poll failed");
        useScanner(
                new TestingLogScanner() {
                    @Override
                    public ScanRecords poll(Duration timeout) {
                        throw failure;
                    }
                });
        try (FlussLogSplitReader reader = create(5, 10)) {
            assertThatThrownBy(() -> reader.poll(TIMEOUT))
                    .isInstanceOf(TrinoException.class)
                    .hasCause(failure)
                    .hasMessageContaining("bucketId=0");
        }
    }

    @Test
    void testEarlyCloseClearsBufferAndIsIdempotent() throws Exception {
        useDataTable();
        AppendWriter writer = table.newAppend().createWriter();
        writer.append(GenericRow.of(5L));
        writer.append(GenericRow.of(6L));
        writer.flush();
        try (FlussLogSplitReader reader = create(0, 10)) {
            awaitRecords(reader);
            reader.close();
            reader.close();
            assertThat(reader.hasNext()).isFalse();
            assertThat(reader.isFinished()).isTrue();
            assertThat(reader.getRetainedSizeInBytes()).isZero();
            assertThat(reader.poll(TIMEOUT)).isEqualTo(FINISHED);
        }
    }

    private void useScanner(LogScanner scanner) {
        table =
                new TestingScanTable(table) {
                    @Override
                    LogScanner createLogScanner(Scan scan) {
                        return scanner;
                    }
                };
    }

    private FlussLogSplitReader create(long start, long stop) {
        return new FlussLogSplitReader(table, bucket, new FlussLogRange(start, stop));
    }

    private ScanRecords records(long... offsets) {
        List<ScanRecord> records = new ArrayList<>();
        for (long offset : offsets) {
            records.add(
                    new ScanRecord(
                            bucket.getTableId(),
                            1,
                            offset,
                            0,
                            ChangeType.INSERT,
                            GenericRow.of(offset),
                            8));
        }
        return new ScanRecords(Collections.singletonMap(bucket.toTableBucket(), records));
    }

    private static void awaitRecords(FlussLogSplitReader reader) throws Exception {
        waitUntil(
                () -> reader.poll(TIMEOUT) != YIELD,
                Duration.ofSeconds(30),
                "Log scanner did not make progress");
    }

    private void useDataTable() throws Exception {
        Table previous = table;
        table = null;
        previous.close();
        table = dataConnection().getTable(TablePath.of("fluss", "log_reader"));
    }
}
