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

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.Scan;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.IOUtils;

import io.trino.spi.TrinoException;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.trino.spi.type.BigintType.BIGINT;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.apache.fluss.trino.TestingRowIterator.rows;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Verifies real page reads and controlled failures at scanner and resource boundaries. */
final class FlussPageSourceTest extends FlussTrinoTestBase {
    private TablePath tablePath;
    private TableInfo info;
    private FlussTableHandle handle;
    private FlussBucketHandle bucket;
    private TrackedTable table;
    private FlussPageSource source;

    @BeforeEach
    void setUp() throws Exception {
        source = null;
        table = null;
        tablePath =
                TablePath.of(
                        "Sales_page_source_" + UUID.randomUUID().toString().replace("-", ""),
                        "Users");

        info = createTable(tablePath, descriptor(true));
        handle = tableHandle(info);
        bucket = new FlussBucketHandle(info.getTableId(), Optional.empty(), 0);
        useTable(new TrackedTable(catalogTable(tablePath)));
    }

    @AfterEach
    void closeSource() throws Exception {
        IOUtils.closeAll(
                () -> {
                    if (source != null) {
                        source.close();
                    }
                },
                () -> {
                    if (table != null && table.closes == 0) {
                        table.close();
                    }
                });
    }

    @Test
    void testLazyInitializationAndEarlyClose() {
        source = create();
        assertThat(source.isBlocked().isDone()).isTrue();
        assertThat(source.isFinished()).isFalse();
        source.close();
        source.close();
        assertThat(source.getNextSourcePage()).isNull();
        assertThat(source.isFinished()).isTrue();
        assertThat(table.scans).isZero();
        assertThat(table.closes).isZero();
    }

    @Test
    void testEmptyBatchYieldsButNullFinishes() throws Exception {
        TestingRowIterator empty = rows(0);
        TestingBatchScanner scanner = new TestingBatchScanner(empty);
        useScanner(scanner);
        source = create();
        assertThat(source.getNextSourcePage()).isNull();
        assertThat(source.isFinished()).isFalse();
        assertThat(scanner.polls).isEqualTo(1);
        assertThat(empty.closes).isEqualTo(1);
        assertThat(source.getNextSourcePage()).isNull();
        assertThat(source.isFinished()).isTrue();
        assertThat(scanner.polls).isEqualTo(2);
        assertThat(scanner.closes).isEqualTo(1);
        assertThat(table.closes).isEqualTo(1);
    }

    @Test
    void testBatchSpansPagesWithoutAdditionalPoll() throws Exception {
        TestingRowIterator batch = rows(2050);
        TestingBatchScanner scanner = new TestingBatchScanner(batch);
        useScanner(scanner);
        source = create();
        int position = 0;
        for (int size : new int[] {1024, 1024, 2}) {
            SourcePage page = source.getNextSourcePage();
            assertThat(page.getPositionCount()).isEqualTo(size);
            for (int i = 0; i < size; i++) {
                assertThat(BIGINT.getLong(page.getBlock(0), i)).isEqualTo(position++);
            }
            assertThat(scanner.polls).isEqualTo(1);
            assertThat(source.isFinished()).isFalse();
        }
        assertThat(batch.closes).isEqualTo(1);
        assertThat(source.getCompletedPositions()).hasValue(2050);
        assertThat(source.getCompletedBytes()).isZero();
        assertThat(source.getNextSourcePage()).isNull();
        assertThat(source.isFinished()).isTrue();
    }

    @Test
    void testRealLogPagesCompleteAtStop() throws Exception {
        TablePath path = TablePath.of(tablePath.getDatabaseName(), "Logs");
        info = createTable(path, descriptor(false));
        handle = tableHandle(info);
        bucket = new FlussBucketHandle(info.getTableId(), Optional.empty(), 0);
        useTable(new TrackedTable(dataConnection().getTable(path)));
        AppendWriter writer = table.newAppend().createWriter();
        for (long i = 0; i < 2052; i++) {
            writer.append(GenericRow.of(i, i));
        }
        writer.flush();
        source =
                create(
                        FlussSplit.forLog(bucket, 0, 2050),
                        Collections.singletonList(new FlussColumnHandle("id", 0)),
                        MemoryContext.NO_LIMIT);
        int position = 0;
        while (!source.isFinished()) {
            SourcePage page = nextPage();
            if (page == null) {
                break;
            }
            assertThat(page.getPositionCount()).isBetween(1, 1024);
            for (int i = 0; i < page.getPositionCount(); i++) {
                assertThat(BIGINT.getLong(page.getBlock(0), i)).isEqualTo(position++);
            }
        }
        assertThat(position).isEqualTo(2050);
        assertThat(source.getCompletedPositions()).hasValue(2050);
        assertThat(source.getCompletedBytes()).isPositive();
        assertThat(table.closes).isEqualTo(1);
    }

    @Test
    void testReadsUsingPhysicalTablePath() throws Exception {
        writeRows(GenericRow.of(7L, 9L));
        source = create();

        SourcePage page = nextPage();
        assertThat(page).isNotNull();
        assertThat(page.getPositionCount()).isEqualTo(1);
        assertThat(BIGINT.getLong(page.getBlock(0), 0)).isEqualTo(7L);
    }

    @Test
    void testZeroColumnsRetainRealRowCount() throws Exception {
        writeRows(GenericRow.of(1L, 1L), GenericRow.of(2L, 2L), GenericRow.of(3L, 3L));
        source = create(FlussSplit.forKv(bucket), Collections.emptyList(), MemoryContext.NO_LIMIT);
        int count = 0;
        while (!source.isFinished()) {
            SourcePage page = nextPage();
            if (page != null) {
                assertThat(page.getChannelCount()).isZero();
                count += page.getPositionCount();
            }
        }
        assertThat(count).isEqualTo(3);
    }

    @Test
    void testReorderedAndNullColumns() throws Exception {
        writeRows(GenericRow.of(1L, null), GenericRow.of(2L, 7L));
        source =
                create(
                        FlussSplit.forKv(bucket),
                        Arrays.asList(
                                new FlussColumnHandle("value", 1), new FlussColumnHandle("id", 0)),
                        MemoryContext.NO_LIMIT);
        List<Long> ids = new ArrayList<>();
        while (!source.isFinished()) {
            SourcePage page = nextPage();
            if (page == null) {
                continue;
            }
            for (int i = 0; i < page.getPositionCount(); i++) {
                long id = BIGINT.getLong(page.getBlock(1), i);
                ids.add(id);
                if (id == 1) {
                    assertThat(page.getBlock(0).isNull(i)).isTrue();
                } else {
                    assertThat(BIGINT.getLong(page.getBlock(0), i)).isEqualTo(7);
                }
            }
        }
        assertThat(ids).containsExactlyInAnyOrder(1L, 2L);
    }

    @Test
    void testIdentityAndScanTypeCheckedBeforeScannerCreation() throws Exception {
        for (int mismatch = 0; mismatch < 3; mismatch++) {
            useTable(new TrackedTable(catalogTable(tablePath)));
            handle =
                    new FlussTableHandle(
                            tablePath.getDatabaseName().toLowerCase(Locale.ENGLISH),
                            tablePath.getTableName().toLowerCase(Locale.ENGLISH),
                            tablePath.getDatabaseName(),
                            tablePath.getTableName(),
                            info.getTableId() + (mismatch == 0 ? 1 : 0),
                            info.getSchemaId() + (mismatch == 1 ? 1 : 0),
                            TupleDomain.all());
            source =
                    create(
                            mismatch == 2
                                    ? FlussSplit.forLog(bucket, 0, 1)
                                    : FlussSplit.forKv(bucket),
                            Collections.singletonList(new FlussColumnHandle("id", 0)),
                            MemoryContext.NO_LIMIT);
            assertThatThrownBy(source::getNextSourcePage).isInstanceOf(TrinoException.class);
            assertThat(source.isFinished()).isTrue();
            assertThat(table.closes).isEqualTo(1);
            assertThat(table.scans).isZero();
        }
    }

    @Test
    void testInvalidColumnFailsBeforeScannerCreation() {
        source =
                create(
                        FlussSplit.forKv(bucket),
                        Collections.singletonList(new FlussColumnHandle("wrong", 0)),
                        MemoryContext.NO_LIMIT);
        assertThatThrownBy(source::getNextSourcePage).hasMessageContaining("schema");
        assertThat(table.closes).isEqualTo(1);
        assertThat(table.scans).isZero();
    }

    @Test
    void testMetadataFailureClosesTable() throws Exception {
        RuntimeException failure = new IllegalStateException("metadata failed");
        useTable(
                new TrackedTable(catalogTable(tablePath)) {
                    @Override
                    public TableInfo getTableInfo() {
                        throw failure;
                    }
                });
        source = create();
        assertThatThrownBy(source::getNextSourcePage).isSameAs(failure);
        assertThat(table.closes).isEqualTo(1);
        assertThat(table.scans).isZero();
    }

    @Test
    void testWorkerRejectsUnsupportedCapabilitiesBeforeScannerCreation() throws Exception {
        for (boolean partitioned : new boolean[] {true, false}) {
            TablePath path =
                    TablePath.of(
                            tablePath.getDatabaseName(), partitioned ? "Partitioned" : "Tiered");
            TableDescriptor.Builder descriptor =
                    TableDescriptor.builder()
                            .schema(
                                    Schema.newBuilder()
                                            .column("id", DataTypes.BIGINT())
                                            .column("value", DataTypes.BIGINT())
                                            .primaryKey("id", "value")
                                            .build())
                            .distributedBy(1);
            if (partitioned) {
                descriptor.partitionedBy("value");
            } else {
                descriptor.property("table.datalake.enabled", "true");
            }
            TableInfo unsupported = createTable(path, descriptor.build());
            handle = tableHandle(unsupported);
            bucket = new FlussBucketHandle(unsupported.getTableId(), Optional.empty(), 0);
            useTable(new TrackedTable(catalogTable(path)));
            source = create();
            assertThatThrownBy(source::getNextSourcePage).isInstanceOf(TrinoException.class);
            assertThat(table.closes).isEqualTo(1);
            assertThat(table.scans).isZero();
        }
    }

    @Test
    void testInitializationFailureClosesAcquiredTable() throws Exception {
        RuntimeException failure = new IllegalStateException("create failed");
        useTable(
                new TrackedTable(catalogTable(tablePath)) {
                    @Override
                    BatchScanner createBatchScanner(Scan scan, TableBucket requested) {
                        throw failure;
                    }
                });
        source = create();
        assertThatThrownBy(source::getNextSourcePage).isSameAs(failure);
        assertThat(table.closes).isEqualTo(1);
    }

    @Test
    void testOpenFailureDoesNotRetryAndRestoresClassLoader() throws Exception {
        RuntimeException failure = new IllegalStateException("open failed");
        clients.close();
        int[] opens = {0};
        clients =
                new FlussClientManager(
                        clientConfig(),
                        ignored ->
                                new TestingConnection(catalogConnection()) {
                                    @Override
                                    public Admin getAdmin() {
                                        return testingAdmin();
                                    }

                                    @Override
                                    public Table getTable(TablePath path) {
                                        opens[0]++;
                                        throw failure;
                                    }

                                    @Override
                                    public void close() {}
                                });
        source = create();
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        ClassLoader other = new ClassLoader(null) {};
        try {
            Thread.currentThread().setContextClassLoader(other);
            assertThatThrownBy(source::getNextSourcePage).isSameAs(failure);
            assertThat(Thread.currentThread().getContextClassLoader()).isSameAs(other);
            assertThat(source.getNextSourcePage()).isNull();
            assertThat(source.isFinished()).isTrue();
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
        assertThat(opens[0]).isEqualTo(1);
    }

    @Test
    void testPollIOExceptionIncludesBucketAndPreservesCleanup() throws Exception {
        IOException failure = new IOException("snapshot expired");
        IOException cleanup = new IOException("close failed");
        TestingBatchScanner scanner =
                new TestingBatchScanner() {
                    @Override
                    public CloseableIterator<InternalRow> pollBatch(Duration timeout)
                            throws IOException {
                        super.pollBatch(timeout);
                        throw failure;
                    }

                    @Override
                    public void close() throws IOException {
                        super.close();
                        throw cleanup;
                    }
                };
        useScanner(scanner);
        source = create();
        assertThatThrownBy(source::getNextSourcePage)
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("bucketId=0")
                .hasCause(failure)
                .satisfies(error -> assertThat(error.getSuppressed()).hasSize(1));
        assertThat(source.getReadTimeNanos()).isPositive();
        assertThat(source.getNextSourcePage()).isNull();
        assertThat(table.closes).isEqualTo(1);
        assertThat(scanner.polls).isEqualTo(1);
    }

    @Test
    void testIteratorFailuresCloseAllResources() throws Exception {
        for (boolean failHasNext : new boolean[] {true, false}) {
            RuntimeException failure = new IllegalStateException("iterator failed");
            TestingRowIterator batch =
                    new TestingRowIterator(rows(1)) {
                        @Override
                        public boolean hasNext() {
                            if (failHasNext) {
                                throw failure;
                            }
                            return super.hasNext();
                        }

                        @Override
                        public InternalRow next() {
                            throw failure;
                        }
                    };
            TestingBatchScanner scanner = new TestingBatchScanner(batch);
            useScanner(scanner);
            source = create();
            assertThatThrownBy(source::getNextSourcePage).isSameAs(failure);
            assertThat(batch.closes).isEqualTo(1);
            assertThat(scanner.closes).isEqualTo(1);
            assertThat(table.closes).isEqualTo(1);
        }
    }

    @Test
    void testDecodeFailurePreservesIteratorCloseFailure() throws Exception {
        RuntimeException cleanup = new IllegalStateException("iterator close failed");
        // A malformed row exercises the real BIGINT decoder's type check.
        InternalRow invalid = GenericRow.of("not a long", 1L);
        TestingRowIterator batch =
                new TestingRowIterator(Arrays.asList(invalid, invalid).iterator()) {
                    @Override
                    public void close() {
                        super.close();
                        throw cleanup;
                    }
                };
        TestingBatchScanner scanner = new TestingBatchScanner(batch);
        useScanner(scanner);
        source = create();
        assertThatThrownBy(source::getNextSourcePage)
                .isInstanceOf(ClassCastException.class)
                .satisfies(
                        error -> {
                            assertThat(error.getSuppressed()).hasSize(1);
                            assertThat(error.getSuppressed()[0]).hasCause(cleanup);
                        });
        assertThat(batch.closes).isEqualTo(1);
        assertThat(scanner.closes).isEqualTo(1);
        assertThat(table.closes).isEqualTo(1);
    }

    @Test
    void testExhaustedIteratorCloseFailureIsNotRetried() throws Exception {
        RuntimeException failure = new IllegalStateException("iterator close failed");
        TestingRowIterator batch =
                new TestingRowIterator(rows(1)) {
                    @Override
                    public void close() {
                        super.close();
                        throw failure;
                    }
                };
        TestingBatchScanner scanner = new TestingBatchScanner(batch);
        useScanner(scanner);
        source = create();
        assertThatThrownBy(source::getNextSourcePage).isSameAs(failure);
        source.close();
        assertThat(batch.closes).isEqualTo(1);
        assertThat(scanner.closes).isEqualTo(1);
        assertThat(table.closes).isEqualTo(1);
        assertThat(source.getCompletedPositions()).hasValue(0);
    }

    @Test
    void testEarlyCloseOrderAndSuppressedFailures() throws Exception {
        List<String> closed = new ArrayList<>();
        RuntimeException iteratorFailure = new IllegalStateException("iterator close failed");
        IOException scannerFailure = new IOException("scanner close failed");
        IOException tableFailure = new IOException("table close failed");
        RuntimeException memoryFailure = new IllegalStateException("memory release failed");
        TestingRowIterator batch =
                new TestingRowIterator(rows(2050)) {
                    @Override
                    public void close() {
                        super.close();
                        closed.add("iterator");
                        throw iteratorFailure;
                    }
                };
        TestingBatchScanner scanner =
                new TestingBatchScanner(batch) {
                    @Override
                    public void close() throws IOException {
                        super.close();
                        closed.add("scanner");
                        throw scannerFailure;
                    }
                };
        useTable(
                new TrackedTable(catalogTable(tablePath)) {
                    @Override
                    BatchScanner createBatchScanner(Scan scan, TableBucket requested) {
                        return scanner;
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        closed.add("table");
                        throw tableFailure;
                    }
                });
        boolean[] closing = {false};
        MemoryContext memory =
                bytes -> {
                    if (closing[0] && bytes == 0) {
                        closed.add("memory");
                        throw memoryFailure;
                    }
                };
        source = create(FlussSplit.forKv(bucket), Collections.emptyList(), memory);
        source.getNextSourcePage();
        closing[0] = true;
        assertThatThrownBy(source::close).hasCause(iteratorFailure);
        assertThat(iteratorFailure.getSuppressed())
                .containsExactly(scannerFailure, tableFailure, memoryFailure);
        source.close();
        assertThat(closed).containsExactly("iterator", "scanner", "table", "memory");
        assertThat(source.getNextSourcePage()).isNull();
    }

    @Test
    void testCloseErrorStillReleasesOtherResources() throws Exception {
        AssertionError failure = new AssertionError("iterator close error");
        TestingRowIterator batch =
                new TestingRowIterator(rows(2050)) {
                    @Override
                    public void close() {
                        super.close();
                        throw failure;
                    }
                };
        TestingBatchScanner scanner = new TestingBatchScanner(batch);
        useScanner(scanner);
        List<Long> reservations = new ArrayList<>();
        source = create(FlussSplit.forKv(bucket), Collections.emptyList(), reservations::add);
        source.getNextSourcePage();
        assertThatThrownBy(source::close).isSameAs(failure);
        assertThat(scanner.closes).isEqualTo(1);
        assertThat(table.closes).isEqualTo(1);
        assertThat(reservations.get(reservations.size() - 1)).isZero();
        source.close();
        assertThat(batch.closes).isEqualTo(1);
    }

    @Test
    void testMemoryFailureClosesRemainingIterator() throws Exception {
        TestingRowIterator batch = rows(2050);
        TestingBatchScanner scanner = new TestingBatchScanner(batch);
        useScanner(scanner);
        RuntimeException failure = new IllegalStateException("reservation denied");
        List<Long> reservations = new ArrayList<>();
        MemoryContext memory =
                bytes -> {
                    reservations.add(bytes);
                    if (bytes > 0) {
                        throw failure;
                    }
                };
        source =
                create(
                        FlussSplit.forKv(bucket),
                        Collections.singletonList(new FlussColumnHandle("id", 0)),
                        memory);
        assertThatThrownBy(source::getNextSourcePage).isSameAs(failure);
        assertThat(batch.closes).isEqualTo(1);
        assertThat(scanner.closes).isEqualTo(1);
        assertThat(table.closes).isEqualTo(1);
        assertThat(reservations.get(reservations.size() - 1)).isZero();
        assertThat(source.getCompletedPositions()).hasValue(0);
    }

    @Test
    void testPollingAndMemoryStayOnCallingThread() throws Exception {
        Thread caller = Thread.currentThread();
        writeRows(GenericRow.of(1L, 1L));
        List<Long> reservations = new ArrayList<>();
        MemoryContext memory =
                bytes -> {
                    assertThat(Thread.currentThread()).isSameAs(caller);
                    reservations.add(bytes);
                };
        useTable(
                new TrackedTable(dataConnection().getTable(tablePath)) {
                    @Override
                    BatchScanner createBatchScanner(Scan scan, TableBucket requested) {
                        BatchScanner delegate = super.createBatchScanner(scan, requested);
                        return new BatchScanner() {
                            @Override
                            public CloseableIterator<InternalRow> pollBatch(Duration timeout)
                                    throws IOException {
                                assertThat(Thread.currentThread()).isSameAs(caller);
                                return delegate.pollBatch(timeout);
                            }

                            @Override
                            public void close() throws IOException {
                                delegate.close();
                            }
                        };
                    }
                });
        source =
                create(
                        FlussSplit.forKv(bucket),
                        Collections.singletonList(new FlussColumnHandle("id", 0)),
                        memory);
        assertThat(nextPage()).isNotNull();
        assertThat(reservations).anyMatch(value -> value > 0);
        assertThat(reservations.get(reservations.size() - 1)).isZero();
        assertThat(source.getCompletedBytes()).isZero();
    }

    @Test
    void testReadAndCloseUsePluginClassLoader() throws Exception {
        writeRows(GenericRow.of(1L, 1L));
        useTable(
                new TrackedTable(dataConnection().getTable(tablePath)) {
                    @Override
                    public TableInfo getTableInfo() {
                        assertPluginClassLoader();
                        return super.getTableInfo();
                    }

                    @Override
                    BatchScanner createBatchScanner(Scan scan, TableBucket requested) {
                        assertPluginClassLoader();
                        BatchScanner delegate = super.createBatchScanner(scan, requested);
                        return new BatchScanner() {
                            @Override
                            public CloseableIterator<InternalRow> pollBatch(Duration timeout)
                                    throws IOException {
                                assertPluginClassLoader();
                                return delegate.pollBatch(timeout);
                            }

                            @Override
                            public void close() throws IOException {
                                assertPluginClassLoader();
                                delegate.close();
                            }
                        };
                    }

                    @Override
                    public void close() throws Exception {
                        assertPluginClassLoader();
                        super.close();
                    }
                });
        source = create();
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        ClassLoader other = new ClassLoader(null) {};
        try {
            Thread.currentThread().setContextClassLoader(other);
            assertThat(nextPage()).isNotNull();
            assertThat(Thread.currentThread().getContextClassLoader()).isSameAs(other);
            source.close();
            assertThat(source.isFinished()).isTrue();
            assertThat(Thread.currentThread().getContextClassLoader()).isSameAs(other);
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
        assertThat(table.closes).isEqualTo(1);
    }

    private void useScanner(BatchScanner scanner) throws Exception {
        useTable(
                new TrackedTable(catalogTable(tablePath)) {
                    @Override
                    BatchScanner createBatchScanner(Scan scan, TableBucket requested) {
                        assertThat(requested).isEqualTo(bucket.toTableBucket());
                        return scanner;
                    }
                });
    }

    private void useTable(TrackedTable replacement) throws Exception {
        TablePath expectedPath =
                TablePath.of(handle.getFlussDatabaseName(), handle.getFlussTableName());
        clients.close();
        if (table != null && table.closes == 0) {
            table.close();
        }
        table = replacement;
        clients =
                new FlussClientManager(
                        clientConfig(),
                        ignored ->
                                new TestingConnection(catalogConnection()) {
                                    @Override
                                    public Admin getAdmin() {
                                        return testingAdmin();
                                    }

                                    @Override
                                    public Table getTable(TablePath path) {
                                        assertPluginClassLoader();
                                        assertThat(path).isEqualTo(expectedPath);
                                        return replacement;
                                    }

                                    @Override
                                    public void close() {
                                        // The fixture owns the catalog connection.
                                    }
                                });
    }

    private void writeRows(InternalRow... rows) throws Exception {
        useTable(new TrackedTable(dataConnection().getTable(tablePath)));
        UpsertWriter writer = table.newUpsert().createWriter();
        for (InternalRow row : rows) {
            writer.upsert(row).get(30, TimeUnit.SECONDS);
        }
    }

    private SourcePage nextPage() throws Exception {
        SourcePage[] page = {null};
        waitUntil(
                () -> {
                    page[0] = source.getNextSourcePage();
                    return page[0] != null || source.isFinished();
                },
                Duration.ofSeconds(30),
                "Page source did not make progress");
        return page[0];
    }

    private FlussPageSource create() {
        return create(
                FlussSplit.forKv(bucket),
                Collections.singletonList(new FlussColumnHandle("id", 0)),
                MemoryContext.NO_LIMIT);
    }

    private FlussPageSource create(
            FlussSplit split, List<FlussColumnHandle> columns, MemoryContext memory) {
        return new FlussPageSource(clients, handle, split, columns, memory);
    }

    private static TableDescriptor descriptor(boolean primaryKey) {
        Schema.Builder schema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("value", DataTypes.BIGINT());
        if (primaryKey) {
            schema.primaryKey("id");
        }
        return TableDescriptor.builder().schema(schema.build()).distributedBy(1).build();
    }

    private static void assertPluginClassLoader() {
        assertThat(Thread.currentThread().getContextClassLoader())
                .isSameAs(FlussPageSource.class.getClassLoader());
    }

    private static class TrackedTable extends TestingScanTable {
        int scans;
        int closes;

        TrackedTable(Table delegate) {
            super(delegate);
        }

        @Override
        public Scan newScan() {
            scans++;
            return super.newScan();
        }

        @Override
        public void close() throws Exception {
            closes++;
            super.close();
        }
    }
}
