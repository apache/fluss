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

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.batch.KvBatchScanner;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.exception.ScannerExpiredException;
import org.apache.fluss.exception.UnknownScannerIdException;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.ScanKvRequest;
import org.apache.fluss.rpc.messages.ScanKvResponse;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.IOUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Real-client checks for the difference between local close and remote snapshot reclamation. */
class FlussKvScannerLifecycleITCase {

    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(30);
    private static final int ROW_COUNT = 200;
    private static final String PAYLOAD = new String(new char[512]).replace('\0', 'x');

    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(clusterConfig())
                    .build();

    private Connection connection;
    private Admin admin;
    private TestingKvScanControl control;

    @BeforeEach
    void setUp() {
        connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
        admin = connection.getAdmin();
    }

    @AfterEach
    void tearDown() throws Exception {
        try {
            if (control != null) {
                control.removeAll();
            }
        } finally {
            IOUtils.closeAll(admin, connection);
        }
    }

    @Test
    void testLostOpenResponseRequiresRemoteExpiration() throws Exception {
        TableInfo info = createTable("lost_open");
        FaultGateway gateway = new FaultGateway(info, Failure.LOST_OPEN);
        try (KvBatchScanner scanner = newScanner(info, gateway)) {
            assertThatThrownBy(() -> scanner.pollBatch(POLL_TIMEOUT))
                    .isInstanceOf(IOException.class)
                    .hasStackTraceContaining("lost open response");
            scanner.close();
            assertThat(scanner.pollBatch(POLL_TIMEOUT)).isNull();
            assertThat(gateway.opens.get()).isEqualTo(1);
            assertThat(gateway.closes.get()).isZero();
            assertThat(control.activeScannerCount()).isEqualTo(1);
            control.expireAll();
            assertThat(control.activeScannerCount()).isZero();
        }
    }

    @Test
    void testFailedCloseCompletesLocallyBeforeRemoteExpiration() throws Exception {
        TableInfo info = createTable("failed_close");
        FaultGateway gateway = new FaultGateway(info, Failure.CLOSE);
        try (KvBatchScanner scanner = newScanner(info, gateway)) {
            assertFirstBatch(scanner);
            waitForPrefetch(gateway);
            assertThat(control.activeScannerCount()).isEqualTo(1);
            scanner.close();
            scanner.close();
            assertThat(scanner.pollBatch(POLL_TIMEOUT)).isNull();
            assertThat(gateway.opens.get()).isEqualTo(1);
            assertThat(gateway.closes.get()).isEqualTo(1);
            assertThat(control.activeScannerCount()).isEqualTo(1);
            control.expireAll();
            assertThat(control.activeScannerCount()).isZero();
        }
    }

    @Test
    void testLostContinuationFailsWithoutReopeningSnapshot() throws Exception {
        TableInfo info = createTable("lost_continuation");
        FaultGateway gateway = new FaultGateway(info, Failure.CONTINUATION);
        try (KvBatchScanner scanner = newScanner(info, gateway)) {
            assertFirstBatch(scanner);
            waitForPrefetch(gateway);
            assertThatThrownBy(() -> scanner.pollBatch(POLL_TIMEOUT))
                    .isInstanceOf(IOException.class)
                    .hasStackTraceContaining("lost continuation response");
            assertThat(scanner.pollBatch(POLL_TIMEOUT)).isNull();
            assertThat(gateway.opens.get()).isEqualTo(1);
            assertThat(gateway.closes.get()).isEqualTo(1);
            waitUntil(
                    () -> control.activeScannerCount() == 0,
                    POLL_TIMEOUT,
                    "Best-effort close did not release the server scanner");
        }
    }

    @Test
    void testExpiredSessionFailsWithoutReopeningSnapshot() throws Exception {
        assertInvalidatedSessionFails("expired_session", true);
    }

    @Test
    void testUnknownSessionFailsWithoutReopeningSnapshot() throws Exception {
        assertInvalidatedSessionFails("unknown_session", false);
    }

    private void assertInvalidatedSessionFails(String tableName, boolean expire) throws Exception {
        TableInfo info = createTable(tableName);
        FaultGateway gateway = new FaultGateway(info, Failure.NONE);
        try (KvBatchScanner scanner = newScanner(info, gateway)) {
            assertFirstBatch(scanner);
            waitForPrefetch(gateway);
            assertThat(control.activeScannerCount()).isEqualTo(1);
            if (expire) {
                control.expireAll();
            } else {
                control.removeAll();
            }
            assertThat(control.activeScannerCount()).isZero();

            // The already fetched second batch remains valid; its next prefetch sees the loss.
            assertFirstBatch(scanner);
            assertThatThrownBy(() -> scanner.pollBatch(POLL_TIMEOUT))
                    .isInstanceOf(IOException.class)
                    .hasCauseInstanceOf(
                            expire
                                    ? ScannerExpiredException.class
                                    : UnknownScannerIdException.class);
            assertThat(scanner.pollBatch(POLL_TIMEOUT)).isNull();
            scanner.close();
            assertThat(gateway.opens.get()).isEqualTo(1);
            assertThat(gateway.closes.get()).isZero();
            assertThat(control.activeScannerCount()).isZero();
        }
    }

    private TableInfo createTable(String name) throws Exception {
        TablePath path = TablePath.of("fluss", name);
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("payload", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        admin.createTable(
                        path,
                        TableDescriptor.builder().schema(schema).distributedBy(1).build(),
                        false)
                .get(30, TimeUnit.SECONDS);
        TableInfo info = admin.getTableInfo(path).get(30, TimeUnit.SECONDS);
        CLUSTER.waitUntilTableReady(info.getTableId());
        try (Table table = connection.getTable(path)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            List<CompletableFuture<?>> writes = new ArrayList<>();
            for (int i = 0; i < ROW_COUNT; i++) {
                writes.add(writer.upsert(GenericRow.of(i, BinaryString.fromString(PAYLOAD))));
            }
            CompletableFuture.allOf(writes.toArray(new CompletableFuture[0]))
                    .get(30, TimeUnit.SECONDS);
        }
        control = TestingKvScanControl.forBucket(CLUSTER, new TableBucket(info.getTableId(), 0));
        return info;
    }

    private KvBatchScanner newScanner(TableInfo info, FaultGateway gateway) {
        SchemaGetter schemaGetter = mock(SchemaGetter.class);
        when(schemaGetter.getSchema(info.getSchemaId())).thenReturn(info.getSchema());
        return new KvBatchScanner(
                info,
                new TableBucket(info.getTableId(), 0),
                schemaGetter,
                new ForwardingMetadataUpdater(gateway.proxy),
                4096,
                null);
    }

    private static void assertFirstBatch(KvBatchScanner scanner) throws Exception {
        try (CloseableIterator<InternalRow> rows = scanner.pollBatch(POLL_TIMEOUT)) {
            assertThat(rows).isNotNull();
            int count = 0;
            while (rows.hasNext()) {
                InternalRow row = rows.next();
                assertThat(row.getInt(0)).isBetween(0, ROW_COUNT - 1);
                assertThat(row.getString(1).toString()).isEqualTo(PAYLOAD);
                count++;
            }
            assertThat(count).isBetween(1, ROW_COUNT / 3);
        }
    }

    private static void waitForPrefetch(FaultGateway gateway) throws Exception {
        waitUntil(
                () -> gateway.responses.get() >= 2,
                POLL_TIMEOUT,
                "The server did not complete the prefetched continuation");
    }

    private static Configuration clusterConfig() {
        Configuration configuration = new Configuration();
        configuration.set(ConfigOptions.KV_SCANNER_MAX_BATCH_SIZE, new MemorySize(4096));
        configuration.set(ConfigOptions.KV_SCANNER_TTL, Duration.ofMinutes(10));
        configuration.set(ConfigOptions.KV_SCANNER_EXPIRATION_INTERVAL, Duration.ofDays(1));
        return configuration;
    }

    private enum Failure {
        NONE,
        LOST_OPEN,
        CLOSE,
        CONTINUATION
    }

    private static final class FaultGateway {
        private final AtomicInteger opens = new AtomicInteger();
        private final AtomicInteger closes = new AtomicInteger();
        private final AtomicInteger responses = new AtomicInteger();
        private final TabletServerGateway proxy;

        private FaultGateway(TableInfo info, Failure failure) {
            TabletServerGateway delegate =
                    CLUSTER.newTabletServerClientForNode(
                            CLUSTER.waitAndGetLeader(new TableBucket(info.getTableId(), 0)));
            proxy =
                    (TabletServerGateway)
                            Proxy.newProxyInstance(
                                    TabletServerGateway.class.getClassLoader(),
                                    new Class<?>[] {TabletServerGateway.class},
                                    (ignored, method, args) -> {
                                        if (!method.getName().equals("scanKv")) {
                                            try {
                                                return method.invoke(delegate, args);
                                            } catch (InvocationTargetException e) {
                                                throw e.getCause();
                                            }
                                        }
                                        ScanKvRequest request = (ScanKvRequest) args[0];
                                        boolean close =
                                                request.hasCloseScanner()
                                                        && request.isCloseScanner();
                                        boolean open = request.hasBucketScanReq();
                                        if (close) {
                                            closes.incrementAndGet();
                                            if (failure == Failure.CLOSE) {
                                                CompletableFuture<ScanKvResponse> failed =
                                                        new CompletableFuture<>();
                                                failed.completeExceptionally(
                                                        new IOException("close RPC unavailable"));
                                                return failed;
                                            }
                                        } else if (open) {
                                            opens.incrementAndGet();
                                        }
                                        return delegate.scanKv(request)
                                                .thenApply(
                                                        response -> {
                                                            if (response.hasErrorCode()
                                                                    && response.getErrorCode()
                                                                            != 0) {
                                                                return response;
                                                            }
                                                            if (!close) {
                                                                assertThat(response.hasRecords())
                                                                        .isTrue();
                                                                assertThat(
                                                                                response
                                                                                        .isHasMoreResults())
                                                                        .isTrue();
                                                                responses.incrementAndGet();
                                                                if (open
                                                                        && failure
                                                                                == Failure
                                                                                        .LOST_OPEN) {
                                                                    throw new CompletionException(
                                                                            new IOException(
                                                                                    "lost open response"));
                                                                }
                                                                if (!open
                                                                        && failure
                                                                                == Failure
                                                                                        .CONTINUATION) {
                                                                    throw new CompletionException(
                                                                            new IOException(
                                                                                    "lost continuation response"));
                                                                }
                                                            }
                                                            return response;
                                                        });
                                    });
        }
    }

    private static final class ForwardingMetadataUpdater extends MetadataUpdater {
        private final TabletServerGateway gateway;

        private ForwardingMetadataUpdater(TabletServerGateway gateway) {
            super(null, new Configuration(), Cluster.empty());
            this.gateway = gateway;
        }

        @Override
        public void checkAndUpdateMetadata(TablePath tablePath, TableBucket tableBucket) {}

        @Override
        public int leaderFor(TablePath tablePath, TableBucket tableBucket) {
            return 0;
        }

        @Override
        public TabletServerGateway newTabletServerClientForNode(int serverId) {
            return gateway;
        }
    }
}
