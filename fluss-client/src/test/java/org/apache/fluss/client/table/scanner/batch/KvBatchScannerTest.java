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

package org.apache.fluss.client.table.scanner.batch;

import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidScanRequestException;
import org.apache.fluss.exception.NotLeaderOrFollowerException;
import org.apache.fluss.exception.ScannerExpiredException;
import org.apache.fluss.exception.TooManyScannersException;
import org.apache.fluss.exception.UnknownScannerIdException;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.rpc.TestingTabletGatewayService;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.ScanKvRequest;
import org.apache.fluss.rpc.messages.ScanKvResponse;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO_PK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Protocol-level unit tests for {@link KvBatchScanner} against a recording fake gateway. */
class KvBatchScannerTest {

    private static final TableBucket BUCKET_0 = new TableBucket(DATA1_TABLE_ID_PK, 0);
    private static final byte[] SCANNER_ID = new byte[] {1, 2, 3, 4};
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(5);
    private static final SchemaGetter SCHEMA_GETTER =
            new TestingSchemaGetter((short) 1, DATA1_SCHEMA_PK);

    @Test
    void firstPollOpensScannerWithCallSeqIdZero() throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        gateway.enqueue(emptyTerminalResponse(SCANNER_ID));

        try (KvBatchScanner scanner = newScanner(gateway)) {
            assertThat(scanner.pollBatch(POLL_TIMEOUT)).isNull();

            ScanKvRequest open = gateway.requests.get(0);
            assertThat(open.hasBucketScanReq()).isTrue();
            assertThat(open.hasScannerId()).isFalse();
            assertThat(open.hasCallSeqId()).isTrue(); // open request also carries callSeqId
            assertThat(open.getCallSeqId()).isEqualTo(0); // open request with 0 seq_id
            assertThat(open.getBucketScanReq().getTableId()).isEqualTo(DATA1_TABLE_ID_PK);
            assertThat(open.getBucketScanReq().getBucketId()).isEqualTo(0);
        }
    }

    @Test
    void continuationsUsePreIncrementedCallSeqIdStartingAtOne() throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        gateway.enqueue(emptyContinuationResponse(SCANNER_ID));
        gateway.enqueue(emptyContinuationResponse(SCANNER_ID));
        gateway.enqueue(emptyContinuationResponse(SCANNER_ID));
        gateway.enqueue(emptyTerminalResponse(SCANNER_ID));

        try (KvBatchScanner scanner = newScanner(gateway)) {
            for (int i = 0; i < 4; i++) {
                scanner.pollBatch(POLL_TIMEOUT);
            }
        }

        assertThat(gateway.requests).hasSize(4);
        assertThat(gateway.requests.get(0).getCallSeqId()).isEqualTo(0);
        assertThat(gateway.requests.get(1).getCallSeqId()).isEqualTo(1);
        assertThat(gateway.requests.get(2).getCallSeqId()).isEqualTo(2);
        assertThat(gateway.requests.get(3).getCallSeqId()).isEqualTo(3);
    }

    @Test
    void continuationsCarryScannerIdFromFirstResponse() throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        gateway.enqueue(emptyContinuationResponse(SCANNER_ID));
        gateway.enqueue(emptyTerminalResponse(SCANNER_ID));

        try (KvBatchScanner scanner = newScanner(gateway)) {
            scanner.pollBatch(POLL_TIMEOUT);
            scanner.pollBatch(POLL_TIMEOUT);
        }

        assertThat(gateway.requests.get(1).hasScannerId()).isTrue();
        assertThat(gateway.requests.get(1).getScannerId()).isEqualTo(SCANNER_ID);
    }

    @Test
    void emptyBucketReturnsNullOnFirstPoll() throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        gateway.enqueue(emptyTerminalResponse(SCANNER_ID));

        try (KvBatchScanner scanner = newScanner(gateway)) {
            assertThat(scanner.pollBatch(POLL_TIMEOUT)).isNull();
            assertThat(scanner.pollBatch(POLL_TIMEOUT)).isNull();
        }

        assertThat(gateway.requests).hasSize(1);
    }

    @Test
    void pipelinesNextRequestImmediatelyAfterResponse() throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        gateway.enqueue(emptyContinuationResponse(SCANNER_ID));
        gateway.enqueue(emptyTerminalResponse(SCANNER_ID));

        try (KvBatchScanner scanner = newScanner(gateway)) {
            scanner.pollBatch(POLL_TIMEOUT);
            assertThat(gateway.requests).hasSize(2);
        }
    }

    // -------------------------------------------------------------------------
    // close() discipline
    // -------------------------------------------------------------------------

    @Test
    void closeIsIdempotent() throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        gateway.enqueue(emptyContinuationResponse(SCANNER_ID));
        gateway.enqueue(emptyTerminalResponse(SCANNER_ID));

        KvBatchScanner scanner = newScanner(gateway);
        scanner.pollBatch(POLL_TIMEOUT);
        scanner.close();
        int after1 = gateway.requests.size();
        scanner.close();
        scanner.close();
        assertThat(gateway.requests).hasSize(after1);
        assertThat(scanner.isClosed()).isTrue();
    }

    @Test
    void closeAfterDrainedDoesNotSendCloseScanner() throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        gateway.enqueue(emptyTerminalResponse(SCANNER_ID));

        try (KvBatchScanner scanner = newScanner(gateway)) {
            assertThat(scanner.pollBatch(POLL_TIMEOUT)).isNull();
            assertThat(scanner.isDrained()).isTrue();
        }

        assertThat(gateway.requests).hasSize(1);
        assertThat(gateway.requests.stream().anyMatch(KvBatchScannerTest::isCloseRequest))
                .isFalse();
    }

    @Test
    void closeMidStreamSendsCloseScannerWithScannerId() throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        gateway.enqueue(emptyContinuationResponse(SCANNER_ID));
        gateway.enqueue(emptyContinuationResponse(SCANNER_ID));

        KvBatchScanner scanner = newScanner(gateway);
        scanner.pollBatch(POLL_TIMEOUT);
        scanner.close();

        // open + pipelined continuation + close_scanner
        assertThat(gateway.requests).hasSize(3);
        ScanKvRequest closeReq = gateway.requests.get(2);
        assertThat(isCloseRequest(closeReq)).isTrue();
        assertThat(closeReq.getScannerId()).isEqualTo(SCANNER_ID);
    }

    // -------------------------------------------------------------------------
    // TOO_MANY_SCANNERS retry
    // -------------------------------------------------------------------------

    @Test
    void tooManyScannersOnOpenRetriesUpToLimitThenSucceeds() throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        gateway.enqueue(errorResponse(Errors.TOO_MANY_SCANNERS));
        gateway.enqueue(errorResponse(Errors.TOO_MANY_SCANNERS));
        gateway.enqueue(errorResponse(Errors.TOO_MANY_SCANNERS));
        gateway.enqueue(emptyTerminalResponse(SCANNER_ID));

        try (KvBatchScanner scanner = newScanner(gateway)) {
            assertThat(scanner.pollBatch(POLL_TIMEOUT))
                    .isNotNull()
                    .satisfies(it -> assertThat(it.hasNext()).isFalse());
            assertThat(scanner.pollBatch(POLL_TIMEOUT))
                    .isNotNull()
                    .satisfies(it -> assertThat(it.hasNext()).isFalse());
            assertThat(scanner.pollBatch(POLL_TIMEOUT))
                    .isNotNull()
                    .satisfies(it -> assertThat(it.hasNext()).isFalse());
            assertThat(scanner.pollBatch(POLL_TIMEOUT)).isNull();
            assertThat(scanner.openRetries()).isEqualTo(3);
        }
    }

    @Test
    void tooManyScannersOnOpenExhaustsRetriesAndFails() throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        gateway.enqueue(errorResponse(Errors.TOO_MANY_SCANNERS));
        gateway.enqueue(errorResponse(Errors.TOO_MANY_SCANNERS));
        gateway.enqueue(errorResponse(Errors.TOO_MANY_SCANNERS));
        gateway.enqueue(errorResponse(Errors.TOO_MANY_SCANNERS));

        KvBatchScanner scanner = newScanner(gateway);
        scanner.pollBatch(POLL_TIMEOUT);
        scanner.pollBatch(POLL_TIMEOUT);
        scanner.pollBatch(POLL_TIMEOUT);

        assertThatThrownBy(() -> scanner.pollBatch(POLL_TIMEOUT))
                .isInstanceOf(IOException.class)
                .hasCauseInstanceOf(TooManyScannersException.class);
        assertThat(scanner.isClosed()).isTrue();
    }

    // -------------------------------------------------------------------------
    // Other terminal errors
    // -------------------------------------------------------------------------

    @Test
    void notLeaderOrFollowerRefreshesMetadataAndFails() throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        gateway.enqueue(errorResponse(Errors.NOT_LEADER_OR_FOLLOWER));

        TestMetadataUpdater meta = new TestMetadataUpdater(gateway);
        try (KvBatchScanner scanner =
                new KvBatchScanner(
                        DATA1_TABLE_INFO_PK, BUCKET_0, SCHEMA_GETTER, meta, 4096, null)) {
            assertThatThrownBy(() -> scanner.pollBatch(POLL_TIMEOUT))
                    .isInstanceOf(IOException.class)
                    .hasCauseInstanceOf(NotLeaderOrFollowerException.class);

            // open + post-error refresh
            assertThat(meta.metadataRefreshes.get()).isEqualTo(2);
            assertThat(scanner.isClosed()).isTrue();
        }
    }

    @Test
    void scannerExpiredIsTerminalAndDoesNotSendCloseScanner() throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        gateway.enqueue(emptyContinuationResponse(SCANNER_ID));
        gateway.enqueue(errorResponse(Errors.SCANNER_EXPIRED));

        try (KvBatchScanner scanner = newScanner(gateway)) {
            scanner.pollBatch(POLL_TIMEOUT);
            assertThatThrownBy(() -> scanner.pollBatch(POLL_TIMEOUT))
                    .isInstanceOf(IOException.class)
                    .hasCauseInstanceOf(ScannerExpiredException.class);
        }

        assertThat(gateway.requests.stream().anyMatch(KvBatchScannerTest::isCloseRequest))
                .isFalse();
    }

    @Test
    void unknownScannerIdIsTerminalAndDoesNotSendCloseScanner() throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        gateway.enqueue(emptyContinuationResponse(SCANNER_ID));
        gateway.enqueue(errorResponse(Errors.UNKNOWN_SCANNER_ID));

        try (KvBatchScanner scanner = newScanner(gateway)) {
            scanner.pollBatch(POLL_TIMEOUT);
            assertThatThrownBy(() -> scanner.pollBatch(POLL_TIMEOUT))
                    .isInstanceOf(IOException.class)
                    .hasCauseInstanceOf(UnknownScannerIdException.class);
        }

        assertThat(gateway.requests.stream().anyMatch(KvBatchScannerTest::isCloseRequest))
                .isFalse();
    }

    @Test
    void invalidScanRequestIsTerminalAndSendsCloseScanner() throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        gateway.enqueue(emptyContinuationResponse(SCANNER_ID));
        gateway.enqueue(errorResponse(Errors.INVALID_SCAN_REQUEST));

        try (KvBatchScanner scanner = newScanner(gateway)) {
            scanner.pollBatch(POLL_TIMEOUT);
            assertThatThrownBy(() -> scanner.pollBatch(POLL_TIMEOUT))
                    .isInstanceOf(IOException.class)
                    .hasCauseInstanceOf(InvalidScanRequestException.class);
        }

        assertThat(gateway.requests.stream().anyMatch(KvBatchScannerTest::isCloseRequest)).isTrue();
    }

    // -------------------------------------------------------------------------
    // Timeout
    // -------------------------------------------------------------------------

    @Test
    void timeoutReturnsEmptyIteratorAndKeepsFutureInFlight() throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        gateway.enqueue(neverCompleting());

        KvBatchScanner scanner = newScanner(gateway);
        CloseableIterator<InternalRow> first = scanner.pollBatch(Duration.ofMillis(50));
        assertThat(first).isNotNull();
        assertThat(first.hasNext()).isFalse();

        assertThat(gateway.requests).hasSize(1);
        scanner.close();
    }

    // -------------------------------------------------------------------------
    // Test helpers
    // -------------------------------------------------------------------------

    private KvBatchScanner newScanner(RecordingGateway gateway) {
        return new KvBatchScanner(
                DATA1_TABLE_INFO_PK,
                BUCKET_0,
                SCHEMA_GETTER,
                new TestMetadataUpdater(gateway),
                4096,
                null);
    }

    private static ScanKvResponse emptyContinuationResponse(byte[] scannerId) {
        return new ScanKvResponse().setScannerId(scannerId).setHasMoreResults(true);
    }

    private static ScanKvResponse emptyTerminalResponse(byte[] scannerId) {
        return new ScanKvResponse()
                .setScannerId(scannerId)
                .setHasMoreResults(false)
                .setLogOffset(0L);
    }

    private static ScanKvResponse errorResponse(Errors error) {
        return new ScanKvResponse()
                .setErrorCode(error.code())
                .setErrorMessage(error.exception().getMessage());
    }

    private static CompletableFuture<ScanKvResponse> neverCompleting() {
        return new CompletableFuture<>();
    }

    private static boolean isCloseRequest(ScanKvRequest req) {
        return req.hasCloseScanner() && req.isCloseScanner();
    }

    private static final class RecordingGateway extends TestingTabletGatewayService {
        final List<ScanKvRequest> requests = new ArrayList<>();
        private final Queue<CompletableFuture<ScanKvResponse>> queued = new LinkedList<>();

        void enqueue(ScanKvResponse response) {
            queued.add(CompletableFuture.completedFuture(response));
        }

        void enqueue(CompletableFuture<ScanKvResponse> future) {
            queued.add(future);
        }

        @Override
        public CompletableFuture<ScanKvResponse> scanKv(ScanKvRequest request) {
            requests.add(request);
            if (request.hasCloseScanner() && request.isCloseScanner()) {
                return CompletableFuture.completedFuture(
                        new ScanKvResponse().setHasMoreResults(false));
            }
            CompletableFuture<ScanKvResponse> next = queued.poll();
            if (next == null) {
                CompletableFuture<ScanKvResponse> failed = new CompletableFuture<>();
                failed.completeExceptionally(
                        new AssertionError(
                                "RecordingGateway received an unexpected request (no response queued): "
                                        + request));
                return failed;
            }
            return next;
        }
    }

    private static final class TestMetadataUpdater extends MetadataUpdater {
        private final TabletServerGateway gateway;
        final AtomicInteger metadataRefreshes = new AtomicInteger();

        TestMetadataUpdater(TabletServerGateway gateway) {
            super(null, new Configuration(), Cluster.empty());
            this.gateway = gateway;
        }

        @Override
        public void checkAndUpdateMetadata(TablePath tablePath, TableBucket tableBucket) {
            metadataRefreshes.incrementAndGet();
        }

        @Override
        public int leaderFor(TablePath tablePath, TableBucket tableBucket) {
            return 0;
        }

        @Override
        public @Nullable TabletServerGateway newTabletServerClientForNode(int serverId) {
            return gateway;
        }
    }
}
