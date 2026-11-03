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
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.rpc.TestingTabletGatewayService;
import org.apache.fluss.rpc.messages.LimitScanRequest;
import org.apache.fluss.rpc.messages.LimitScanResponse;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO_PK;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests response buffer ownership when a limit scan is closed. */
class LimitBatchScannerTest {

    @Test
    void testCloseReleasesCompletedUnconsumedResponse() throws Exception {
        ByteBuf buffer = Unpooled.buffer();
        LimitScanResponse response = parsedResponse(buffer);
        try (LimitBatchScanner scanner = newScanner(CompletableFuture.completedFuture(response))) {
            scanner.close();
            scanner.close();
            assertThat(buffer.refCnt()).isZero();
            assertThat(scanner.pollBatch(Duration.ZERO)).isNull();
        } finally {
            if (buffer.refCnt() > 0) {
                buffer.release();
            }
        }
    }

    @Test
    void testCloseReleasesLateResponse() throws Exception {
        CompletableFuture<LimitScanResponse> future = new CompletableFuture<>();
        ByteBuf buffer = Unpooled.buffer();
        LimitScanResponse response = parsedResponse(buffer);
        try (LimitBatchScanner scanner = newScanner(future)) {
            scanner.close();
            assertThat(future.complete(response)).isTrue();
            assertThat(buffer.refCnt()).isZero();
        } finally {
            if (buffer.refCnt() > 0) {
                buffer.release();
            }
        }
    }

    @Test
    void testCloseAfterConsumptionDoesNotReleaseAgain() throws Exception {
        ByteBuf buffer = Unpooled.buffer();
        LimitScanResponse response = parsedResponse(buffer);
        // Keep an independent owner to detect an accidental second release.
        buffer.retain();
        try (LimitBatchScanner scanner = newScanner(CompletableFuture.completedFuture(response))) {
            scanner.pollBatch(Duration.ofSeconds(1)).close();
            assertThat(buffer.refCnt()).isOne();
            scanner.close();
            assertThat(buffer.refCnt()).isOne();
        } finally {
            if (buffer.refCnt() > 0) {
                buffer.release();
            }
        }
    }

    @Test
    void testCloseUnblocksPendingPollAndReleasesLateResponse() throws Exception {
        CompletableFuture<LimitScanResponse> future = new CompletableFuture<>();
        CompletableFuture<CloseableIterator<InternalRow>> polled = new CompletableFuture<>();
        ByteBuf buffer = Unpooled.buffer();
        LimitScanResponse response = parsedResponse(buffer);
        try (LimitBatchScanner scanner = newScanner(future)) {
            Thread pollingThread =
                    new Thread(
                            () -> {
                                try {
                                    polled.complete(scanner.pollBatch(Duration.ofDays(1)));
                                } catch (Throwable error) {
                                    polled.completeExceptionally(error);
                                }
                            });
            pollingThread.start();
            try {
                retry(
                        Duration.ofSeconds(5),
                        () ->
                                assertThat(pollingThread.getState())
                                        .isEqualTo(Thread.State.TIMED_WAITING));
                scanner.close();
                assertThat(polled.get(5, TimeUnit.SECONDS)).isNull();
                assertThat(future.complete(response)).isTrue();
                assertThat(buffer.refCnt()).isZero();
            } finally {
                pollingThread.interrupt();
                pollingThread.join(5000);
            }
        } finally {
            if (buffer.refCnt() > 0) {
                buffer.release();
            }
        }
    }

    private static LimitScanResponse parsedResponse(ByteBuf buffer) {
        new LimitScanResponse().writeTo(buffer);
        LimitScanResponse response = new LimitScanResponse();
        response.parseFrom(buffer, buffer.readableBytes());
        return response;
    }

    private static LimitBatchScanner newScanner(CompletableFuture<LimitScanResponse> future) {
        TestingTabletGatewayService gateway =
                new TestingTabletGatewayService() {
                    @Override
                    public CompletableFuture<LimitScanResponse> limitScan(
                            LimitScanRequest request) {
                        return future;
                    }
                };
        MetadataUpdater updater =
                new MetadataUpdater(null, new Configuration(), Cluster.empty()) {
                    @Override
                    public void checkAndUpdateMetadata(TablePath tablePath, TableBucket bucket) {}

                    @Override
                    public int leaderFor(TablePath tablePath, TableBucket bucket) {
                        return 0;
                    }

                    @Override
                    public TestingTabletGatewayService newTabletServerClientForNode(int serverId) {
                        return gateway;
                    }
                };
        return new LimitBatchScanner(
                DATA1_TABLE_INFO_PK,
                new TableBucket(DATA1_TABLE_ID_PK, 0),
                new TestingSchemaGetter((short) 1, DATA1_SCHEMA_PK),
                updater,
                null,
                1);
    }
}
