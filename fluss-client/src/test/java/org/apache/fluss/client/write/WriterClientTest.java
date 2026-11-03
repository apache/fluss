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

import org.apache.fluss.client.metadata.TestingMetadataUpdater;
import org.apache.fluss.client.metrics.TestingWriterMetricGroup;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.entity.ProduceLogResultForBucket;
import org.apache.fluss.rpc.messages.ProduceLogRequest;
import org.apache.fluss.rpc.messages.ProduceLogResponse;
import org.apache.fluss.server.tablet.TestTabletServerGateway;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeProduceLogResponse;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link WriterClient}. */
class WriterClientTest {

    @Test
    void testCloseFromWriteCallbackDoesNotWaitForPendingWrites() throws Exception {
        TestTabletServerGateway gateway =
                new TestTabletServerGateway(false, Collections.emptySet());
        TestingMetadataUpdater metadataUpdater =
                TestingMetadataUpdater.builder(
                                Collections.singletonMap(DATA1_TABLE_PATH, DATA1_TABLE_INFO))
                        .withTabletServerGateway(1, gateway)
                        .withTabletServerGateway(2, gateway)
                        .withTabletServerGateway(3, gateway)
                        .build();
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.CLIENT_WRITER_BATCH_TIMEOUT, Duration.ZERO);
        WriterClient writer =
                new WriterClient(
                        conf, metadataUpdater, TestingWriterMetricGroup.newInstance(), null);
        ExecutorService responseThread = Executors.newSingleThreadExecutor();
        try {
            CompletableFuture<Exception> firstWrite = new CompletableFuture<>();
            CompletableFuture<Void> closedInCallback = new CompletableFuture<>();
            writer.send(
                    appendRecord(1),
                    (tb, leo, e) -> {
                        firstWrite.complete(e);
                        writer.close(Duration.ofMillis(Long.MAX_VALUE));
                        closedInCallback.complete(null);
                    });
            retry(Duration.ofSeconds(20), () -> assertThat(gateway.pendingRequestSize()).isOne());
            CompletableFuture<Exception> secondWrite = new CompletableFuture<>();
            AtomicInteger secondCallbacks = new AtomicInteger();
            writer.send(
                    appendRecord(2),
                    (tb, leo, e) -> {
                        secondCallbacks.incrementAndGet();
                        secondWrite.complete(e);
                    });
            retry(
                    Duration.ofSeconds(20),
                    () -> assertThat(gateway.pendingRequestSize()).isEqualTo(2));

            ProduceLogResponse firstResponse = successResponse(gateway, 0);
            ProduceLogResponse secondResponse = successResponse(gateway, 1);
            responseThread.submit(() -> gateway.response(0, firstResponse));

            assertThat(closedInCallback).succeedsWithin(Duration.ofSeconds(20));
            assertThat(firstWrite).isCompletedWithValue(null);
            assertThat(secondWrite.get(20, TimeUnit.SECONDS))
                    .isInstanceOf(FlussRuntimeException.class)
                    .hasMessageContaining("force closed");

            responseThread
                    .submit(() -> gateway.response(0, secondResponse))
                    .get(20, TimeUnit.SECONDS);
            assertThat(secondCallbacks).hasValue(1);
            assertThat(responseThread.submit(WriteBatch::isInCallback).get(20, TimeUnit.SECONDS))
                    .isFalse();
        } finally {
            writer.close(Duration.ZERO);
            responseThread.shutdownNow();
            assertThat(responseThread.awaitTermination(20, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static WriteRecord appendRecord(int id) {
        return WriteRecord.forArrowAppend(
                DATA1_TABLE_INFO, PhysicalTablePath.of(DATA1_TABLE_PATH), row(id, "a"), null);
    }

    private static ProduceLogResponse successResponse(TestTabletServerGateway gateway, int index) {
        ProduceLogRequest request = (ProduceLogRequest) gateway.getRequest(index);
        TableBucket bucket =
                new TableBucket(request.getTableId(), request.getBucketsReqAt(0).getBucketId());
        return makeProduceLogResponse(
                Collections.singletonList(new ProduceLogResultForBucket(bucket, 0, 1)));
    }
}
