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

package org.apache.fluss.rpc.netty.server;

import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.Histogram;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.groups.GenericMetricGroup;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.rpc.TestingTabletGatewayService;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.messages.MetadataRequest;
import org.apache.fluss.rpc.messages.PrefixLookupRequest;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.rpc.protocol.RequestType;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the synchronous request processor time independently of asynchronous response completion.
 */
class RequestProcessorMetricsTest {

    @Test
    void recordsWorkerTimeByRequestTypeBeforeResponseCompletesAndOnFailure() throws Exception {
        RequestsMetricsTest.RecordingMetricRegistry registry =
                new RequestsMetricsTest.RecordingMetricRegistry();
        MetricGroup serverMetricGroup = new GenericMetricGroup(registry, null, "tabletserver");
        RequestsMetrics requestsMetrics =
                RequestsMetrics.createTabletServerRequestMetrics(serverMetricGroup);
        RequestChannel channel = new RequestChannel(10);
        RequestHandler<?>[] handlers = new RequestHandler<?>[RequestType.values().length - 1];
        handlers[RequestType.FLUSS.id] =
                new RequestHandler<FlussRequest>() {
                    @Override
                    public RequestType requestType() {
                        return RequestType.FLUSS;
                    }

                    @Override
                    public void processRequest(FlussRequest request) {
                        if (request.getApiKey() == ApiKeys.PREFIX_LOOKUP.id) {
                            request.fail(new IllegalStateException("test failure"));
                        }
                    }
                };
        RequestProcessor processor =
                new RequestProcessor(
                        0, channel, new TestingTabletGatewayService(), handlers, requestsMetrics);
        CompletableFuture<ApiMessage> pendingResponse = new CompletableFuture<>();
        CompletableFuture<ApiMessage> failedResponse = new CompletableFuture<>();
        channel.putRequest(
                newRequest(ApiKeys.GET_METADATA, new MetadataRequest(), pendingResponse));
        channel.putRequest(
                newRequest(ApiKeys.PREFIX_LOOKUP, new PrefixLookupRequest(), failedResponse));
        processor.initiateShutdown();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> processorTask = executor.submit(processor);
            processor.getShutdownFuture().get(5, TimeUnit.SECONDS);
            processorTask.get(5, TimeUnit.SECONDS);

            assertThat(pendingResponse).isNotDone();
            assertThat(failedResponse).isCompletedExceptionally();
            assertRecordedTime(registry, "metadata");
            assertRecordedTime(registry, "prefixLookup");
        } finally {
            executor.shutdownNow();
        }
    }

    private static FlussRequest newRequest(
            ApiKeys apiKey, ApiMessage message, CompletableFuture<ApiMessage> response) {
        return new FlussRequest(
                apiKey.id,
                (short) 0,
                1,
                null,
                message,
                Unpooled.EMPTY_BUFFER,
                "FLUSS",
                true,
                null,
                null,
                response);
    }

    private static void assertRecordedTime(
            RequestsMetricsTest.RecordingMetricRegistry registry, String requestType) {
        Histogram histogram =
                (Histogram)
                        registry.metrics(MetricNames.REQUEST_SYNC_PROCESS_TIME_US, requestType)
                                .get(0)
                                .metric;
        Counter sum =
                (Counter)
                        registry.metrics(MetricNames.REQUEST_SYNC_PROCESS_TIME_US_SUM, requestType)
                                .get(0)
                                .metric;
        assertThat(histogram.getCount()).isEqualTo(1);
        assertThat(sum.getCount()).isPositive();
    }
}
