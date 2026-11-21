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

import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metrics.Meter;
import org.apache.fluss.metrics.Metric;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.groups.AbstractMetricGroup;
import org.apache.fluss.metrics.groups.GenericMetricGroup;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.metrics.util.NOPMetricsGroup;
import org.apache.fluss.record.FileLogRecords;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.record.bytesview.MemorySegmentBytesView;
import org.apache.fluss.rpc.TestingTabletGatewayService;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.messages.ApiVersionsRequest;
import org.apache.fluss.rpc.messages.ApiVersionsResponse;
import org.apache.fluss.rpc.messages.FetchLogRequest;
import org.apache.fluss.rpc.messages.FetchLogResponse;
import org.apache.fluss.rpc.messages.GetTableStatsRequest;
import org.apache.fluss.rpc.messages.GetTableStatsResponse;
import org.apache.fluss.rpc.messages.LimitScanRequest;
import org.apache.fluss.rpc.messages.LimitScanResponse;
import org.apache.fluss.rpc.messages.ListOffsetsRequest;
import org.apache.fluss.rpc.messages.ListOffsetsResponse;
import org.apache.fluss.rpc.messages.LookupRequest;
import org.apache.fluss.rpc.messages.LookupResponse;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrRequest;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrResponse;
import org.apache.fluss.rpc.messages.PbApiVersion;
import org.apache.fluss.rpc.messages.PbFetchLogRespForTable;
import org.apache.fluss.rpc.messages.PbLookupReqForBucket;
import org.apache.fluss.rpc.messages.PbLookupRespForBucket;
import org.apache.fluss.rpc.messages.PbNotifyLeaderAndIsrRespForBucket;
import org.apache.fluss.rpc.messages.PbProduceLogReqForBucket;
import org.apache.fluss.rpc.messages.PbPutKvReqForBucket;
import org.apache.fluss.rpc.messages.PbStopReplicaRespForBucket;
import org.apache.fluss.rpc.messages.PbValue;
import org.apache.fluss.rpc.messages.PrefixLookupRequest;
import org.apache.fluss.rpc.messages.ProduceLogRequest;
import org.apache.fluss.rpc.messages.ProduceLogResponse;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.rpc.messages.ScanKvRequest;
import org.apache.fluss.rpc.messages.ScanKvResponse;
import org.apache.fluss.rpc.messages.StopReplicaRequest;
import org.apache.fluss.rpc.messages.StopReplicaResponse;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.rpc.protocol.ApiManager;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.rpc.protocol.MessageCodec;
import org.apache.fluss.rpc.protocol.RequestType;
import org.apache.fluss.rpc.protocol.ResponseType;
import org.apache.fluss.security.auth.PlainTextAuthenticationPlugin;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.fluss.shaded.netty4.io.netty.channel.Channel;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelId;
import org.apache.fluss.shaded.netty4.io.netty.channel.DefaultChannelId;
import org.apache.fluss.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.fluss.shaded.netty4.io.netty.util.concurrent.DefaultEventExecutor;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Test for {@link NettyServerHandler}. */
final class NettyServerHandlerTest {

    private static final InetSocketAddress REMOTE_ADDRESS =
            new InetSocketAddress("127.0.0.1", 9092);

    private NettyServerHandler serverHandler;
    private TestingRequestChannel requestChannel;
    private ChannelHandlerContext ctx;

    @Test
    void testFailedRequestMarksAggregateAndErrorMetrics() {
        RequestsMetricsTest.RecordingMetricRegistry metricRegistry =
                new RequestsMetricsTest.RecordingMetricRegistry();
        HandlerFixture fixture = createTabletServerHandler(metricRegistry);
        try {
            writeRequest(fixture, ApiKeys.LOOKUP, new LookupRequest().setTableId(1), 1001);
            FlussRequest request = (FlussRequest) fixture.requestChannel.getAndRemoveRequest(0);
            request.fail(new TableNotExistException("table does not exist"));
            fixture.channel.runPendingTasks();

            assertResponse(fixture.channel, ResponseType.ERROR_RESPONSE, 1001);
            assertThat(metricRegistry.metrics(MetricNames.ERRORS_RATE, "lookup"))
                    .hasSize(2)
                    .allSatisfy(
                            registered ->
                                    assertThat(((Meter) registered.metric).getCount()).isEqualTo(1))
                    .anySatisfy(
                            registered ->
                                    assertThat(registered.group.getAllVariables())
                                            .doesNotContainKey("error"))
                    .anySatisfy(
                            registered ->
                                    assertThat(registered.group.getAllVariables())
                                            .containsEntry("error", Errors.TABLE_NOT_EXIST.name()));
        } finally {
            fixture.channel.finishAndReleaseAll();
        }
    }

    @Test
    void testInactiveLazyRequestIsRejectedBeforeQueuing() throws Exception {
        RecordingRequestChannel channel = new RecordingRequestChannel();
        NettyServerHandler handler = newServerHandler(channel);
        TestEmbeddedChannel embeddedChannel = new TestEmbeddedChannel(handler);
        ByteBuf requestBuffer = encodeProduceLogRequest();

        try {
            ChannelHandlerContext context = embeddedChannel.pipeline().context(handler);
            handler.exceptionCaught(context, new RuntimeException("close connection"));
            handler.channelRead(context, requestBuffer);

            assertThat(channel.getPutRequestInvocations()).isZero();
            assertThat(channel.requestsCount()).isZero();
            assertThat(requestBuffer.refCnt()).isZero();
        } finally {
            embeddedChannel.finishAndReleaseAll();
        }
    }

    @Test
    void testIdleWorkerTakesWholeConnectionAndItsPressure() {
        ConnectionRequestScheduler scheduler = new ConnectionRequestScheduler(false, true);
        ConnectionRequestScheduler.Queue source = scheduler.new Queue(100);
        ConnectionRequestScheduler.Queue idle = scheduler.new Queue(100);
        TestEmbeddedChannel running = new TestEmbeddedChannel(newServerHandler(source));
        TestEmbeddedChannel movable = new TestEmbeddedChannel(newServerHandler(source));
        List<Integer> calls = new ArrayList<>();
        FlussRequestHandler handler =
                new FlussRequestHandler(
                        new TestingTabletGatewayService() {
                            @Override
                            public CompletableFuture<PutKvResponse> putKv(PutKvRequest request) {
                                calls.add(request.getBucketsReqAt(0).getBucketId());
                                return CompletableFuture.completedFuture(new PutKvResponse());
                            }
                        });
        try {
            running.writeInbound(encodeBucketPut(1, 1));
            ConnectionRequestScheduler.Work held =
                    (ConnectionRequestScheduler.Work) source.pollRequest(0);
            movable.writeInbound(encodeBucketPut(2, 2));
            movable.writeInbound(encodeBucketPut(3, 3));
            ConnectionRequestScheduler.Work taken =
                    (ConnectionRequestScheduler.Work) idle.pollRequest(0);
            assertThat(taken).isNotNull();
            assertThat(source.requestsCount()).isOne();
            assertThat(idle.requestsCount()).isEqualTo(2);
            movable.writeInbound(encodeBucketPut(4, 4));
            assertThat(idle.requestsCount()).isEqualTo(3);
            taken.execute(handler);
            ((ConnectionRequestScheduler.Work) idle.pollRequest(0)).execute(handler);
            ((ConnectionRequestScheduler.Work) idle.pollRequest(0)).execute(handler);
            held.execute(handler);
            assertThat(calls).containsExactly(2, 3, 4, 1);
            assertThat(source.requestsCount() + idle.requestsCount()).isZero();
        } finally {
            running.finishAndReleaseAll();
            movable.finishAndReleaseAll();
        }
    }

    @Test
    void testTransferUsesDestinationPressureAfterQueuedPause() throws Exception {
        ConnectionRequestScheduler scheduler = new ConnectionRequestScheduler(false, true);
        ConnectionRequestScheduler.Queue source = scheduler.new Queue(3);
        ConnectionRequestScheduler.Queue target = scheduler.new Queue(3);
        NettyServerHandler blockedHandler = newServerHandler(source);
        NettyServerHandler movedHandler = newServerHandler(source);
        TestEmbeddedChannel blocked = new TestEmbeddedChannel(blockedHandler);
        TestEmbeddedChannel moved = new TestEmbeddedChannel(movedHandler);
        FlussRequestHandler handler =
                new FlussRequestHandler(
                        new TestingTabletGatewayService() {
                            @Override
                            public CompletableFuture<PutKvResponse> putKv(PutKvRequest request) {
                                return CompletableFuture.completedFuture(new PutKvResponse());
                            }
                        });
        try {
            ChannelHandlerContext blockedContext = blocked.pipeline().context(blockedHandler);
            ChannelHandlerContext movedContext = moved.pipeline().context(movedHandler);
            blockedHandler.channelRead(blockedContext, encodeBucketPut(1, 1));
            ConnectionRequestScheduler.Work held =
                    (ConnectionRequestScheduler.Work) source.pollRequest(0);
            blockedHandler.channelRead(blockedContext, encodeBucketPut(2, 1));
            blockedHandler.channelRead(blockedContext, encodeBucketPut(3, 1));
            assertThat(source.requestsCount()).isEqualTo(3);
            blocked.runPendingTasks();
            assertThat(blocked.config().isAutoRead()).isFalse();
            movedHandler.channelRead(movedContext, encodeBucketPut(4, 2));
            assertThat(source.requestsCount()).isEqualTo(3);
            assertThat(target.requestsCount()).isOne();
            moved.runPendingTasks();
            assertThat(moved.config().isAutoRead()).isTrue();
            ((ConnectionRequestScheduler.Work) target.pollRequest(0)).execute(handler);
            held.execute(handler);
            ((ConnectionRequestScheduler.Work) source.pollRequest(0)).execute(handler);
            ((ConnectionRequestScheduler.Work) source.pollRequest(0)).execute(handler);
            blocked.runPendingTasks();
            assertThat(blocked.config().isAutoRead()).isTrue();
        } finally {
            blocked.finishAndReleaseAll();
            moved.finishAndReleaseAll();
        }
    }

    @Test
    void testConcurrentBucketReadsDoNotLetLaterReadsPassAWriter() throws Exception {
        ConnectionRequestScheduler scheduler = new ConnectionRequestScheduler(true);
        ConnectionRequestScheduler.Queue first = scheduler.new Queue(100);
        ConnectionRequestScheduler.Queue second = scheduler.new Queue(100);
        ConnectionRequestScheduler.Queue writer = scheduler.new Queue(100);
        ConnectionRequestScheduler.Queue late = scheduler.new Queue(100);
        TestEmbeddedChannel firstChannel = new TestEmbeddedChannel(newServerHandler(first));
        TestEmbeddedChannel secondChannel = new TestEmbeddedChannel(newServerHandler(second));
        TestEmbeddedChannel writerChannel = new TestEmbeddedChannel(newServerHandler(writer));
        TestEmbeddedChannel lateChannel = new TestEmbeddedChannel(newServerHandler(late));
        CountDownLatch entered = new CountDownLatch(2);
        CompletableFuture<Void> release = new CompletableFuture<>();
        AtomicInteger reads = new AtomicInteger();
        AtomicInteger writes = new AtomicInteger();
        FlussRequestHandler handler =
                new FlussRequestHandler(
                        new TestingTabletGatewayService() {
                            @Override
                            public BucketRequestExecution prepareBucketRequest(ApiMessage message) {
                                boolean read = message instanceof LookupRequest;
                                boolean early =
                                        read && ((LookupRequest) message).getTableId() == 1001;
                                return new BucketRequestExecution() {
                                    @Override
                                    public List<TableBucket> buckets() {
                                        return Collections.singletonList(new TableBucket(1001, 1));
                                    }

                                    @Override
                                    public boolean isRead() {
                                        return read;
                                    }

                                    @Override
                                    public void execute(TableBucket bucket) {
                                        if (early) {
                                            entered.countDown();
                                            release.join();
                                        }
                                        if (read) {
                                            reads.incrementAndGet();
                                        } else {
                                            writes.incrementAndGet();
                                        }
                                    }

                                    @Override
                                    public void detachInputs() {}

                                    @Override
                                    public boolean hasInput(TableBucket bucket) {
                                        return true;
                                    }

                                    @Override
                                    public CompletableFuture<ApiMessage> finish() {
                                        return CompletableFuture.completedFuture(
                                                read ? new LookupResponse() : new PutKvResponse());
                                    }
                                };
                            }
                        });
        ExecutorService threads = Executors.newFixedThreadPool(2);
        try {
            firstChannel.writeInbound(encodeBucketLookup(1, 1001));
            secondChannel.writeInbound(encodeBucketLookup(2, 1001));
            ConnectionRequestScheduler.Work firstRead =
                    (ConnectionRequestScheduler.Work) first.pollRequest(0);
            ConnectionRequestScheduler.Work secondRead =
                    (ConnectionRequestScheduler.Work) second.pollRequest(0);
            Future<?> a = threads.submit(() -> firstRead.execute(handler));
            Future<?> b = threads.submit(() -> secondRead.execute(handler));
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            writerChannel.writeInbound(encodeBucketPut(3, 1));
            ((ConnectionRequestScheduler.Work) writer.pollRequest(0)).execute(handler);
            lateChannel.writeInbound(encodeBucketLookup(4, 1002));
            ((ConnectionRequestScheduler.Work) late.pollRequest(0)).execute(handler);
            assertThat(writes.get()).isZero();
            assertThat(late.pollRequest(0)).isNull();
            release.complete(null);
            a.get(5, TimeUnit.SECONDS);
            b.get(5, TimeUnit.SECONDS);
            assertThat(reads.get()).isEqualTo(2);
            assertThat(late.pollRequest(0)).isNull();
            ((ConnectionRequestScheduler.Work) writer.pollRequest(0)).execute(handler);
            ((ConnectionRequestScheduler.Work) late.pollRequest(0)).execute(handler);
            assertThat(writes.get()).isOne();
            assertThat(reads.get()).isEqualTo(3);
        } finally {
            release.complete(null);
            threads.shutdownNow();
            firstChannel.finishAndReleaseAll();
            secondChannel.finishAndReleaseAll();
            writerChannel.finishAndReleaseAll();
            lateChannel.finishAndReleaseAll();
        }
    }

    private static ByteBuf encodeBucketLookup(int requestId, long tableId) {
        LookupRequest request = new LookupRequest().setTableId(tableId);
        request.addBucketsReq().setBucketId(1).addKey(new byte[] {1});
        return MessageCodec.encodeRequest(
                ByteBufAllocator.DEFAULT,
                ApiKeys.LOOKUP.id,
                ApiKeys.LOOKUP.highestSupportedVersion,
                requestId,
                request);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testPartialPutReleasesFrameAndResumesOnlyRemainingBucket(boolean handoff)
            throws Exception {
        ConnectionRequestScheduler scheduler = new ConnectionRequestScheduler(true, handoff);
        ConnectionRequestScheduler.Queue hotQueue = scheduler.new Queue(100);
        ConnectionRequestScheduler.Queue otherQueue = scheduler.new Queue(100);
        ConnectionRequestScheduler.Queue resumeQueue =
                handoff ? scheduler.new Queue(100) : otherQueue;
        TestEmbeddedChannel busy = new TestEmbeddedChannel(newServerHandler(otherQueue));
        NettyServerHandler hotInput = newServerHandler(hotQueue);
        NettyServerHandler otherInput = newServerHandler(otherQueue);
        TestEmbeddedChannel hot = new TestEmbeddedChannel(hotInput);
        TestEmbeddedChannel other = new TestEmbeddedChannel(otherInput);
        CompletableFuture<Void> entered = new CompletableFuture<>();
        CompletableFuture<Void> release = new CompletableFuture<>();
        List<Integer> completed = new ArrayList<>();
        AtomicInteger finished = new AtomicInteger();
        TestingTabletGatewayService service =
                new TestingTabletGatewayService() {
                    @Override
                    public BucketRequestExecution prepareBucketRequest(ApiMessage message) {
                        PutKvRequest request = (PutKvRequest) message;
                        Map<TableBucket, ByteBuf> borrowed = new LinkedHashMap<>();
                        for (PbPutKvReqForBucket item : request.getBucketsReqsList()) {
                            borrowed.put(
                                    new TableBucket(1001, item.getBucketId()),
                                    item.getRecordsSlice());
                        }
                        List<TableBucket> buckets = new ArrayList<>(borrowed.keySet());
                        Map<TableBucket, Byte> owned = new HashMap<>();
                        boolean hotRequest = buckets.size() == 1;
                        return new BucketRequestExecution() {
                            @Override
                            public List<TableBucket> buckets() {
                                return buckets;
                            }

                            @Override
                            public void execute(TableBucket bucket) {
                                if (hotRequest) {
                                    entered.complete(null);
                                    release.join();
                                } else {
                                    ByteBuf buffer = borrowed.remove(bucket);
                                    byte value =
                                            buffer == null
                                                    ? owned.remove(bucket)
                                                    : buffer.getByte(0);
                                    completed.add((int) value);
                                }
                            }

                            @Override
                            public void detachInputs() {
                                borrowed.forEach(
                                        (bucket, bytes) -> owned.put(bucket, bytes.getByte(0)));
                                borrowed.clear();
                            }

                            @Override
                            public boolean hasInput(TableBucket bucket) {
                                return owned.containsKey(bucket);
                            }

                            @Override
                            public CompletableFuture<PutKvResponse> finish() {
                                if (!hotRequest) {
                                    finished.incrementAndGet();
                                }
                                return CompletableFuture.completedFuture(new PutKvResponse());
                            }
                        };
                    }
                };
        FlussRequestHandler handler = new FlussRequestHandler(service);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            hot.writeInbound(encodeBucketPut(1, 1));
            ConnectionRequestScheduler.Work hotWork =
                    (ConnectionRequestScheduler.Work) hotQueue.pollRequest(0);
            Future<?> task = executor.submit(() -> hotWork.execute(handler));
            entered.get(5, TimeUnit.SECONDS);
            ByteBuf original = encodeBucketPut(2, 1, 2);
            byte[] bytes = new byte[original.readableBytes()];
            original.readBytes(bytes);
            original.release();
            ByteBuf frame = Unpooled.wrappedBuffer(bytes);
            other.writeInbound(frame);
            ((ConnectionRequestScheduler.Work) otherQueue.pollRequest(0)).execute(handler);
            assertThat(completed).containsExactly(2);
            assertThat(frame.refCnt()).isZero();
            assertThat(otherQueue.requestsCount()).isOne();
            assertThat(finished.get()).isZero();
            Arrays.fill(bytes, (byte) 99);

            other.writeInbound(encodeBucketPut(3, 1, 3));
            ((ConnectionRequestScheduler.Work) otherQueue.pollRequest(0)).execute(handler);
            assertThat(completed).containsExactly(2, 3);
            assertThat(otherQueue.requestsCount()).isEqualTo(2);

            ConnectionRequestScheduler.Work busyWork = null;
            if (handoff) {
                busy.writeInbound(encodeBucketPut(4, 4));
                busyWork = (ConnectionRequestScheduler.Work) otherQueue.pollRequest(0);
            }
            release.complete(null);
            task.get(5, TimeUnit.SECONDS);
            ((ConnectionRequestScheduler.Work) resumeQueue.pollRequest(0)).execute(handler);
            ((ConnectionRequestScheduler.Work) resumeQueue.pollRequest(0)).execute(handler);
            if (handoff) {
                assertThat(otherQueue.requestsCount()).isOne();
                busyWork.execute(handler);
            }
            other.runPendingTasks();
            assertThat(completed).containsExactly(2, 3, 1, 1);
            assertThat(finished.get()).isEqualTo(2);
            assertThat(otherQueue.requestsCount()).isZero();
            for (int requestId : Arrays.asList(2, 3)) {
                ByteBuf response = other.readOutbound();
                assertThat(response).isNotNull();
                try {
                    response.readInt();
                    response.readByte();
                    assertThat(response.readInt()).isEqualTo(requestId);
                } finally {
                    response.release();
                }
            }
            assertThat((Object) other.readOutbound()).isNull();
        } finally {
            release.complete(null);
            executor.shutdownNow();
            busy.finishAndReleaseAll();
            hot.finishAndReleaseAll();
            other.finishAndReleaseAll();
        }
    }

    private static ByteBuf encodeBucketPut(int requestId, int... bucketIds) {
        PutKvRequest request = new PutKvRequest().setTableId(1001).setAcks(1).setTimeoutMs(1000);
        for (int bucket : bucketIds) {
            request.addBucketsReq().setBucketId(bucket).setRecords(new byte[] {(byte) bucket});
        }
        return MessageCodec.encodeRequest(
                ByteBufAllocator.DEFAULT,
                ApiKeys.PUT_KV.id,
                ApiKeys.PUT_KV.highestSupportedVersion,
                requestId,
                request);
    }

    @Test
    void testActiveLazyRequestRetainsBufferUntilProcessed() throws Exception {
        TestingRequestChannel channel = new TestingRequestChannel(100);
        NettyServerHandler handler = newServerHandler(channel);
        TestEmbeddedChannel embeddedChannel = new TestEmbeddedChannel(handler);
        ByteBuf requestBuffer = encodeProduceLogRequest();

        try {
            handler.channelRead(embeddedChannel.pipeline().context(handler), requestBuffer);

            assertThat(channel.requestsCount()).isOne();
            assertThat(requestBuffer.refCnt()).isOne();

            RpcRequest request = channel.pollRequest(0);
            assertThat(request).isNotNull();
            request.releaseBuffer();
            assertThat(requestBuffer.refCnt()).isZero();
        } finally {
            embeddedChannel.finishAndReleaseAll();
        }
    }

    @Test
    void testOnlyOrdinaryKvRequestsCanBeSplitByBucket() {
        assertThat(FlussRequestHandler.canPrepareBucketRequest(new PutKvRequest())).isTrue();
        assertThat(FlussRequestHandler.canPrepareBucketRequest(new LookupRequest())).isTrue();
        assertThat(
                        FlussRequestHandler.canPrepareBucketRequest(
                                new LookupRequest().setInsertIfNotExists(false)))
                .isTrue();
        assertThat(FlussRequestHandler.canPrepareBucketRequest(new PrefixLookupRequest())).isTrue();
        assertThat(
                        FlussRequestHandler.canPrepareBucketRequest(
                                new LookupRequest().setInsertIfNotExists(true)))
                .isFalse();
        assertThat(
                        FlussRequestHandler.canPrepareBucketRequest(
                                new LookupRequest()
                                        .addAllBucketsReqs(
                                                Collections.singletonList(
                                                        new PbLookupReqForBucket()
                                                                .setOriginalPartitionName("p")))))
                .isFalse();
        assertThat(
                        FlussRequestHandler.canPrepareBucketRequest(
                                new PutKvRequest()
                                        .addAllBucketsReqs(
                                                Collections.singletonList(
                                                        new PbPutKvReqForBucket()
                                                                .setOriginalPartitionName("p")))))
                .isFalse();
        assertThat(FlussRequestHandler.canPrepareBucketRequest(new FetchLogRequest())).isFalse();
    }

    @Test
    void testScheduledLazyInputRemainsOwnedByProcessorAfterCancellation() throws Exception {
        ConnectionRequestScheduler.Queue channel =
                new ConnectionRequestScheduler(false).new Queue(100);
        NettyServerHandler handler = newServerHandler(channel);
        TestEmbeddedChannel embeddedChannel = new TestEmbeddedChannel(handler);
        ByteBuf requestBuffer = encodeProduceLogRequest();

        try {
            handler.channelRead(embeddedChannel.pipeline().context(handler), requestBuffer);
            ConnectionRequestScheduler.Work work =
                    (ConnectionRequestScheduler.Work) channel.pollRequest(0);
            assertThat(work).isNotNull();
            embeddedChannel.close();
            embeddedChannel.runPendingTasks();

            assertThat(requestBuffer.refCnt()).isOne();
            work.execute(new FlussRequestHandler(new TestingTabletGatewayService()));
            assertThat(requestBuffer.refCnt()).isZero();
        } finally {
            embeddedChannel.finishAndReleaseAll();
        }
    }

    @Test
    void testScheduledLazyInputIsReleasedWhenQueueRejectsDuringShutdown() throws Exception {
        ConnectionRequestScheduler.Queue channel =
                new ConnectionRequestScheduler(false).new Queue(100);
        NettyServerHandler handler = newServerHandler(channel);
        TestEmbeddedChannel embeddedChannel = new TestEmbeddedChannel(handler);
        ByteBuf requestBuffer = encodeProduceLogRequest();

        try {
            channel.putShutdownRequest();
            handler.channelRead(embeddedChannel.pipeline().context(handler), requestBuffer);
            embeddedChannel.runPendingTasks();
            assertThat(channel.requestsCount()).isZero();
            assertThat(requestBuffer.refCnt()).isZero();
            assertThat(channel.pollRequest(0)).isSameAs(ShutdownRequest.INSTANCE);
        } finally {
            embeddedChannel.finishAndReleaseAll();
        }
    }

    @Test
    void testInactiveProduceRequestCannotAppendReleasedBuffer(@TempDir Path tempDir)
            throws Exception {
        CompletableFuture<MemoryLogRecords> capturedRecords = new CompletableFuture<>();
        CompletableFuture<Void> continueAppend = new CompletableFuture<>();
        AtomicInteger produceInvocations = new AtomicInteger();
        AtomicReference<Boolean> validBeforeRelease = new AtomicReference<>();
        AtomicReference<Throwable> appendFailure = new AtomicReference<>();
        AtomicReference<Throwable> coordinationFailure = new AtomicReference<>();
        RequestChannel channel =
                new CoordinatedRequestChannel(capturedRecords, coordinationFailure);
        NettyServerHandler handler = newServerHandler(channel);
        TestEmbeddedChannel embeddedChannel = new TestEmbeddedChannel(handler);
        ChannelHandlerContext context = embeddedChannel.pipeline().context(handler);
        ByteBuf requestBuffer = encodeProduceLogRequest();

        try (FileLogRecords fileRecords =
                FileLogRecords.open(tempDir.resolve("inactive-request.log").toFile())) {
            TestingTabletGatewayService service =
                    new TestingTabletGatewayService() {
                        @Override
                        public CompletableFuture<ProduceLogResponse> produceLog(
                                ProduceLogRequest request) {
                            produceInvocations.incrementAndGet();
                            ByteBuf recordsSlice = request.getBucketsReqAt(0).getRecordsSlice();
                            MemoryLogRecords records =
                                    MemoryLogRecords.pointToByteBuffer(recordsSlice.nioBuffer());
                            validBeforeRelease.set(records.batches().iterator().next().isValid());
                            capturedRecords.complete(records);
                            try {
                                getWithTimeout(continueAppend, "permission to append records");
                                fileRecords.append(records);
                            } catch (Throwable t) {
                                appendFailure.set(t);
                            }
                            return CompletableFuture.completedFuture(new ProduceLogResponse());
                        }
                    };
            RequestHandler<?>[] requestHandlers =
                    new RequestHandler<?>[RequestType.values().length - 1];
            requestHandlers[RequestType.FLUSS.id] = new FlussRequestHandler(service);
            RequestProcessor processor = new RequestProcessor(0, channel, service, requestHandlers);
            ExecutorService executor = Executors.newSingleThreadExecutor();
            Future<?> processorTask = executor.submit(processor);

            try {
                handler.exceptionCaught(context, new RuntimeException("close connection"));
                handler.channelRead(context, requestBuffer);

                assertThat(requestBuffer.refCnt()).isZero();
                MemoryLogRecords records = capturedRecords.getNow(null);
                if (records != null) {
                    int lastByte = records.getPosition() + records.sizeInBytes() - 1;
                    byte value = records.getMemorySegment().get(lastByte);
                    // Model the Netty pool reusing released request memory before the append.
                    records.getMemorySegment().put(lastByte, (byte) (value ^ 1));
                }

                processor.initiateShutdown();
                continueAppend.complete(null);
                processor.getShutdownFuture().get(5, TimeUnit.SECONDS);
                processorTask.get(5, TimeUnit.SECONDS);

                boolean validOnDisk =
                        fileRecords.sizeInBytes() == 0
                                || fileRecords.batches().iterator().next().isValid();
                assertThat(coordinationFailure.get()).isNull();
                assertThat(appendFailure.get()).isNull();
                if (fileRecords.sizeInBytes() > 0) {
                    assertThat(validBeforeRelease).hasValue(true);
                    assertThat(validOnDisk).isFalse();
                }
                assertThat(fileRecords.sizeInBytes())
                        .withFailMessage(
                                "Inactive ProduceLogRequest appended %s bytes; valid before release: "
                                        + "%s, valid on disk: %s",
                                fileRecords.sizeInBytes(), validBeforeRelease.get(), validOnDisk)
                        .isZero();
                assertThat(produceInvocations)
                        .as("inactive requests must not reach the RPC service")
                        .hasValue(0);
            } finally {
                continueAppend.complete(null);
                if (!processor.getShutdownFuture().isDone()) {
                    processor.initiateShutdown();
                }
                executor.shutdownNow();
                embeddedChannel.finishAndReleaseAll();
            }
        }
    }

    @Test
    void testSuccessfulResponseMarksBucketErrorsWithoutAggregate() {
        RequestsMetricsTest.RecordingMetricRegistry metricRegistry =
                new RequestsMetricsTest.RecordingMetricRegistry();
        HandlerFixture fixture = createTabletServerHandler(metricRegistry);
        try {
            writeRequest(fixture, ApiKeys.LOOKUP, new LookupRequest().setTableId(1), 1001);

            LookupResponse response = new LookupResponse();
            response.addBucketsResp().setBucketId(1).setErrorCode(Errors.TABLE_NOT_EXIST.code());
            response.addBucketsResp().setBucketId(2).setErrorCode(Errors.TABLE_NOT_EXIST.code());
            response.addBucketsResp().setBucketId(3).setErrorCode(Integer.MAX_VALUE);
            response.addBucketsResp().setBucketId(4);
            response.addBucketsResp().setBucketId(5).setErrorCode(Errors.NONE.code());
            completeRequest(fixture, response);

            assertResponse(fixture.channel, ResponseType.SUCCESS_RESPONSE, 1001);
            assertThat(metricRegistry.metrics(MetricNames.ERRORS_RATE, "lookup"))
                    .hasSize(3)
                    .anySatisfy(
                            registered ->
                                    assertThat(registered.group.getAllVariables())
                                            .doesNotContainKey("error")
                                            .isEqualTo(
                                                    Collections.singletonMap("request", "lookup")));
            assertThat(metricRegistry.metrics(MetricNames.ERRORS_RATE, "lookup"))
                    .filteredOn(
                            registered -> registered.group.getAllVariables().containsKey("error"))
                    .anySatisfy(
                            registered -> {
                                assertThat(registered.group.getAllVariables())
                                        .containsEntry("error", Errors.TABLE_NOT_EXIST.name());
                                assertThat(((Meter) registered.metric).getCount()).isEqualTo(2);
                            })
                    .anySatisfy(
                            registered -> {
                                assertThat(registered.group.getAllVariables())
                                        .containsEntry("error", Errors.UNKNOWN_SERVER_ERROR.name());
                                assertThat(((Meter) registered.metric).getCount()).isEqualTo(1);
                            });
            assertAggregateErrorsAreZero(metricRegistry, "lookup");
        } finally {
            fixture.channel.finishAndReleaseAll();
        }
    }

    @Test
    void testEncodingFailureMarksOnlyWholeRpcError() {
        RequestsMetricsTest.RecordingMetricRegistry metricRegistry =
                new RequestsMetricsTest.RecordingMetricRegistry();
        HandlerFixture fixture = createTabletServerHandler(metricRegistry);
        try {
            writeRequest(fixture, ApiKeys.LOOKUP, new LookupRequest().setTableId(1), 1001);

            LookupResponse response = new LookupResponse();
            response.addBucketsResp().setErrorCode(Errors.TABLE_NOT_EXIST.code());
            completeRequest(fixture, response);

            assertResponse(fixture.channel, ResponseType.ERROR_RESPONSE, 1001);
            assertThat(fixture.channel.outboundMessages()).isEmpty();
            assertThat(metricRegistry.metrics(MetricNames.ERRORS_RATE, "lookup"))
                    .hasSize(2)
                    .allSatisfy(
                            registered ->
                                    assertThat(((Meter) registered.metric).getCount()).isEqualTo(1))
                    .anySatisfy(
                            registered ->
                                    assertThat(registered.group.getAllVariables())
                                            .doesNotContainKey("error"))
                    .anySatisfy(
                            registered ->
                                    assertThat(registered.group.getAllVariables())
                                            .containsEntry(
                                                    "error", Errors.UNKNOWN_SERVER_ERROR.name()));
            assertThat(metricRegistry.metrics(MetricNames.ERRORS_RATE, "lookup"))
                    .filteredOn(
                            registered -> registered.group.getAllVariables().containsKey("error"))
                    .noneSatisfy(
                            registered ->
                                    assertThat(registered.group.getAllVariables())
                                            .containsEntry("error", Errors.TABLE_NOT_EXIST.name()));
        } finally {
            fixture.channel.finishAndReleaseAll();
        }
    }

    @Test
    void testBatchResponsesCollectErrorCounts() {
        Map<Integer, Integer> errorCounts = new HashMap<>();

        ProduceLogResponse produceLogResponse = new ProduceLogResponse();
        produceLogResponse.addBucketsResp().setBucketId(1).setErrorCode(1);
        PutKvResponse putKvResponse = new PutKvResponse();
        putKvResponse.addBucketsResp().setBucketId(1).setErrorCode(2);
        LookupResponse lookupResponse = new LookupResponse();
        lookupResponse.addBucketsResp().setBucketId(1).setErrorCode(3);
        FetchLogResponse fetchLogResponse = new FetchLogResponse();
        PbFetchLogRespForTable tableResponse = fetchLogResponse.addTablesResp().setTableId(1);
        tableResponse.addBucketsResp().setBucketId(1).setErrorCode(4);

        produceLogResponse.collectErrorCounts(errorCounts);
        putKvResponse.collectErrorCounts(errorCounts);
        lookupResponse.collectErrorCounts(errorCounts);
        fetchLogResponse.collectErrorCounts(errorCounts);

        assertThat(errorCounts)
                .containsEntry(1, 1)
                .containsEntry(2, 1)
                .containsEntry(3, 1)
                .containsEntry(4, 1);
    }

    @Test
    void testResponseMetricsFailureDoesNotSendErrorResponse() {
        MetricGroup metricGroup =
                new GenericMetricGroup(
                        new ErrorMetricRegistrationFailureRegistry(), null, "tabletserver");
        HandlerFixture fixture = createTabletServerHandler(metricGroup);
        try {
            writeRequest(fixture, ApiKeys.LOOKUP, new LookupRequest().setTableId(1), 1001);

            LookupResponse response = new LookupResponse();
            response.addBucketsResp().setBucketId(1).setErrorCode(Errors.TABLE_NOT_EXIST.code());
            completeRequest(fixture, response);

            assertResponse(fixture.channel, ResponseType.SUCCESS_RESPONSE, 1001);
            assertThat(fixture.channel.outboundMessages()).isEmpty();
        } finally {
            fixture.channel.finishAndReleaseAll();
        }
    }

    private static class ErrorMetricRegistrationFailureRegistry
            extends RequestsMetricsTest.RecordingMetricRegistry {

        @Override
        public void register(Metric metric, String metricName, AbstractMetricGroup group) {
            if (group.getAllVariables().containsKey("error")) {
                throw new Error("metric registration failed");
            }
            super.register(metric, metricName, group);
        }
    }

    @ParameterizedTest
    @MethodSource("successfulResponsesWithErrors")
    void successfulResponsesWithErrorsOnlyMarkPerErrorMetric(
            ApiKeys apiKey, String requestName, ApiMessage request, ApiMessage response) {
        RequestsMetricsTest.RecordingMetricRegistry metricRegistry =
                new RequestsMetricsTest.RecordingMetricRegistry();
        HandlerFixture fixture = createTabletServerHandler(metricRegistry);
        try {
            writeRequest(fixture, apiKey, request, 1001);
            completeRequest(fixture, response);

            assertResponse(fixture.channel, ResponseType.SUCCESS_RESPONSE, 1001);
            assertPerErrorMetric(metricRegistry, requestName);
            assertAggregateErrorsAreZero(metricRegistry, requestName);
        } finally {
            fixture.channel.finishAndReleaseAll();
        }
    }

    @ParameterizedTest
    @MethodSource("requestsByOrigin")
    void fetchLogAndListOffsetsMetricsAreIsolatedByRequestOrigin(
            ApiKeys apiKey, int followerServerId, String requestName, String otherRequestName) {
        RequestsMetricsTest.RecordingMetricRegistry metricRegistry =
                new RequestsMetricsTest.RecordingMetricRegistry();
        HandlerFixture fixture = createTabletServerHandler(metricRegistry);
        try {
            writeRequest(fixture, apiKey, requestWithOrigin(apiKey, followerServerId), 1001);
            completeRequest(fixture, responseWithError(apiKey));

            assertResponse(fixture.channel, ResponseType.SUCCESS_RESPONSE, 1001);
            assertRequestMetricCount(metricRegistry, requestName, 1);
            assertRequestMetricCount(metricRegistry, otherRequestName, 0);
            assertPerErrorMetric(metricRegistry, requestName);
            assertNoPerErrorMetric(metricRegistry, otherRequestName);
            assertAggregateErrorsAreZero(metricRegistry, requestName);
            assertAggregateErrorsAreZero(metricRegistry, otherRequestName);
        } finally {
            fixture.channel.finishAndReleaseAll();
        }
    }

    private static Stream<Arguments> successfulResponsesWithErrors() {
        return Stream.of(
                Arguments.of(
                        ApiKeys.LIST_OFFSETS,
                        "listOffsetsClient",
                        listOffsetsRequest(-1),
                        listOffsetsResponse()),
                Arguments.of(
                        ApiKeys.LIMIT_SCAN,
                        "limitScan",
                        new LimitScanRequest().setTableId(1).setBucketId(1).setLimit(1),
                        new LimitScanResponse().setErrorCode(Errors.TABLE_NOT_EXIST.code())),
                Arguments.of(
                        ApiKeys.SCAN_KV,
                        "scanKv",
                        new ScanKvRequest(),
                        new ScanKvResponse().setErrorCode(Errors.TABLE_NOT_EXIST.code())),
                Arguments.of(
                        ApiKeys.GET_TABLE_STATS,
                        "tableStats",
                        new GetTableStatsRequest().setTableId(1),
                        tableStatsResponse()),
                Arguments.of(
                        ApiKeys.NOTIFY_LEADER_AND_ISR,
                        "notifyLeaderAndIsr",
                        new NotifyLeaderAndIsrRequest().setCoordinatorEpoch(1),
                        notifyLeaderAndIsrResponse()),
                Arguments.of(
                        ApiKeys.STOP_REPLICA,
                        "stopReplica",
                        new StopReplicaRequest().setCoordinatorEpoch(1),
                        stopReplicaResponse()));
    }

    private static Stream<Arguments> requestsByOrigin() {
        return Stream.of(
                Arguments.of(ApiKeys.FETCH_LOG, -1, "fetchLogClient", "fetchLogFollower"),
                Arguments.of(ApiKeys.FETCH_LOG, 0, "fetchLogFollower", "fetchLogClient"),
                Arguments.of(ApiKeys.LIST_OFFSETS, -1, "listOffsetsClient", "listOffsetsFollower"),
                Arguments.of(ApiKeys.LIST_OFFSETS, 0, "listOffsetsFollower", "listOffsetsClient"));
    }

    private static ApiMessage requestWithOrigin(ApiKeys apiKey, int followerServerId) {
        if (apiKey == ApiKeys.FETCH_LOG) {
            return new FetchLogRequest().setFollowerServerId(followerServerId).setMaxBytes(1);
        }
        return listOffsetsRequest(followerServerId);
    }

    private static ApiMessage responseWithError(ApiKeys apiKey) {
        if (apiKey == ApiKeys.FETCH_LOG) {
            FetchLogResponse response = new FetchLogResponse();
            PbFetchLogRespForTable tableResponse = response.addTablesResp().setTableId(1);
            tableResponse
                    .addBucketsResp()
                    .setBucketId(1)
                    .setErrorCode(Errors.TABLE_NOT_EXIST.code());
            return response;
        }
        return listOffsetsResponse();
    }

    private static ListOffsetsRequest listOffsetsRequest(int followerServerId) {
        return new ListOffsetsRequest()
                .setFollowerServerId(followerServerId)
                .setOffsetType(0)
                .setTableId(1);
    }

    private static ListOffsetsResponse listOffsetsResponse() {
        ListOffsetsResponse response = new ListOffsetsResponse();
        response.addBucketsResp().setBucketId(1).setErrorCode(Errors.TABLE_NOT_EXIST.code());
        return response;
    }

    private static GetTableStatsResponse tableStatsResponse() {
        GetTableStatsResponse response = new GetTableStatsResponse();
        response.addBucketsResp().setBucketId(1).setErrorCode(Errors.TABLE_NOT_EXIST.code());
        return response;
    }

    private static NotifyLeaderAndIsrResponse notifyLeaderAndIsrResponse() {
        NotifyLeaderAndIsrResponse response = new NotifyLeaderAndIsrResponse();
        PbNotifyLeaderAndIsrRespForBucket bucketResponse = response.addNotifyBucketsLeaderResp();
        bucketResponse.setTableBucket().setTableId(1).setBucketId(1);
        bucketResponse.setErrorCode(Errors.TABLE_NOT_EXIST.code());
        return response;
    }

    private static StopReplicaResponse stopReplicaResponse() {
        StopReplicaResponse response = new StopReplicaResponse();
        PbStopReplicaRespForBucket bucketResponse = response.addStopReplicasResp();
        bucketResponse.setTableBucket().setTableId(1).setBucketId(1);
        bucketResponse.setErrorCode(Errors.TABLE_NOT_EXIST.code());
        return response;
    }

    private static HandlerFixture createTabletServerHandler(
            RequestsMetricsTest.RecordingMetricRegistry metricRegistry) {
        return createTabletServerHandler(
                new GenericMetricGroup(metricRegistry, null, "tabletserver"));
    }

    private static HandlerFixture createTabletServerHandler(MetricGroup metricGroup) {
        TestingRequestChannel requestChannel = new TestingRequestChannel(100);
        NettyServerHandler handler =
                new NettyServerHandler(
                        requestChannel,
                        new ApiManager(ServerType.TABLET_SERVER),
                        "FLUSS",
                        true,
                        RequestsMetrics.createTabletServerRequestMetrics(metricGroup),
                        new PlainTextAuthenticationPlugin.PlainTextServerAuthenticator());
        EmbeddedChannel channel =
                new EmbeddedChannel(handler) {
                    @Override
                    public SocketAddress remoteAddress() {
                        return REMOTE_ADDRESS;
                    }
                };
        return new HandlerFixture(channel, requestChannel);
    }

    private static void writeRequest(
            HandlerFixture fixture, ApiKeys apiKey, ApiMessage request, int requestId) {
        fixture.channel.writeInbound(
                MessageCodec.encodeRequest(
                        ByteBufAllocator.DEFAULT,
                        apiKey.id,
                        apiKey.highestSupportedVersion,
                        requestId,
                        request));
    }

    private static void completeRequest(HandlerFixture fixture, ApiMessage response) {
        FlussRequest request = (FlussRequest) fixture.requestChannel.getAndRemoveRequest(0);
        request.complete(response);
        fixture.channel.runPendingTasks();
    }

    private static void assertResponse(
            EmbeddedChannel channel, ResponseType expectedResponseType, int expectedRequestId) {
        ByteBuf response = channel.readOutbound();
        try {
            assertThat(response).isNotNull();
            int frameLength = response.readInt();
            assertThat(frameLength).isEqualTo(response.readableBytes());
            assertThat(ResponseType.forId(response.readByte())).isEqualTo(expectedResponseType);
            assertThat(response.readInt()).isEqualTo(expectedRequestId);
        } finally {
            if (response != null) {
                response.release();
            }
        }
    }

    private static void assertPerErrorMetric(
            RequestsMetricsTest.RecordingMetricRegistry metricRegistry, String requestName) {
        assertThat(metricRegistry.metrics(MetricNames.ERRORS_RATE, requestName))
                .filteredOn(registered -> registered.group.getAllVariables().containsKey("error"))
                .singleElement()
                .satisfies(
                        registered -> {
                            assertThat(registered.group.getAllVariables())
                                    .containsEntry("error", Errors.TABLE_NOT_EXIST.name());
                            assertThat(((Meter) registered.metric).getCount()).isEqualTo(1);
                        });
    }

    private static void assertAggregateErrorsAreZero(
            RequestsMetricsTest.RecordingMetricRegistry metricRegistry, String requestName) {
        assertThat(metricRegistry.metrics(MetricNames.ERRORS_RATE, requestName))
                .filteredOn(registered -> !registered.group.getAllVariables().containsKey("error"))
                .singleElement()
                .satisfies(
                        registered -> assertThat(((Meter) registered.metric).getCount()).isZero());
    }

    private static void assertNoPerErrorMetric(
            RequestsMetricsTest.RecordingMetricRegistry metricRegistry, String requestName) {
        assertThat(metricRegistry.metrics(MetricNames.ERRORS_RATE, requestName))
                .filteredOn(registered -> registered.group.getAllVariables().containsKey("error"))
                .isEmpty();
    }

    private static void assertRequestMetricCount(
            RequestsMetricsTest.RecordingMetricRegistry metricRegistry,
            String requestName,
            int expectedCount) {
        assertThat(metricRegistry.metrics(MetricNames.REQUESTS_RATE, requestName))
                .singleElement()
                .satisfies(
                        registered ->
                                assertThat(((Meter) registered.metric).getCount())
                                        .isEqualTo(expectedCount));
    }

    private static final class HandlerFixture {
        private final EmbeddedChannel channel;
        private final TestingRequestChannel requestChannel;

        private HandlerFixture(EmbeddedChannel channel, TestingRequestChannel requestChannel) {
            this.channel = channel;
            this.requestChannel = requestChannel;
        }
    }

    @Test
    @Disabled("TODO: add back in https://github.com/apache/fluss/issues/771")
    void testResponseReturnInOrder() throws Exception {
        initializeLegacyFixture();
        // first write 10 requests to serverHandler.
        for (int i = 0; i < 10; i++) {
            ApiVersionsRequest request = new ApiVersionsRequest();
            request.setClientSoftwareName("test").setClientSoftwareVersion("1.0.0");
            ByteBuf byteBuf =
                    MessageCodec.encodeRequest(
                            ByteBufAllocator.DEFAULT,
                            ApiKeys.API_VERSIONS.id,
                            ApiKeys.API_VERSIONS.highestSupportedVersion,
                            1001,
                            request);
            serverHandler.channelRead(ctx, byteBuf);
        }

        Deque<FlussRequest> inflightResponses =
                serverHandler.inflightResponses(ApiKeys.API_VERSIONS.id);
        assertThat(requestChannel.requestsCount()).isEqualTo(10);
        assertThat(inflightResponses.size()).isEqualTo(10);

        // 1. try to response first request, it will return immediately.
        FlussRequest request1 = (FlussRequest) requestChannel.getAndRemoveRequest(0);
        request1.setRequestCompletedTimeMs(System.currentTimeMillis());
        request1.setRequestDequeTimeMs(System.currentTimeMillis());
        request1.complete(makeApiVersionResponse());
        retry(Duration.ofSeconds(20), () -> assertThat(inflightResponses.size()).isEqualTo(9));

        // 2. try to response 6th, 7th, 8th requests, but it will not return immediately.
        Set<Integer> finishedRequests = new HashSet<>();
        for (int i = 5; i < 8; i++) {
            // always get index 5 as request will be removed from requestChannel after get.
            FlussRequest request = (FlussRequest) requestChannel.getAndRemoveRequest(5);
            request.setRequestCompletedTimeMs(System.currentTimeMillis());
            request.setRequestDequeTimeMs(System.currentTimeMillis());
            request.complete(makeApiVersionResponse());
            assertThat(inflightResponses.size()).isEqualTo(9);
            finishedRequests.add(i);
        }
        int currentIndex = 0;
        for (FlussRequest rpcRequest : inflightResponses) {
            if (finishedRequests.contains(currentIndex)) {
                assertThat(rpcRequest.getResponseFuture().isDone()).isTrue();
            } else {
                assertThat(rpcRequest.getResponseFuture().isDone()).isFalse();
            }
            currentIndex++;
        }

        // 3. try to finish the requests 0 - 3.
        for (int i = 0; i < 4; i++) {
            FlussRequest request = (FlussRequest) requestChannel.getAndRemoveRequest(0);
            request.setRequestCompletedTimeMs(System.currentTimeMillis());
            request.setRequestDequeTimeMs(System.currentTimeMillis());
            request.complete(makeApiVersionResponse());
            final int size = 8 - i;
            retry(
                    Duration.ofSeconds(20),
                    () -> assertThat(inflightResponses.size()).isEqualTo(size));
        }

        // 4. try to finish 5th request, 6th, 7th, 8th requests will also return as it has been done
        // before.
        FlussRequest request = (FlussRequest) requestChannel.getAndRemoveRequest(0);
        request.setRequestCompletedTimeMs(System.currentTimeMillis());
        request.setRequestDequeTimeMs(System.currentTimeMillis());
        request.complete(makeApiVersionResponse());
        retry(Duration.ofSeconds(20), () -> assertThat(inflightResponses.size()).isEqualTo(1));
    }

    @Test
    @Disabled("TODO: add back in https://github.com/apache/fluss/issues/771")
    void testDifferentResponseTypeReturnInSeparateOrder() throws Exception {
        initializeLegacyFixture();
        // 1. first write 5 requests with api as ApiKeys.API_VERSIONS to serverHandler.
        for (int i = 0; i < 5; i++) {
            ApiVersionsRequest request = new ApiVersionsRequest();
            request.setClientSoftwareName("test").setClientSoftwareVersion("1.0.0");
            ByteBuf byteBuf =
                    MessageCodec.encodeRequest(
                            ByteBufAllocator.DEFAULT,
                            ApiKeys.API_VERSIONS.id,
                            ApiKeys.API_VERSIONS.highestSupportedVersion,
                            1001,
                            request);
            serverHandler.channelRead(ctx, byteBuf);
        }

        // 2. second write 5 with api as ApiKeys.LOOKUP to serverHandler.
        LookupRequest lookupRequest = new LookupRequest().setTableId(1);
        PbLookupReqForBucket pbLookupReqForBucket =
                new PbLookupReqForBucket().setPartitionId(1).setBucketId(1);
        pbLookupReqForBucket.addKey("key".getBytes());
        lookupRequest.addAllBucketsReqs(Collections.singleton(pbLookupReqForBucket));
        for (int i = 0; i < 5; i++) {
            ByteBuf byteBuf =
                    MessageCodec.encodeRequest(
                            ByteBufAllocator.DEFAULT,
                            ApiKeys.LOOKUP.id,
                            ApiKeys.LOOKUP.highestSupportedVersion,
                            1001,
                            lookupRequest);
            serverHandler.channelRead(ctx, byteBuf);
        }

        assertThat(requestChannel.requestsCount()).isEqualTo(10);
        Deque<FlussRequest> inflightApiVersionResponses =
                serverHandler.inflightResponses(ApiKeys.API_VERSIONS.id);
        assertThat(inflightApiVersionResponses.size()).isEqualTo(5);

        Deque<FlussRequest> inflightLookupResponses =
                serverHandler.inflightResponses(ApiKeys.LOOKUP.id);
        assertThat(inflightLookupResponses.size()).isEqualTo(5);

        // 3. try to finish one Lookup request, return immediately not to wait the previous five
        // ApiVersionsRequest response first.
        FlussRequest request = (FlussRequest) requestChannel.getAndRemoveRequest(5);
        request.setRequestCompletedTimeMs(System.currentTimeMillis());
        request.setRequestDequeTimeMs(System.currentTimeMillis());
        request.complete(makeLookupResponse());
        retry(
                Duration.ofSeconds(20),
                () -> assertThat(inflightLookupResponses.size()).isEqualTo(4));
        assertThat(inflightApiVersionResponses.size()).isEqualTo(5);
    }

    private static NettyServerHandler newServerHandler(RequestChannel requestChannel) {
        MetricGroup metricGroup = NOPMetricsGroup.newInstance();
        return new NettyServerHandler(
                requestChannel,
                new ApiManager(ServerType.TABLET_SERVER),
                "FLUSS",
                true,
                RequestsMetrics.createCoordinatorServerRequestMetrics(metricGroup),
                new PlainTextAuthenticationPlugin.PlainTextServerAuthenticator());
    }

    private static ByteBuf encodeProduceLogRequest() throws Exception {
        MemoryLogRecords records = genMemoryLogRecordsByObject(DATA1);
        PbProduceLogReqForBucket bucketRequest =
                new PbProduceLogReqForBucket()
                        .setBucketId(0)
                        .setRecordsBytesView(
                                new MemorySegmentBytesView(
                                        records.getMemorySegment(),
                                        records.getPosition(),
                                        records.sizeInBytes()));
        ProduceLogRequest request =
                new ProduceLogRequest()
                        .setTableId(1001L)
                        .setAcks(1)
                        .setTimeoutMs(10_000)
                        .addAllBucketsReqs(Collections.singletonList(bucketRequest));
        return MessageCodec.encodeRequest(
                ByteBufAllocator.DEFAULT,
                ApiKeys.PRODUCE_LOG.id,
                ApiKeys.PRODUCE_LOG.highestSupportedVersion,
                1,
                request);
    }

    private void initializeLegacyFixture() throws Exception {
        requestChannel = new TestingRequestChannel(100);
        MetricGroup metricGroup = NOPMetricsGroup.newInstance();
        serverHandler =
                new NettyServerHandler(
                        requestChannel,
                        new ApiManager(ServerType.TABLET_SERVER),
                        "FLUSS",
                        true,
                        RequestsMetrics.createCoordinatorServerRequestMetrics(metricGroup),
                        new PlainTextAuthenticationPlugin.PlainTextServerAuthenticator());
        ctx = mockChannelHandlerContext();
        serverHandler.channelActive(ctx);
    }

    private static ChannelHandlerContext mockChannelHandlerContext() {
        ChannelId channelId = mock(ChannelId.class);
        when(channelId.asShortText()).thenReturn("short_text");
        when(channelId.asLongText()).thenReturn("long_text");
        Channel channel = mock(Channel.class);
        when(channel.id()).thenReturn(channelId);
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(channel);
        when(ctx.alloc()).thenReturn(ByteBufAllocator.DEFAULT);
        when(ctx.executor()).thenReturn(new DefaultEventExecutor());
        return ctx;
    }

    private ApiVersionsResponse makeApiVersionResponse() {
        ApiVersionsResponse response = new ApiVersionsResponse();
        PbApiVersion apiVersion = new PbApiVersion();
        apiVersion
                .setApiKey(ApiKeys.API_VERSIONS.id)
                .setMinVersion(ApiKeys.API_VERSIONS.lowestSupportedVersion)
                .setMaxVersion(ApiKeys.API_VERSIONS.highestSupportedVersion);
        response.addAllApiVersions(Collections.singletonList(apiVersion));
        return response;
    }

    private static <T> T getWithTimeout(CompletableFuture<T> future, String operation) {
        try {
            return future.get(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while waiting for " + operation, e);
        } catch (Exception e) {
            throw new AssertionError("Failed while waiting for " + operation, e);
        }
    }

    private static final class RecordingRequestChannel extends RequestChannel {
        private int putRequestInvocations;

        private RecordingRequestChannel() {
            super(100);
        }

        @Override
        public void putRequest(RpcRequest request) {
            putRequestInvocations++;
            super.putRequest(request);
        }

        private int getPutRequestInvocations() {
            return putRequestInvocations;
        }
    }

    private static final class CoordinatedRequestChannel extends RequestChannel {
        private final CompletableFuture<MemoryLogRecords> capturedRecords;
        private final AtomicReference<Throwable> coordinationFailure;

        private CoordinatedRequestChannel(
                CompletableFuture<MemoryLogRecords> capturedRecords,
                AtomicReference<Throwable> coordinationFailure) {
            super(100);
            this.capturedRecords = capturedRecords;
            this.coordinationFailure = coordinationFailure;
        }

        @Override
        public void putRequest(RpcRequest request) {
            super.putRequest(request);
            if (request != ShutdownRequest.INSTANCE) {
                try {
                    getWithTimeout(capturedRecords, "request processor to capture records");
                } catch (Throwable t) {
                    coordinationFailure.set(t);
                    throw t;
                }
            }
        }
    }

    private static final class TestEmbeddedChannel extends EmbeddedChannel {

        private static final InetSocketAddress REMOTE_ADDRESS =
                new InetSocketAddress("127.0.0.1", 8080);

        private TestEmbeddedChannel(NettyServerHandler serverHandler) {
            super(DefaultChannelId.newInstance(), serverHandler);
        }

        @Override
        protected SocketAddress remoteAddress0() {
            return REMOTE_ADDRESS;
        }

        @Override
        protected SocketAddress localAddress0() {
            return new InetSocketAddress("127.0.0.1", 18080);
        }
    }

    private LookupResponse makeLookupResponse() {
        LookupResponse response = new LookupResponse();
        PbLookupRespForBucket pbLookupRespForBucket =
                new PbLookupRespForBucket()
                        .setPartitionId(1)
                        .setBucketId(1)
                        .addAllValues(Collections.singleton(new PbValue()));
        response.addAllBucketsResps(Collections.singletonList(pbLookupRespForBucket));
        return response;
    }
}
