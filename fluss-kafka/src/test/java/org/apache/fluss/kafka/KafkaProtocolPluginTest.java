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

package org.apache.fluss.kafka;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.KafkaValueConversionType;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.Record;
import org.apache.fluss.kafka.transcode.ArrowKafkaRecordTranscoder;
import org.apache.fluss.kafka.transcode.EncodedRecords;
import org.apache.fluss.kafka.transcode.KafkaRecordTranscoder;
import org.apache.fluss.memory.LazyMemorySegmentPool;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.metrics.util.NOPMetricsGroup;
import org.apache.fluss.rpc.TestingTabletGatewayService;
import org.apache.fluss.rpc.messages.GetTableInfoRequest;
import org.apache.fluss.rpc.messages.GetTableInfoResponse;
import org.apache.fluss.rpc.messages.PbProduceLogRespForBucket;
import org.apache.fluss.rpc.messages.ProduceLogRequest;
import org.apache.fluss.rpc.messages.ProduceLogResponse;
import org.apache.fluss.rpc.netty.server.NettyServer;
import org.apache.fluss.rpc.netty.server.RequestChannel;
import org.apache.fluss.rpc.netty.server.RequestsMetrics;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocatorUtil;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.fluss.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.fluss.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.fluss.shaded.netty4.io.netty.handler.timeout.IdleStateHandler;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.IOUtils;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests Kafka listener configuration and plugin-owned conversion resources. */
class KafkaProtocolPluginTest {

    @Test
    void testZeroDisablesNettyIdleTimeout() throws Exception {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.NETTY_CONNECTION_MAX_IDLE_TIME, Duration.ZERO);
        EmbeddedChannel channel = new EmbeddedChannel();
        try {
            // Adapt the socket type while installing the real pipeline on the embedded event loop.
            SocketChannel socket = mock(SocketChannel.class);
            when(socket.pipeline()).thenReturn(channel.pipeline());
            KafkaProtocolPlugin plugin = new KafkaProtocolPlugin();
            plugin.setup(conf);
            KafkaChannelInitializer initializer =
                    (KafkaChannelInitializer)
                            plugin.createChannelHandler(
                                    new RequestChannel[] {new RequestChannel(1)}, "KAFKA");
            initializer.initChannel(socket);
            IdleStateHandler idleHandler = channel.pipeline().get(IdleStateHandler.class);
            assertThat(idleHandler.getAllIdleTimeInMillis()).isZero();
            assertThat(idleHandler.getReaderIdleTimeInMillis()).isZero();
            assertThat(idleHandler.getWriterIdleTimeInMillis()).isZero();
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testClosesAllTranscodersOnce(boolean failFirstClose) throws Exception {
        List<TrackingTranscoder> transcoders = new ArrayList<>();
        KafkaProtocolPlugin plugin =
                new KafkaProtocolPlugin(
                        () -> {
                            TrackingTranscoder transcoder = new TrackingTranscoder();
                            transcoder.failClose = failFirstClose && transcoders.isEmpty();
                            transcoders.add(transcoder);
                            return transcoder;
                        });
        plugin.setup(new Configuration());
        plugin.createRequestHandler(new TestingTabletGatewayService());
        plugin.createRequestHandler(new TestingTabletGatewayService());
        CompletableFuture<Void> closed = plugin.closeAsync();
        if (failFirstClose) {
            assertThatThrownBy(() -> closed.get(10, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(IOException.class);
        } else {
            closed.get(10, TimeUnit.SECONDS);
        }
        assertThat(transcoders).hasSize(2).allSatisfy(t -> assertThat(t.closeCalls).isEqualTo(1));
        assertThat(plugin.closeAsync()).isSameAs(closed);
        assertThatThrownBy(() -> plugin.createRequestHandler(new TestingTabletGatewayService()))
                .isInstanceOf(IllegalStateException.class);
        assertThat(transcoders).hasSize(2);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testCloseDoesNotWaitForAcknowledgement(boolean failAppend) throws Exception {
        TrackingTranscoder transcoder = new TrackingTranscoder();
        CompletableFuture<ProduceLogResponse> appended = new CompletableFuture<>();
        ProduceMetadataService service =
                new ProduceMetadataService() {
                    @Override
                    public CompletableFuture<ProduceLogResponse> produceLog(
                            ProduceLogRequest request) {
                        return appended;
                    }
                };
        KafkaProtocolPlugin plugin = new KafkaProtocolPlugin(() -> transcoder);
        plugin.setup(new Configuration());
        KafkaRequestHandler handler = (KafkaRequestHandler) plugin.createRequestHandler(service);
        KafkaRequest request = produceRequest();
        try {
            handler.processRequest(request);
            assertThat(request.future()).isNotDone();
            assertThat(transcoder.memorySegmentPool.usedPages()).isZero();
            plugin.closeAsync().get(10, TimeUnit.SECONDS);
            assertThat(transcoder.closeCalls).isEqualTo(1);
            assertThat(appended).isNotDone();
            if (failAppend) {
                appended.completeExceptionally(new IOException("append failed"));
            } else {
                appended.complete(success());
            }
            request.future().get(10, TimeUnit.SECONDS);
        } finally {
            appended.completeExceptionally(new IOException("test cleanup"));
            plugin.closeAsync().get(10, TimeUnit.SECONDS);
            request.releaseBuffer();
        }
    }

    @Test
    void testNettyWaitsForRequestWorkerBeforeClosingProtocol() throws Exception {
        CountDownLatch metadataStarted = new CountDownLatch(1);
        CountDownLatch releaseMetadata = new CountDownLatch(1);
        AtomicBoolean appended = new AtomicBoolean();
        ProduceMetadataService service =
                new ProduceMetadataService() {
                    @Override
                    public CompletableFuture<GetTableInfoResponse> getTableInfo(
                            GetTableInfoRequest request) {
                        metadataStarted.countDown();
                        try {
                            assertThat(releaseMetadata.await(10, TimeUnit.SECONDS)).isTrue();
                        } catch (InterruptedException failure) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError(failure);
                        }
                        return super.getTableInfo(request);
                    }

                    @Override
                    public CompletableFuture<ProduceLogResponse> produceLog(
                            ProduceLogRequest request) {
                        appended.set(true);
                        return CompletableFuture.completedFuture(success());
                    }
                };
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KAFKA_ENABLED, true);
        conf.set(ConfigOptions.NETTY_SERVER_NUM_WORKER_THREADS, 1);
        conf.set(ConfigOptions.NETTY_SERVER_NUM_NETWORK_THREADS, 1);
        MetricGroup metricGroup = NOPMetricsGroup.newInstance();
        KafkaRequest request = produceRequest();
        try (NettyServer server =
                new NettyServer(
                        conf,
                        Collections.singletonList(new Endpoint("localhost", 0, "KAFKA")),
                        service,
                        metricGroup,
                        RequestsMetrics.createCoordinatorServerRequestMetrics(metricGroup))) {
            server.start();
            Endpoint endpoint = server.getBindEndpoints().get(0);
            try (Socket socket = new Socket(endpoint.getHost(), endpoint.getPort())) {
                ProduceRequest produce = request.request();
                ByteBuffer serialized = produce.serializeWithHeader(request.header());
                byte[] bytes = new byte[serialized.remaining()];
                serialized.get(bytes);
                DataOutputStream output = new DataOutputStream(socket.getOutputStream());
                output.writeInt(bytes.length);
                output.write(bytes);
                output.flush();
                assertThat(metadataStarted.await(10, TimeUnit.SECONDS)).isTrue();

                CompletableFuture<Void> closed = server.closeAsync();
                assertThat(closed).isNotDone();
                releaseMetadata.countDown();
                closed.get(10, TimeUnit.SECONDS);
                assertThat(appended).isTrue();
            } finally {
                releaseMetadata.countDown();
            }
        } finally {
            request.releaseBuffer();
        }
    }

    private static ProduceLogResponse success() {
        return new ProduceLogResponse()
                .addAllBucketsResps(
                        Collections.singletonList(
                                new PbProduceLogRespForBucket().setBucketId(0).setBaseOffset(0L)));
    }

    private static KafkaRequest produceRequest() {
        ProduceRequestData data = new ProduceRequestData().setAcks((short) 1).setTimeoutMs(10000);
        data.topicData()
                .add(
                        new ProduceRequestData.TopicProduceData()
                                .setName("kafka.topic")
                                .setPartitionData(
                                        Collections.singletonList(
                                                new ProduceRequestData.PartitionProduceData()
                                                        .setIndex(0)
                                                        .setRecords(
                                                                MemoryRecords.withRecords(
                                                                        Compression.NONE,
                                                                        new SimpleRecord(
                                                                                new byte[] {
                                                                                    65
                                                                                }))))));
        ByteBuf buffer = Unpooled.buffer(1);
        try {
            return new KafkaRequest(
                    ApiKeys.PRODUCE,
                    (short) 9,
                    new RequestHeader(ApiKeys.PRODUCE, (short) 9, "test", 1),
                    new ProduceRequest(data, (short) 9),
                    "KAFKA",
                    buffer,
                    new TestingChannelHandlerContext(),
                    new CompletableFuture<>());
        } finally {
            buffer.release();
        }
    }

    private static class TrackingTranscoder implements KafkaRecordTranscoder {
        private final BufferAllocator allocator = BufferAllocatorUtil.createBufferAllocator(null);
        private final LazyMemorySegmentPool memorySegmentPool =
                LazyMemorySegmentPool.createServerBufferPool(new Configuration());
        protected final ArrowKafkaRecordTranscoder arrow =
                new ArrowKafkaRecordTranscoder(allocator, memorySegmentPool);
        protected volatile int closeCalls;
        private boolean failClose;

        @Override
        public EncodedRecords transcode(int partitionId, List<Record> records, TableInfo table)
                throws Exception {
            return arrow.transcode(partitionId, records, table);
        }

        @Override
        public void close() throws Exception {
            closeCalls++;
            assertThat(memorySegmentPool.usedPages()).isZero();
            IOUtils.closeAll(arrow, allocator, memorySegmentPool);
            if (failClose) {
                throw new IOException("test close failure");
            }
        }
    }

    private static class ProduceMetadataService extends TestingTabletGatewayService {
        @Override
        public CompletableFuture<GetTableInfoResponse> getTableInfo(GetTableInfoRequest request) {
            TableDescriptor descriptor =
                    TableDescriptor.builder()
                            .schema(
                                    Schema.newBuilder()
                                            .column("_kafka_value", DataTypes.BYTES())
                                            .build())
                            .distributedBy(1)
                            .logFormat(LogFormat.ARROW)
                            .property(ConfigOptions.TABLE_KAFKA_ENABLED, true)
                            .property(
                                    ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE,
                                    KafkaValueConversionType.BINARY)
                            .build();
            return CompletableFuture.completedFuture(
                    new GetTableInfoResponse()
                            .setTableId(42L)
                            .setSchemaId(1)
                            .setTableJson(descriptor.toJsonBytes())
                            .setCreatedTime(1L)
                            .setModifiedTime(1L));
        }
    }
}
