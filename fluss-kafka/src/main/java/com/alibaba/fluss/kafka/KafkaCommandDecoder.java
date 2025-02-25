/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.kafka;

import com.alibaba.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.RequestAndSize;
import org.apache.kafka.common.requests.RequestHeader;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.kafka.common.protocol.ApiKeys.API_VERSIONS;
import static org.apache.kafka.common.protocol.ApiKeys.PRODUCE;

@Slf4j
public abstract class KafkaCommandDecoder extends SimpleChannelInboundHandler<ByteBuf> {

    // Need to use a Queue to store the inflight responses, because Kafka clients require the responses to be sent in order.
    // See: org.apache.kafka.clients.InFlightRequests#completeNext
    private final ConcurrentLinkedDeque<KafkaRequest> inflightResponses = new ConcurrentLinkedDeque<>();
    protected final AtomicBoolean isActive = new AtomicBoolean(true);
    protected volatile ChannelHandlerContext ctx;
    protected SocketAddress remoteAddress;

    @Override
    public void channelRead0(ChannelHandlerContext ctx, ByteBuf buffer) throws Exception {
        CompletableFuture<AbstractResponse> future = new CompletableFuture<>();
        try {
            KafkaRequest request = parseRequest(ctx, future, buffer);
            inflightResponses.addLast(request);
            future.whenCompleteAsync((r, t) -> sendResponse(ctx), ctx.executor());

            if (!isActive.get()) {
                handleInactive(request, future);
                return;
            }
            switch (request.apiKey()) {
                case API_VERSIONS:
                    handleApiVersionsRequest(request, future);
                    break;
                case METADATA:
                    handleMetadataRequest(request, future);
                    break;
                case PRODUCE:
                    handleProducerRequest(request, future);
                    break;
                case FIND_COORDINATOR:
                    handleFindCoordinatorRequest(request, future);
                    break;
                case LIST_OFFSETS:
                    handleListOffsetRequest(request, future);
                    break;
                case OFFSET_FETCH:
                    handleOffsetFetchRequest(request, future);
                    break;
                case OFFSET_COMMIT:
                    handleOffsetCommitRequest(request, future);
                    break;
                case FETCH:
                    handleFetchRequest(request, future);
                    break;
                case JOIN_GROUP:
                    handleJoinGroupRequest(request, future);
                    break;
                case SYNC_GROUP:
                    handleSyncGroupRequest(request, future);
                    break;
                case HEARTBEAT:
                    handleHeartbeatRequest(request, future);
                    break;
                case LEAVE_GROUP:
                    handleLeaveGroupRequest(request, future);
                    break;
                case DESCRIBE_GROUPS:
                    handleDescribeGroupsRequest(request, future);
                    break;
                case LIST_GROUPS:
                    handleListGroupsRequest(request, future);
                    break;
                case DELETE_GROUPS:
                    handleDeleteGroupsRequest(request, future);
                    break;
                case SASL_HANDSHAKE:
                    handleSaslHandshakeRequest(request, future);
                    break;
                case SASL_AUTHENTICATE:
                    handleSaslAuthenticateRequest(request, future);
                    break;
                case CREATE_TOPICS:
                    handleCreateTopicsRequest(request, future);
                    break;
                case INIT_PRODUCER_ID:
                    handleInitProducerIdRequest(request, future);
                    break;
                case ADD_PARTITIONS_TO_TXN:
                    handleAddPartitionsToTxnRequest(request, future);
                    break;
                case ADD_OFFSETS_TO_TXN:
                    handleAddOffsetsToTxnRequest(request, future);
                    break;
                case TXN_OFFSET_COMMIT:
                    handleTxnOffsetCommitRequest(request, future);
                    break;
                case END_TXN:
                    handleEndTxnRequest(request, future);
                    break;
                case WRITE_TXN_MARKERS:
                    handleWriteTxnMarkersRequest(request, future);
                    break;
                case DESCRIBE_CONFIGS:
                    handleDescribeConfigsRequest(request, future);
                    break;
                case ALTER_CONFIGS:
                    handleAlterConfigsRequest(request, future);
                    break;
                case DELETE_TOPICS:
                    handleDeleteTopicsRequest(request, future);
                    break;
                case DELETE_RECORDS:
                    handleDeleteRecordsRequest(request, future);
                    break;
                case OFFSET_DELETE:
                    handleOffsetDeleteRequest(request, future);
                    break;
                case CREATE_PARTITIONS:
                    handleCreatePartitionsRequest(request, future);
                    break;
                case DESCRIBE_CLUSTER:
                    handleDescribeClusterRequest(request, future);
                    break;
                default:
                    handleUnsupportedRequest(request, future);
            }
        } catch (Throwable t) {
            log.error("Error handling request", t);
            future.completeExceptionally(t);
        } finally {
            buffer.release();
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        this.ctx = ctx;
        this.remoteAddress = ctx.channel().remoteAddress();
        isActive.set(true);
    }

    private void sendResponse(ChannelHandlerContext ctx) {
        KafkaRequest request;
        while ((request = inflightResponses.peekFirst()) != null) {
            CompletableFuture<AbstractResponse> f = request.future();
            ApiKeys apiKey = request.apiKey();
            boolean isDone = f.isDone();
            boolean cancelled = request.cancelled();

            if (apiKey.equals(PRODUCE)) {
                ProduceRequest produceRequest = request.request();
                if (produceRequest.acks() == 0) {
                    // if acks=0, we don't need to wait for the response to be sent
                    inflightResponses.pollFirst();
                    continue;
                }
            }

            if (!isDone) {
                break;
            }

            if (cancelled) {
                inflightResponses.pollFirst();
                request.releaseBuffer();
                continue;
            }

            inflightResponses.pollFirst();
            if (isActive.get()) {
                ByteBuf buffer = request.serialize();
                ctx.writeAndFlush(buffer);
            }
        }
    }

    protected void close() {
        isActive.set(false);
        ctx.close();
        log.warn("Close channel {} with {} pending requests.", remoteAddress, inflightResponses.size());
        for (KafkaRequest request : inflightResponses) {
            request.cancel();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("Exception caught on channel {}", remoteAddress, cause);
        close();
    }

    protected void handleUnsupportedRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future) {
        String message = String.format("Unsupported request with api key %s", request.apiKey());
        AbstractRequest abstractRequest = request.request();
        AbstractResponse response = abstractRequest.getErrorResponse(new UnsupportedOperationException(message));
        future.complete(response);
    }

    protected abstract void handleInactive(KafkaRequest request, CompletableFuture<AbstractResponse> future);

    protected abstract void handleApiVersionsRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future);

    protected abstract void handleProducerRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future);

    protected abstract void handleMetadataRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future);

    protected abstract void handleFindCoordinatorRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future);

    protected abstract void handleListOffsetRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future);

    protected abstract void handleOffsetFetchRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future);

    protected abstract void handleOffsetCommitRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future);

    protected abstract void handleFetchRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future);

    protected abstract void handleJoinGroupRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future);

    protected abstract void handleSyncGroupRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future);

    protected abstract void handleHeartbeatRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future);

    protected abstract void handleLeaveGroupRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future);

    protected abstract void handleDescribeGroupsRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future);

    protected abstract void handleListGroupsRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future);

    protected abstract void handleDeleteGroupsRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future);

    protected abstract void handleSaslHandshakeRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future);

    protected abstract void handleSaslAuthenticateRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future);

    protected abstract void handleCreateTopicsRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future);

    protected abstract void handleInitProducerIdRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future);

    protected abstract void handleAddPartitionsToTxnRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future);

    protected abstract void handleAddOffsetsToTxnRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future);

    protected abstract void handleTxnOffsetCommitRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future);

    protected abstract void handleEndTxnRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future);

    protected abstract void handleWriteTxnMarkersRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future);

    protected abstract void handleDescribeConfigsRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future);

    protected abstract void handleAlterConfigsRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future);

    protected abstract void handleDeleteTopicsRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future);

    protected abstract void handleDeleteRecordsRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future);

    protected abstract void handleOffsetDeleteRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future);

    protected abstract void handleCreatePartitionsRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future);

    protected abstract void handleDescribeClusterRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future);


    private static KafkaRequest parseRequest(ChannelHandlerContext ctx, CompletableFuture<AbstractResponse> future,
                                             ByteBuf buffer) {
        ByteBuffer nioBuffer = buffer.nioBuffer();
        RequestHeader header = RequestHeader.parse(nioBuffer);
        if (isUnsupportedApiVersionRequest(header)) {
            ApiVersionsRequest request = new ApiVersionsRequest.Builder(header.apiVersion()).build();
            return new KafkaRequest(API_VERSIONS, header.apiVersion(), header, request, buffer, ctx, future);
        }
        RequestAndSize request = AbstractRequest.parseRequest(header.apiKey(), header.apiVersion(), nioBuffer);
        return new KafkaRequest(header.apiKey(), header.apiVersion(), header, request.request, buffer, ctx, future);
    }

    private static boolean isUnsupportedApiVersionRequest(RequestHeader header) {
        return header.apiKey() == API_VERSIONS && !API_VERSIONS.isVersionSupported(header.apiVersion());
    }
}
