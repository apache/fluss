/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
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
import com.alibaba.fluss.shaded.netty4.io.netty.util.ReferenceCountUtil;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.RequestAndSize;
import org.apache.kafka.common.requests.RequestHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.kafka.common.protocol.ApiKeys.API_VERSIONS;
import static org.apache.kafka.common.protocol.ApiKeys.PRODUCE;

public abstract class KafkaCommandDecoder extends SimpleChannelInboundHandler<ByteBuf> {
    private static final Logger log = LoggerFactory.getLogger(KafkaCommandDecoder.class);

    // Need to use a Queue to store the inflight responses, because Kafka clients require the
    // responses to be sent in order.
    // See: org.apache.kafka.clients.InFlightRequests#completeNext
    private final ConcurrentLinkedDeque<KafkaRequest> inflightResponses =
            new ConcurrentLinkedDeque<>();
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
                try {
                    handleInactive(request);
                } finally {
                    ReferenceCountUtil.release(buffer);
                }
                return;
            }
            switch (request.apiKey()) {
                case API_VERSIONS:
                    handleApiVersionsRequest(request);
                    break;
                case METADATA:
                    handleMetadataRequest(request);
                    break;
                case PRODUCE:
                    handleProducerRequest(request);
                    break;
                case FIND_COORDINATOR:
                    handleFindCoordinatorRequest(request);
                    break;
                case LIST_OFFSETS:
                    handleListOffsetRequest(request);
                    break;
                case OFFSET_FETCH:
                    handleOffsetFetchRequest(request);
                    break;
                case OFFSET_COMMIT:
                    handleOffsetCommitRequest(request);
                    break;
                case FETCH:
                    handleFetchRequest(request);
                    break;
                case JOIN_GROUP:
                    handleJoinGroupRequest(request);
                    break;
                case SYNC_GROUP:
                    handleSyncGroupRequest(request);
                    break;
                case HEARTBEAT:
                    handleHeartbeatRequest(request);
                    break;
                case LEAVE_GROUP:
                    handleLeaveGroupRequest(request);
                    break;
                case DESCRIBE_GROUPS:
                    handleDescribeGroupsRequest(request);
                    break;
                case LIST_GROUPS:
                    handleListGroupsRequest(request);
                    break;
                case DELETE_GROUPS:
                    handleDeleteGroupsRequest(request);
                    break;
                case SASL_HANDSHAKE:
                    handleSaslHandshakeRequest(request);
                    break;
                case SASL_AUTHENTICATE:
                    handleSaslAuthenticateRequest(request);
                    break;
                case CREATE_TOPICS:
                    handleCreateTopicsRequest(request);
                    break;
                case INIT_PRODUCER_ID:
                    handleInitProducerIdRequest(request);
                    break;
                case ADD_PARTITIONS_TO_TXN:
                    handleAddPartitionsToTxnRequest(request);
                    break;
                case ADD_OFFSETS_TO_TXN:
                    handleAddOffsetsToTxnRequest(request);
                    break;
                case TXN_OFFSET_COMMIT:
                    handleTxnOffsetCommitRequest(request);
                    break;
                case END_TXN:
                    handleEndTxnRequest(request);
                    break;
                case WRITE_TXN_MARKERS:
                    handleWriteTxnMarkersRequest(request);
                    break;
                case DESCRIBE_CONFIGS:
                    handleDescribeConfigsRequest(request);
                    break;
                case ALTER_CONFIGS:
                    handleAlterConfigsRequest(request);
                    break;
                case DELETE_TOPICS:
                    handleDeleteTopicsRequest(request);
                    break;
                case DELETE_RECORDS:
                    handleDeleteRecordsRequest(request);
                    break;
                case OFFSET_DELETE:
                    handleOffsetDeleteRequest(request);
                    break;
                case CREATE_PARTITIONS:
                    handleCreatePartitionsRequest(request);
                    break;
                case DESCRIBE_CLUSTER:
                    handleDescribeClusterRequest(request);
                    break;
                default:
                    handleUnsupportedRequest(request);
            }
        } catch (Throwable t) {
            log.error("Error handling request", t);
            future.completeExceptionally(t);
        } finally {
            ReferenceCountUtil.release(buffer);
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
                if (produceRequest.acks() == 0 && isDone) {
                    // if acks=0, we don't need to wait for the response to be sent
                    inflightResponses.pollFirst();
                    request.releaseBuffer();
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
        log.warn(
                "Close channel {} with {} pending requests.",
                remoteAddress,
                inflightResponses.size());
        for (KafkaRequest request : inflightResponses) {
            request.cancel();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("Exception caught on channel {}", remoteAddress, cause);
        close();
    }

    protected void handleUnsupportedRequest(KafkaRequest request) {
        String message = String.format("Unsupported request with api key %s", request.apiKey());
        AbstractRequest abstractRequest = request.request();
        AbstractResponse response =
                abstractRequest.getErrorResponse(new UnsupportedOperationException(message));
        request.complete(response);
    }

    protected abstract void handleInactive(KafkaRequest request);

    protected abstract void handleApiVersionsRequest(KafkaRequest request);

    protected abstract void handleProducerRequest(KafkaRequest request);

    protected abstract void handleMetadataRequest(KafkaRequest request);

    protected abstract void handleFindCoordinatorRequest(KafkaRequest request);

    protected abstract void handleListOffsetRequest(KafkaRequest request);

    protected abstract void handleOffsetFetchRequest(KafkaRequest request);

    protected abstract void handleOffsetCommitRequest(KafkaRequest request);

    protected abstract void handleFetchRequest(KafkaRequest request);

    protected abstract void handleJoinGroupRequest(KafkaRequest request);

    protected abstract void handleSyncGroupRequest(KafkaRequest request);

    protected abstract void handleHeartbeatRequest(KafkaRequest request);

    protected abstract void handleLeaveGroupRequest(KafkaRequest request);

    protected abstract void handleDescribeGroupsRequest(KafkaRequest request);

    protected abstract void handleListGroupsRequest(KafkaRequest request);

    protected abstract void handleDeleteGroupsRequest(KafkaRequest request);

    protected abstract void handleSaslHandshakeRequest(KafkaRequest request);

    protected abstract void handleSaslAuthenticateRequest(KafkaRequest request);

    protected abstract void handleCreateTopicsRequest(KafkaRequest request);

    protected abstract void handleInitProducerIdRequest(KafkaRequest request);

    protected abstract void handleAddPartitionsToTxnRequest(KafkaRequest request);

    protected abstract void handleAddOffsetsToTxnRequest(KafkaRequest request);

    protected abstract void handleTxnOffsetCommitRequest(KafkaRequest request);

    protected abstract void handleEndTxnRequest(KafkaRequest request);

    protected abstract void handleWriteTxnMarkersRequest(KafkaRequest request);

    protected abstract void handleDescribeConfigsRequest(KafkaRequest request);

    protected abstract void handleAlterConfigsRequest(KafkaRequest request);

    protected abstract void handleDeleteTopicsRequest(KafkaRequest request);

    protected abstract void handleDeleteRecordsRequest(KafkaRequest request);

    protected abstract void handleOffsetDeleteRequest(KafkaRequest request);

    protected abstract void handleCreatePartitionsRequest(KafkaRequest request);

    protected abstract void handleDescribeClusterRequest(KafkaRequest request);

    private static KafkaRequest parseRequest(
            ChannelHandlerContext ctx, CompletableFuture<AbstractResponse> future, ByteBuf buffer) {
        ByteBuffer nioBuffer = buffer.nioBuffer();
        RequestHeader header = RequestHeader.parse(nioBuffer);
        if (isUnsupportedApiVersionRequest(header)) {
            ApiVersionsRequest request =
                    new ApiVersionsRequest.Builder(header.apiVersion()).build();
            return new KafkaRequest(
                    API_VERSIONS, header.apiVersion(), header, request, buffer, ctx, future);
        }
        RequestAndSize request =
                AbstractRequest.parseRequest(header.apiKey(), header.apiVersion(), nioBuffer);
        return new KafkaRequest(
                header.apiKey(), header.apiVersion(), header, request.request, buffer, ctx, future);
    }

    private static boolean isUnsupportedApiVersionRequest(RequestHeader header) {
        return header.apiKey() == API_VERSIONS
                && !API_VERSIONS.isVersionSupported(header.apiVersion());
    }
}
