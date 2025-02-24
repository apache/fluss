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

import com.alibaba.fluss.kafka.protocols.KafkaRequest;
import com.alibaba.fluss.kafka.protocols.KafkaResponse;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.RequestAndSize;
import org.apache.kafka.common.requests.RequestHeader;

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
    private final ConcurrentLinkedDeque<KafkaResponse> inflightResponses = new ConcurrentLinkedDeque<>();
    protected final AtomicBoolean isActive = new AtomicBoolean(true);

    @Override
    public void channelRead0(ChannelHandlerContext ctx, ByteBuf buffer) throws Exception {
        CompletableFuture<AbstractResponse> future = new CompletableFuture<>();
        KafkaResponse response = parseRequest(ctx, future, buffer);
        inflightResponses.addLast(response);

        future.whenCompleteAsync((r, t) -> {
            sendResponse(ctx);
        }, ctx.executor());

        try {
            if (!isActive.get()) {
                handleInactive(response, future);
                return;
            }
            switch (response.apiKey) {
                case API_VERSIONS:
                    handleApiVersionsRequest(response, future);
                    break;
                case METADATA:
                    handleMetadataRequest(response, future);
                    break;
                case PRODUCE:
                    handleProducerRequest(response, future);
                    break;
                case FIND_COORDINATOR:
                    handleFindCoordinatorRequest(response, future);
                    break;
                case LIST_OFFSETS:
                    handleListOffsetRequest(response, future);
                    break;
                case OFFSET_FETCH:
                    handleOffsetFetchRequest(response, future);
                    break;
                case OFFSET_COMMIT:
                    handleOffsetCommitRequest(response, future);
                    break;
                case FETCH:
                    handleFetchRequest(response, future);
                    break;
                case JOIN_GROUP:
                    handleJoinGroupRequest(response, future);
                    break;
                case SYNC_GROUP:
                    handleSyncGroupRequest(response, future);
                    break;
                case HEARTBEAT:
                    handleHeartbeatRequest(response, future);
                    break;
                case LEAVE_GROUP:
                    handleLeaveGroupRequest(response, future);
                    break;
                case DESCRIBE_GROUPS:
                    handleDescribeGroupsRequest(response, future);
                    break;
                case LIST_GROUPS:
                    handleListGroupsRequest(response, future);
                    break;
                case DELETE_GROUPS:
                    handleDeleteGroupsRequest(response, future);
                    break;
                case SASL_HANDSHAKE:
                    handleSaslHandshakeRequest(response, future);
                    break;
                case SASL_AUTHENTICATE:
                    handleSaslAuthenticateRequest(response, future);
                    break;
                case CREATE_TOPICS:
                    handleCreateTopicsRequest(response, future);
                    break;
                case INIT_PRODUCER_ID:
                    handleInitProducerIdRequest(response, future);
                    break;
                case ADD_PARTITIONS_TO_TXN:
                    handleAddPartitionsToTxnRequest(response, future);
                    break;
                case ADD_OFFSETS_TO_TXN:
                    handleAddOffsetsToTxnRequest(response, future);
                    break;
                case TXN_OFFSET_COMMIT:
                    handleTxnOffsetCommitRequest(response, future);
                    break;
                case END_TXN:
                    handleEndTxnRequest(response, future);
                    break;
                case WRITE_TXN_MARKERS:
                    handleWriteTxnMarkersRequest(response, future);
                    break;
                case DESCRIBE_CONFIGS:
                    handleDescribeConfigsRequest(response, future);
                    break;
                case ALTER_CONFIGS:
                    handleAlterConfigsRequest(response, future);
                    break;
                case DELETE_TOPICS:
                    handleDeleteTopicsRequest(response, future);
                    break;
                case DELETE_RECORDS:
                    handleDeleteRecordsRequest(response, future);
                    break;
                case OFFSET_DELETE:
                    handleOffsetDeleteRequest(response, future);
                    break;
                case CREATE_PARTITIONS:
                    handleCreatePartitionsRequest(response, future);
                    break;
                case DESCRIBE_CLUSTER:
                    handleDescribeClusterRequest(response, future);
                    break;
                default:
                    handleUnsupportedRequest(response, future);
            }
        } catch (Throwable t) {
            log.error("Error handling request", t);
            future.complete(response.request.getErrorResponse(t));
        } finally {
            buffer.release();
        }
    }

    private void sendResponse(ChannelHandlerContext ctx) {
        KafkaResponse response;
        while ((response = inflightResponses.peekFirst()) != null) {
            CompletableFuture<AbstractResponse> f = response.future;
            AbstractRequest request = response.request;
            ApiKeys apiKey = response.apiKey;
            boolean isDone = f.isDone();

            if (apiKey.equals(PRODUCE)) {
                ProduceRequest produceRequest = (ProduceRequest) request;
                if (produceRequest.acks() == 0) {
                    // if acks=0, we don't need to wait for the response to be sent
                    inflightResponses.pollFirst();
                    continue;
                }
            }

            if (!isDone) {
                break;
            }

            inflightResponses.pollFirst();
            ByteBuf buffer = serialize(response, ctx);
            ctx.writeAndFlush(buffer);
        }
    }

    protected void close() {
        isActive.set(false);
        // TODO cancel all inflight requests
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("Exception caught", cause);
        ctx.close();
    }

    protected void handleUnsupportedRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future) {
        String message = String.format("Unsupported request with api key %s", request.apiKey);
        AbstractResponse response = request.request.getErrorResponse(new UnsupportedOperationException(message));
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


    private static ByteBuf serialize(KafkaResponse response, ChannelHandlerContext ctx) {
        final ObjectSerializationCache cache = new ObjectSerializationCache();
        int headerSize = response.responseHeader.size();
        short apiVersion = response.apiVersion;
        AbstractResponse resp = response.createResponse();
        ApiMessage apiMessage = resp.data();
        int messageSize = apiMessage.size(cache, apiVersion);
        final ByteBuf buffer = ctx.alloc().buffer(headerSize + messageSize);
        buffer.writerIndex(headerSize + messageSize);
        final ByteBuffer nioBuffer = buffer.nioBuffer();
        final ByteBufferAccessor writable = new ByteBufferAccessor(nioBuffer);
        response.responseHeader.data().write(writable, cache, apiVersion);
        apiMessage.write(writable, cache, apiVersion);
        return buffer;
    }

    private static KafkaResponse parseRequest(ChannelHandlerContext ctx, CompletableFuture<AbstractResponse> future,
                                              ByteBuf buffer) {
        ByteBuffer nioBuffer = buffer.nioBuffer();
        RequestHeader header = RequestHeader.parse(nioBuffer);
        if (isUnsupportedApiVersionRequest(header)) {
            ApiVersionsRequest request = new ApiVersionsRequest.Builder(header.apiVersion()).build();
            return new KafkaResponse(API_VERSIONS, header.apiVersion(), header, request, buffer, ctx, future);
        }
        RequestAndSize request = AbstractRequest.parseRequest(header.apiKey(), header.apiVersion(), nioBuffer);
        return new KafkaResponse(header.apiKey(), header.apiVersion(), header, request.request, buffer, ctx, future);
    }

    private static boolean isUnsupportedApiVersionRequest(RequestHeader header) {
        return header.apiKey() == API_VERSIONS && !API_VERSIONS.isVersionSupported(header.apiVersion());
    }
}
