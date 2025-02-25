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

import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;

import java.util.concurrent.CompletableFuture;

@Slf4j
public final class KafkaRequestHandler extends KafkaCommandDecoder {

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        log.info("New connection from {}", ctx.channel().remoteAddress());
        // TODO Channel metrics
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        log.info("Connection closed from {}", ctx.channel().remoteAddress());
        // TODO Channel metrics
    }

    @Override
    protected void close() {
        super.close();
        // Close internal resources
    }

    @Override
    protected void handleInactive(KafkaRequest request, CompletableFuture<AbstractResponse> future) {
        AbstractRequest req = request.request();
        log.warn("Received a request on an inactive channel: {}", remoteAddress);
        AbstractResponse response = req.getErrorResponse(new LeaderNotAvailableException("Channel is not ready"));
        future.complete(response);
    }

    @Override
    protected void handleApiVersionsRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future) {

    }

    @Override
    protected void handleProducerRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future) {

    }

    @Override
    protected void handleMetadataRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future) {

    }

    @Override
    protected void handleFindCoordinatorRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future) {

    }

    @Override
    protected void handleListOffsetRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future) {

    }

    @Override
    protected void handleOffsetFetchRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future) {

    }

    @Override
    protected void handleOffsetCommitRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future) {

    }

    @Override
    protected void handleFetchRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future) {

    }

    @Override
    protected void handleJoinGroupRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future) {

    }

    @Override
    protected void handleSyncGroupRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future) {

    }

    @Override
    protected void handleHeartbeatRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future) {

    }

    @Override
    protected void handleLeaveGroupRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future) {

    }

    @Override
    protected void handleDescribeGroupsRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future) {

    }

    @Override
    protected void handleListGroupsRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future) {

    }

    @Override
    protected void handleDeleteGroupsRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future) {

    }

    @Override
    protected void handleSaslHandshakeRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future) {

    }

    @Override
    protected void handleSaslAuthenticateRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future) {

    }

    @Override
    protected void handleCreateTopicsRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future) {

    }

    @Override
    protected void handleInitProducerIdRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future) {

    }

    @Override
    protected void handleAddPartitionsToTxnRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future) {

    }

    @Override
    protected void handleAddOffsetsToTxnRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future) {

    }

    @Override
    protected void handleTxnOffsetCommitRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future) {

    }

    @Override
    protected void handleEndTxnRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future) {

    }

    @Override
    protected void handleWriteTxnMarkersRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future) {

    }

    @Override
    protected void handleDescribeConfigsRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future) {

    }

    @Override
    protected void handleAlterConfigsRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future) {

    }

    @Override
    protected void handleDeleteTopicsRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future) {

    }

    @Override
    protected void handleDeleteRecordsRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future) {

    }

    @Override
    protected void handleOffsetDeleteRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future) {

    }

    @Override
    protected void handleCreatePartitionsRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future) {

    }

    @Override
    protected void handleDescribeClusterRequest(KafkaRequest request, CompletableFuture<AbstractResponse> future) {

    }
}
