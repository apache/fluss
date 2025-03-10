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

import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;

import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class KafkaRequestHandler extends KafkaCommandDecoder {
    private static final Logger log = LoggerFactory.getLogger(KafkaRequestHandler.class);

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
    protected void handleInactive(KafkaRequest request) {
        log.warn("Received a request on an inactive channel: {}", remoteAddress);
        request.fail(new LeaderNotAvailableException("Channel is inactive"));
    }

    @Override
    protected void handleApiVersionsRequest(KafkaRequest request) {}

    @Override
    protected void handleProducerRequest(KafkaRequest request) {}

    @Override
    protected void handleMetadataRequest(KafkaRequest request) {}

    @Override
    protected void handleFindCoordinatorRequest(KafkaRequest request) {}

    @Override
    protected void handleListOffsetRequest(KafkaRequest request) {}

    @Override
    protected void handleOffsetFetchRequest(KafkaRequest request) {}

    @Override
    protected void handleOffsetCommitRequest(KafkaRequest request) {}

    @Override
    protected void handleFetchRequest(KafkaRequest request) {}

    @Override
    protected void handleJoinGroupRequest(KafkaRequest request) {}

    @Override
    protected void handleSyncGroupRequest(KafkaRequest request) {}

    @Override
    protected void handleHeartbeatRequest(KafkaRequest request) {}

    @Override
    protected void handleLeaveGroupRequest(KafkaRequest request) {}

    @Override
    protected void handleDescribeGroupsRequest(KafkaRequest request) {}

    @Override
    protected void handleListGroupsRequest(KafkaRequest request) {}

    @Override
    protected void handleDeleteGroupsRequest(KafkaRequest request) {}

    @Override
    protected void handleSaslHandshakeRequest(KafkaRequest request) {}

    @Override
    protected void handleSaslAuthenticateRequest(KafkaRequest request) {}

    @Override
    protected void handleCreateTopicsRequest(KafkaRequest request) {}

    @Override
    protected void handleInitProducerIdRequest(KafkaRequest request) {}

    @Override
    protected void handleAddPartitionsToTxnRequest(KafkaRequest request) {}

    @Override
    protected void handleAddOffsetsToTxnRequest(KafkaRequest request) {}

    @Override
    protected void handleTxnOffsetCommitRequest(KafkaRequest request) {}

    @Override
    protected void handleEndTxnRequest(KafkaRequest request) {}

    @Override
    protected void handleWriteTxnMarkersRequest(KafkaRequest request) {}

    @Override
    protected void handleDescribeConfigsRequest(KafkaRequest request) {}

    @Override
    protected void handleAlterConfigsRequest(KafkaRequest request) {}

    @Override
    protected void handleDeleteTopicsRequest(KafkaRequest request) {}

    @Override
    protected void handleDeleteRecordsRequest(KafkaRequest request) {}

    @Override
    protected void handleOffsetDeleteRequest(KafkaRequest request) {}

    @Override
    protected void handleCreatePartitionsRequest(KafkaRequest request) {}

    @Override
    protected void handleDescribeClusterRequest(KafkaRequest request) {}
}
