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

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.cluster.MetadataCache;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.rpc.gateway.AdminGateway;
import com.alibaba.fluss.rpc.gateway.TabletServerGateway;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;

import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class KafkaRequestHandler extends KafkaCommandDecoder {
    private static final Logger log = LoggerFactory.getLogger(KafkaRequestHandler.class);

    private final String hostname;
    private final int port;
    private final int serverId;
    private final Configuration conf;
    private final AdminGateway admin;
    private final MetadataCache metadataCache;
    private final TabletServerGateway tablet;
    private final String database;

    public KafkaRequestHandler(
            String hostname,
            int port,
            int serverId,
            Configuration conf,
            AdminGateway admin,
            MetadataCache metadataCache,
            TabletServerGateway tablet) {
        this.hostname = hostname;
        this.port = port;
        this.serverId = serverId;
        this.conf = conf;
        this.admin = admin;
        this.metadataCache = metadataCache;
        this.tablet = tablet;
        this.database = conf.getString(ConfigOptions.KAFKA_DATABASE);
    }

    @VisibleForTesting
    public KafkaRequestHandler() {
        this.hostname = "localhost";
        this.port = 9092;
        this.serverId = 0;
        this.conf = null;
        this.admin = null;
        this.metadataCache = null;
        this.tablet = null;
        this.database = "_kafka";
    }

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
    protected void handleApiVersionsRequest(KafkaRequest request) {
        short apiVersion = request.apiVersion();
        if (!ApiKeys.API_VERSIONS.isVersionSupported(apiVersion)) {
            request.fail(Errors.UNSUPPORTED_VERSION.exception());
            return;
        }
        ApiVersionsResponseData data = new ApiVersionsResponseData();
        for (ApiKeys apiKey : ApiKeys.values()) {
            if (apiKey.minRequiredInterBrokerMagic <= RecordBatch.CURRENT_MAGIC_VALUE) {
                ApiVersionsResponseData.ApiVersion apiVersionData =
                        new ApiVersionsResponseData.ApiVersion()
                                .setApiKey(apiKey.id)
                                .setMinVersion(apiKey.oldestVersion())
                                .setMaxVersion(apiKey.latestVersion());
                if (apiKey.equals(ApiKeys.METADATA)) {
                    // Not support TopicId
                    short v = apiKey.latestVersion() > 11 ? 11 : apiKey.latestVersion();
                    apiVersionData.setMaxVersion(v);
                } else if (apiKey.equals(ApiKeys.FETCH)) {
                    // Not support TopicId
                    short v = apiKey.latestVersion() > 12 ? 12 : apiKey.latestVersion();
                    apiVersionData.setMaxVersion(v);
                }
                data.apiKeys().add(apiVersionData);
            }
        }
        request.complete(new ApiVersionsResponse(data));
    }

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
