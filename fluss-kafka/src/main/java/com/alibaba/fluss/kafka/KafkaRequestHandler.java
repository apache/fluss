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

package com.alibaba.fluss.kafka;

import com.alibaba.fluss.rpc.gateway.TabletServerGateway;
import com.alibaba.fluss.rpc.netty.server.RequestHandler;
import com.alibaba.fluss.rpc.protocol.RequestType;

import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsResponse;

/** Kafka protocol implementation for request handler. */
public class KafkaRequestHandler implements RequestHandler<KafkaRequest> {

    // TODO: we may need a new abstraction between TabletService and ReplicaManager to avoid
    //  affecting Fluss protocol when supporting compatibility with Kafka.
    private final TabletServerGateway gateway;

    public KafkaRequestHandler(TabletServerGateway gateway) {
        this.gateway = gateway;
    }

    @Override
    public RequestType requestType() {
        return RequestType.KAFKA;
    }

    @Override
    public void processRequest(KafkaRequest request) {
        // See kafka.server.KafkaApis#handle
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
    }

    private void handleUnsupportedRequest(KafkaRequest request) {
        String message = String.format("Unsupported request with api key %s", request.apiKey());
        AbstractRequest abstractRequest = request.request();
        AbstractResponse response =
                abstractRequest.getErrorResponse(new UnsupportedOperationException(message));
        request.complete(response);
    }

    void handleApiVersionsRequest(KafkaRequest request) {
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

    void handleProducerRequest(KafkaRequest request) {}

    void handleMetadataRequest(KafkaRequest request) {}

    void handleFindCoordinatorRequest(KafkaRequest request) {}

    void handleListOffsetRequest(KafkaRequest request) {}

    void handleOffsetFetchRequest(KafkaRequest request) {}

    void handleOffsetCommitRequest(KafkaRequest request) {}

    void handleFetchRequest(KafkaRequest request) {}

    void handleJoinGroupRequest(KafkaRequest request) {}

    void handleSyncGroupRequest(KafkaRequest request) {}

    void handleHeartbeatRequest(KafkaRequest request) {}

    void handleLeaveGroupRequest(KafkaRequest request) {}

    void handleDescribeGroupsRequest(KafkaRequest request) {}

    void handleListGroupsRequest(KafkaRequest request) {}

    void handleDeleteGroupsRequest(KafkaRequest request) {}

    void handleSaslHandshakeRequest(KafkaRequest request) {}

    void handleSaslAuthenticateRequest(KafkaRequest request) {}

    void handleCreateTopicsRequest(KafkaRequest request) {}

    void handleInitProducerIdRequest(KafkaRequest request) {}

    void handleAddPartitionsToTxnRequest(KafkaRequest request) {}

    void handleAddOffsetsToTxnRequest(KafkaRequest request) {}

    void handleTxnOffsetCommitRequest(KafkaRequest request) {}

    void handleEndTxnRequest(KafkaRequest request) {}

    void handleWriteTxnMarkersRequest(KafkaRequest request) {}

    void handleDescribeConfigsRequest(KafkaRequest request) {}

    void handleAlterConfigsRequest(KafkaRequest request) {}

    void handleDeleteTopicsRequest(KafkaRequest request) {}

    void handleDeleteRecordsRequest(KafkaRequest request) {}

    void handleOffsetDeleteRequest(KafkaRequest request) {}

    void handleCreatePartitionsRequest(KafkaRequest request) {}

    void handleDescribeClusterRequest(KafkaRequest request) {}
}
