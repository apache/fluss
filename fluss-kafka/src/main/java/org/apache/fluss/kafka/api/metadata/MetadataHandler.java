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

package org.apache.fluss.kafka.api.metadata;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.kafka.KafkaRequestContext;
import org.apache.fluss.kafka.backend.metadata.KafkaClusterMetadata;
import org.apache.fluss.kafka.backend.metadata.KafkaClusterMetadata.Broker;
import org.apache.fluss.kafka.backend.metadata.KafkaClusterMetadata.Partition;
import org.apache.fluss.kafka.backend.metadata.KafkaClusterMetadata.TopicError;
import org.apache.fluss.kafka.backend.metadata.KafkaMetadataBackend;
import org.apache.fluss.kafka.backend.metadata.KafkaMetadataQuery;
import org.apache.fluss.kafka.backend.metadata.KafkaMetadataQuery.TopicReference;
import org.apache.fluss.kafka.dispatcher.KafkaApiHandler;
import org.apache.fluss.kafka.dispatcher.KafkaApiSpec;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.MetadataRequestData.MetadataRequestTopic;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Implements Kafka Metadata versions 0 through 11 using a narrow Fluss metadata backend. */
@Internal
public final class MetadataHandler implements KafkaApiHandler<MetadataRequest> {

    private static final short MAX_SUPPORTED_VERSION = 11;
    private static final KafkaApiSpec API_SPEC =
            new KafkaApiSpec(
                    ApiKeys.METADATA,
                    ApiKeys.METADATA.oldestVersion(),
                    (short) Math.min(ApiKeys.METADATA.latestVersion(), MAX_SUPPORTED_VERSION),
                    true);

    private final KafkaMetadataBackend backend;
    private final boolean controllerAvailable;

    /** Creates a Metadata handler. */
    public MetadataHandler(KafkaMetadataBackend backend) {
        this(backend, false);
    }

    /** Creates a Metadata handler and optionally exposes a Kafka-reachable controller. */
    public MetadataHandler(KafkaMetadataBackend backend, boolean controllerAvailable) {
        this.backend = checkNotNull(backend);
        this.controllerAvailable = controllerAvailable;
    }

    @Override
    public KafkaApiSpec apiSpec() {
        return API_SPEC;
    }

    @Override
    public CompletableFuture<? extends AbstractResponse> handle(
            KafkaRequestContext context, MetadataRequest request) {
        List<TopicReference> validTopics = new ArrayList<>();
        List<KafkaClusterMetadata.Topic> invalidTopics = new ArrayList<>();
        if (!request.isAllTopics()) {
            for (MetadataRequestTopic topic : request.data().topics()) {
                if (topic.name() != null && !Topic.isValid(topic.name())) {
                    invalidTopics.add(
                            new KafkaClusterMetadata.Topic(
                                    topic.name(),
                                    topic.topicId(),
                                    TopicError.INVALID_TOPIC,
                                    Collections.emptyList()));
                } else {
                    // Kafka added the topic ID fields in v10, but ID-based Metadata lookup was not
                    // implemented until v12. This handler intentionally stops at v11.
                    validTopics.add(new TopicReference(topic.name(), Uuid.ZERO_UUID));
                }
            }
        }

        KafkaMetadataQuery query =
                new KafkaMetadataQuery(
                        request.isAllTopics(),
                        validTopics,
                        context.listenerName(),
                        clientAddress(context.remoteAddress()),
                        context.principal());
        return backend.getMetadata(query)
                .thenApply(
                        metadata -> {
                            List<KafkaClusterMetadata.Topic> topics =
                                    new ArrayList<>(metadata.topics());
                            topics.addAll(invalidTopics);
                            return toResponse(
                                    request.version(),
                                    new KafkaClusterMetadata(metadata.brokers(), topics),
                                    controllerAvailable);
                        });
    }

    private static MetadataResponse toResponse(
            short version, KafkaClusterMetadata metadata, boolean controllerAvailable) {
        int controllerId =
                controllerAvailable && !metadata.brokers().isEmpty()
                        ? metadata.brokers().get(0).id()
                        : MetadataResponse.NO_CONTROLLER_ID;
        MetadataResponseData data =
                new MetadataResponseData()
                        .setThrottleTimeMs(0)
                        .setControllerId(controllerId)
                        .setClusterAuthorizedOperations(
                                MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED);
        for (Broker broker : metadata.brokers()) {
            MetadataResponseData.MetadataResponseBroker responseBroker =
                    new MetadataResponseData.MetadataResponseBroker()
                            .setNodeId(broker.id())
                            .setHost(broker.host())
                            .setPort(broker.port());
            if (broker.rack() != null) {
                responseBroker.setRack(broker.rack());
            }
            data.brokers().add(responseBroker);
        }
        for (KafkaClusterMetadata.Topic topic : metadata.topics()) {
            MetadataResponseData.MetadataResponseTopic responseTopic =
                    new MetadataResponseData.MetadataResponseTopic()
                            .setName(topic.name())
                            .setTopicId(topic.topicId())
                            .setErrorCode(toKafkaError(topic.error()).code())
                            .setIsInternal(topic.name() != null && Topic.isInternal(topic.name()))
                            .setTopicAuthorizedOperations(
                                    MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED);
            for (Partition partition : topic.partitions()) {
                responseTopic
                        .partitions()
                        .add(
                                new MetadataResponseData.MetadataResponsePartition()
                                        .setErrorCode(
                                                partition.leaderAvailable()
                                                        ? Errors.NONE.code()
                                                        : Errors.LEADER_NOT_AVAILABLE.code())
                                        .setPartitionIndex(partition.partitionId())
                                        .setLeaderId(partition.leaderId())
                                        .setLeaderEpoch(partition.leaderEpoch())
                                        .setReplicaNodes(partition.replicas())
                                        .setIsrNodes(partition.isr())
                                        .setOfflineReplicas(partition.offlineReplicas()));
            }
            data.topics().add(responseTopic);
        }
        return new MetadataResponse(data, version);
    }

    private static Errors toKafkaError(TopicError error) {
        switch (error) {
            case NONE:
                return Errors.NONE;
            case UNKNOWN_TOPIC_OR_PARTITION:
                return Errors.UNKNOWN_TOPIC_OR_PARTITION;
            case UNKNOWN_TOPIC_ID:
                return Errors.UNKNOWN_TOPIC_ID;
            case INVALID_TOPIC:
                return Errors.INVALID_TOPIC_EXCEPTION;
            default:
                throw new IllegalArgumentException("Unsupported metadata error " + error);
        }
    }

    private static InetAddress clientAddress(SocketAddress remoteAddress) {
        if (remoteAddress instanceof InetSocketAddress) {
            return ((InetSocketAddress) remoteAddress).getAddress();
        }
        return null;
    }
}
