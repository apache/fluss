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

package org.apache.fluss.kafka.backend.metadata;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.kafka.backend.metadata.KafkaClusterMetadata.Broker;
import org.apache.fluss.kafka.backend.metadata.KafkaClusterMetadata.Partition;
import org.apache.fluss.kafka.backend.metadata.KafkaClusterMetadata.Topic;
import org.apache.fluss.kafka.backend.metadata.KafkaClusterMetadata.TopicError;
import org.apache.fluss.kafka.backend.metadata.KafkaMetadataQuery.TopicReference;
import org.apache.fluss.kafka.mapping.KafkaTopicMapper;
import org.apache.fluss.rpc.RpcGatewayService;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.ListTablesRequest;
import org.apache.fluss.rpc.messages.MetadataRequest;
import org.apache.fluss.rpc.messages.MetadataResponse;
import org.apache.fluss.rpc.messages.PbBucketMetadata;
import org.apache.fluss.rpc.messages.PbServerNode;
import org.apache.fluss.rpc.messages.PbTableMetadata;
import org.apache.fluss.rpc.messages.PbTablePath;
import org.apache.fluss.rpc.netty.server.Session;
import org.apache.fluss.security.acl.FlussPrincipal;

import org.apache.kafka.common.Uuid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Adapts the existing Fluss metadata RPC to the Kafka Metadata backend contract. */
@Internal
public final class GatewayKafkaMetadataBackend implements KafkaMetadataBackend {

    private static final Logger LOG = LoggerFactory.getLogger(GatewayKafkaMetadataBackend.class);

    private final RpcGatewayService service;
    private final TabletServerGateway gateway;
    private final String databaseName;
    private final KafkaTopicMapper topicMapper;

    /** Creates a metadata backend backed by the local TabletServer gateway. */
    public GatewayKafkaMetadataBackend(
            RpcGatewayService service, TabletServerGateway gateway, String databaseName) {
        this.service = checkNotNull(service);
        this.gateway = checkNotNull(gateway);
        this.databaseName = checkNotNull(databaseName);
        this.topicMapper = new KafkaTopicMapper(databaseName);
    }

    @Override
    public CompletableFuture<KafkaClusterMetadata> getMetadata(KafkaMetadataQuery query) {
        if (query.allTopics() || containsTopicId(query.topics())) {
            setCurrentSession(query);
            return gateway.listTables(new ListTablesRequest().setDatabaseName(databaseName))
                    .thenCompose(
                            response ->
                                    requestFlussMetadata(
                                            query,
                                            new LinkedHashSet<>(response.getTableNamesList())));
        }

        Set<String> topicNames = new LinkedHashSet<>();
        for (TopicReference topic : query.topics()) {
            if (topic.topicName() != null) {
                topicNames.add(topic.topicName());
            }
        }
        return requestFlussMetadata(query, topicNames);
    }

    private CompletableFuture<KafkaClusterMetadata> requestFlussMetadata(
            KafkaMetadataQuery query, Set<String> topicNames) {
        return requestFlussMetadata(query, topicNames, true);
    }

    private CompletableFuture<KafkaClusterMetadata> requestFlussMetadata(
            KafkaMetadataQuery query, Set<String> topicNames, boolean refreshAndRetry) {
        MetadataRequest request = new MetadataRequest();
        for (String topicName : topicNames) {
            request.addAllTablePaths(
                    Collections.singletonList(
                            new PbTablePath()
                                    .setDatabaseName(databaseName)
                                    .setTableName(topicName)));
        }
        setCurrentSession(query);
        try {
            return gateway.metadata(request)
                    .handle(
                            (response, failure) ->
                                    failure == null
                                            ? CompletableFuture.completedFuture(
                                                    toKafkaMetadata(query, response))
                                            : recoverMetadataFailure(
                                                    query, failure, refreshAndRetry))
                    .thenCompose(future -> future);
        } catch (Throwable failure) {
            return recoverMetadataFailure(query, failure, refreshAndRetry);
        }
    }

    private CompletableFuture<KafkaClusterMetadata> recoverMetadataFailure(
            KafkaMetadataQuery query, Throwable failure, boolean refreshAndRetry) {
        if (refreshAndRetry) {
            return currentTopicNames(query)
                    .thenCompose(currentNames -> requestFlussMetadata(query, currentNames, false));
        }
        LOG.warn("Failed to load Kafka metadata from Fluss.", unwrap(failure));
        CompletableFuture<KafkaClusterMetadata> failed = new CompletableFuture<>();
        failed.completeExceptionally(unwrap(failure));
        return failed;
    }

    private CompletableFuture<Set<String>> currentTopicNames(KafkaMetadataQuery query) {
        setCurrentSession(query);
        return gateway.listTables(new ListTablesRequest().setDatabaseName(databaseName))
                .thenApply(
                        response -> {
                            Set<String> currentNames =
                                    new LinkedHashSet<>(response.getTableNamesList());
                            if (!query.allTopics() && !containsTopicId(query.topics())) {
                                Set<String> requestedNames = new HashSet<>();
                                for (TopicReference topic : query.topics()) {
                                    if (topic.topicName() != null) {
                                        requestedNames.add(topic.topicName());
                                    }
                                }
                                currentNames.retainAll(requestedNames);
                            }
                            return currentNames;
                        });
    }

    private KafkaClusterMetadata toKafkaMetadata(
            KafkaMetadataQuery query, MetadataResponse response) {
        List<Broker> brokers = new ArrayList<>();
        Set<Integer> aliveBrokerIds = new HashSet<>();
        for (PbServerNode server : response.getTabletServersList()) {
            brokers.add(
                    new Broker(
                            server.getNodeId(),
                            server.getHost(),
                            server.getPort(),
                            server.hasRack() ? server.getRack() : null));
            aliveBrokerIds.add(server.getNodeId());
        }
        Collections.sort(brokers, Comparator.comparingInt(Broker::id));

        Map<String, Topic> topicsByName = new HashMap<>();
        Map<Uuid, Topic> topicsById = new HashMap<>();
        for (PbTableMetadata table : response.getTableMetadatasList()) {
            if (!databaseName.equals(table.getTablePath().getDatabaseName())) {
                continue;
            }
            Topic topic = toKafkaTopic(table, aliveBrokerIds);
            topicsByName.put(topic.name(), topic);
            topicsById.put(topic.topicId(), topic);
        }

        List<Topic> topics = new ArrayList<>();
        if (query.allTopics()) {
            topics.addAll(topicsByName.values());
            Collections.sort(topics, Comparator.comparing(Topic::name));
        } else {
            for (TopicReference reference : query.topics()) {
                Topic topic =
                        reference.hasTopicId()
                                ? topicsById.get(reference.topicId())
                                : topicsByName.get(reference.topicName());
                if (topic != null && matches(reference, topic)) {
                    topics.add(topic);
                } else {
                    topics.add(missingTopic(reference));
                }
            }
        }
        return new KafkaClusterMetadata(brokers, topics);
    }

    private Topic toKafkaTopic(PbTableMetadata table, Set<Integer> aliveBrokerIds) {
        List<Partition> partitions = new ArrayList<>();
        for (PbBucketMetadata bucket : table.getBucketMetadatasList()) {
            List<Integer> replicas = new ArrayList<>();
            List<Integer> isr = new ArrayList<>();
            List<Integer> offlineReplicas = new ArrayList<>();
            for (int replicaId : bucket.getReplicaIds()) {
                replicas.add(replicaId);
                if (aliveBrokerIds.contains(replicaId)) {
                    isr.add(replicaId);
                } else {
                    offlineReplicas.add(replicaId);
                }
            }
            boolean leaderAvailable =
                    bucket.hasLeaderId() && aliveBrokerIds.contains(bucket.getLeaderId());
            partitions.add(
                    new Partition(
                            bucket.getBucketId(),
                            leaderAvailable ? bucket.getLeaderId() : -1,
                            bucket.hasLeaderEpoch() ? bucket.getLeaderEpoch() : -1,
                            replicas,
                            isr,
                            offlineReplicas,
                            leaderAvailable));
        }
        Collections.sort(partitions, Comparator.comparingInt(Partition::partitionId));
        return new Topic(
                table.getTablePath().getTableName(),
                topicMapper.toTopicId(table.getTableId()),
                TopicError.NONE,
                partitions);
    }

    private static Topic missingTopic(TopicReference reference) {
        TopicError error =
                reference.hasTopicId()
                        ? TopicError.UNKNOWN_TOPIC_ID
                        : TopicError.UNKNOWN_TOPIC_OR_PARTITION;
        return new Topic(
                reference.topicName(), reference.topicId(), error, Collections.emptyList());
    }

    private static boolean matches(TopicReference reference, Topic topic) {
        return (reference.topicName() == null || reference.topicName().equals(topic.name()))
                && (!reference.hasTopicId() || reference.topicId().equals(topic.topicId()));
    }

    private void setCurrentSession(KafkaMetadataQuery query) {
        service.setCurrentSession(
                new Session(
                        (short) 0,
                        query.listenerName(),
                        false,
                        query.clientAddress(),
                        FlussPrincipal.ANONYMOUS));
    }

    private static boolean containsTopicId(List<TopicReference> topics) {
        for (TopicReference topic : topics) {
            if (topic.hasTopicId()) {
                return true;
            }
        }
        return false;
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
