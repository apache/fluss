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
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.kafka.backend.metadata.KafkaClusterMetadata.Broker;
import org.apache.fluss.kafka.backend.metadata.KafkaClusterMetadata.Partition;
import org.apache.fluss.kafka.backend.metadata.KafkaClusterMetadata.Topic;
import org.apache.fluss.kafka.backend.metadata.KafkaClusterMetadata.TopicError;
import org.apache.fluss.kafka.backend.metadata.KafkaMetadataQuery.TopicReference;
import org.apache.fluss.kafka.mapping.KafkaTopicMapper;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.RpcGatewayService;
import org.apache.fluss.rpc.netty.server.Session;
import org.apache.fluss.server.metadata.BucketMetadata;
import org.apache.fluss.server.metadata.MetadataView;
import org.apache.fluss.server.metadata.MetadataViewProvider;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.concurrent.FutureUtils;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Adapts the local TabletServer metadata-cache view to Kafka Metadata responses. */
@Internal
public final class GatewayKafkaMetadataBackend implements KafkaMetadataBackend {
    private static final Logger LOG = LoggerFactory.getLogger(GatewayKafkaMetadataBackend.class);
    private final MetadataViewProvider service;
    private final KafkaTopicMapper topicMapper;

    /** Creates a backend using the local service's schema-free metadata view. */
    public GatewayKafkaMetadataBackend(RpcGatewayService service, KafkaTopicMapper topicMapper) {
        checkNotNull(service);
        checkArgument(
                service instanceof MetadataViewProvider,
                "Kafka Metadata requires a TabletServer metadata view, but the service is %s",
                service.getClass().getSimpleName());
        this.service = (MetadataViewProvider) service;
        this.topicMapper = checkNotNull(topicMapper);
    }

    @Override
    public CompletableFuture<KafkaClusterMetadata> getMetadata(KafkaMetadataQuery query) {
        try {
            MetadataView view =
                    service.getMetadataView(
                            new Session(
                                    (short) 0,
                                    query.listenerName(),
                                    false,
                                    query.clientAddress(),
                                    query.principal()));
            return CompletableFuture.completedFuture(toKafkaMetadata(query, view));
        } catch (Exception failure) {
            Throwable cause = ExceptionUtils.stripCompletionException(failure);
            LOG.warn("Failed to load Kafka metadata from Fluss.", cause);
            return FutureUtils.completedExceptionally(cause);
        }
    }

    private KafkaClusterMetadata toKafkaMetadata(KafkaMetadataQuery query, MetadataView view) {
        List<Broker> brokers = new ArrayList<>();
        Set<Integer> aliveBrokerIds = new HashSet<>();
        for (ServerNode server : view.getTabletServers()) {
            brokers.add(new Broker(server.id(), server.host(), server.port(), server.rack()));
            aliveBrokerIds.add(server.id());
        }
        brokers.sort(Comparator.comparingInt(Broker::id));
        List<Topic> topics = new ArrayList<>();
        if (query.allTopics()) {
            for (TablePath path : view.getTablePaths()) {
                try {
                    Topic topic =
                            view.getTable(path)
                                    .map(table -> toKafkaTopic(table, path, aliveBrokerIds))
                                    .orElse(null);
                    if (topic != null) {
                        topics.add(topic);
                    }
                } catch (Exception failure) {
                    topics.add(failedTopic(path.toString(), Uuid.ZERO_UUID, failure));
                }
            }
            topics.sort(Comparator.comparing(Topic::name));
        } else {
            for (TopicReference reference : query.topics()) {
                try {
                    Optional<MetadataView.Table> table;
                    if (reference.hasTopicId()) {
                        table =
                                topicMapper.isMappedTopicId(reference.topicId())
                                        ? view.getTable(topicMapper.toTableId(reference.topicId()))
                                        : Optional.empty();
                    } else {
                        table = view.getTable(topicMapper.toTablePath(reference.topicName()));
                    }
                    Topic topic =
                            table.map(t -> toKafkaTopic(t, t.getTablePath(), aliveBrokerIds))
                                    .orElse(null);
                    if (topic != null && matches(reference, topic)) {
                        topics.add(
                                new Topic(
                                        reference.topicName() == null
                                                ? topic.name()
                                                : reference.topicName(),
                                        topic.topicId(),
                                        topic.error(),
                                        topic.partitions()));
                    } else {
                        topics.add(missingTopic(reference));
                    }
                } catch (Exception failure) {
                    topics.add(failedTopic(reference.topicName(), reference.topicId(), failure));
                }
            }
        }
        return new KafkaClusterMetadata(brokers, topics);
    }

    @Nullable
    private Topic toKafkaTopic(
            MetadataView.Table table, TablePath tablePath, Set<Integer> aliveBrokerIds) {
        String topicName = tablePath.toString();
        try {
            if (!table.isKafkaEnabled()) {
                return null;
            }
            // Native DDL validates the write contract when Kafka compatibility is enabled.
            topicMapper.toTopicName(tablePath);
        } catch (IllegalArgumentException | InvalidTopicException e) {
            LOG.debug("Table {} has invalid Kafka metadata: {}", topicName, e.getMessage());
            return new Topic(
                    topicName,
                    topicMapper.toTopicId(table.getTableId()),
                    TopicError.INVALID_TOPIC,
                    Collections.emptyList());
        }
        List<Partition> partitions = new ArrayList<>();
        for (BucketMetadata bucket : table.getBuckets()) {
            boolean leaderAvailable =
                    bucket.getLeaderId().isPresent()
                            && aliveBrokerIds.contains(bucket.getLeaderId().getAsInt());
            List<Integer> replicas = new ArrayList<>();
            List<Integer> isr = new ArrayList<>();
            List<Integer> offlineReplicas = new ArrayList<>();
            Set<Integer> isrIds = new HashSet<>();
            if (bucket.getBucketEpoch() != null) {
                for (int isrId : bucket.getIsr()) {
                    isrIds.add(isrId);
                }
            } else if (leaderAvailable) {
                // Legacy metadata has no authoritative ISR. Report only the available leader
                // as a conservative fallback; live followers may still be out of sync.
                isrIds.add(bucket.getLeaderId().getAsInt());
            }
            for (int replicaId : bucket.getReplicas()) {
                replicas.add(replicaId);
                if (isrIds.contains(replicaId)) {
                    isr.add(replicaId);
                }
                if (!aliveBrokerIds.contains(replicaId)) {
                    offlineReplicas.add(replicaId);
                }
            }
            partitions.add(
                    new Partition(
                            bucket.getBucketId(),
                            leaderAvailable ? bucket.getLeaderId().getAsInt() : -1,
                            bucket.getLeaderEpoch().orElse(-1),
                            replicas,
                            isr,
                            offlineReplicas,
                            leaderAvailable));
        }
        Collections.sort(partitions, Comparator.comparingInt(Partition::partitionId));
        return new Topic(
                topicName, topicMapper.toTopicId(table.getTableId()), TopicError.NONE, partitions);
    }

    private static Topic failedTopic(@Nullable String name, Uuid id, Exception failure) {
        LOG.warn("Failed to load Kafka metadata for topic {} (ID {}).", name, id, failure);
        return new Topic(name, id, TopicError.UNKNOWN_SERVER_ERROR, Collections.emptyList());
    }

    private static Topic missingTopic(TopicReference reference) {
        TopicError error =
                reference.hasTopicId()
                        ? TopicError.UNKNOWN_TOPIC_ID
                        : TopicError.UNKNOWN_TOPIC_OR_PARTITION;
        return new Topic(
                reference.topicName(), reference.topicId(), error, Collections.emptyList());
    }

    private boolean matches(TopicReference reference, Topic topic) {
        return (reference.topicName() == null
                        || topicMapper
                                .toTablePath(reference.topicName())
                                .equals(topicMapper.toTablePath(topic.name())))
                && (!reference.hasTopicId() || reference.topicId().equals(topic.topicId()));
    }
}
