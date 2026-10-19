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
import org.apache.fluss.exception.DatabaseNotExistException;
import org.apache.fluss.kafka.backend.metadata.KafkaClusterMetadata.Broker;
import org.apache.fluss.kafka.backend.metadata.KafkaClusterMetadata.Partition;
import org.apache.fluss.kafka.backend.metadata.KafkaClusterMetadata.Topic;
import org.apache.fluss.kafka.backend.metadata.KafkaClusterMetadata.TopicError;
import org.apache.fluss.kafka.backend.metadata.KafkaMetadataQuery.TopicReference;
import org.apache.fluss.kafka.mapping.KafkaTopicMapper;
import org.apache.fluss.kafka.schema.KafkaTopicSchemaResolver;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.RpcGatewayService;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.ListDatabasesRequest;
import org.apache.fluss.rpc.messages.ListTablesRequest;
import org.apache.fluss.rpc.messages.ListTablesResponse;
import org.apache.fluss.rpc.messages.MetadataRequest;
import org.apache.fluss.rpc.messages.MetadataResponse;
import org.apache.fluss.rpc.messages.PbBucketMetadata;
import org.apache.fluss.rpc.messages.PbServerNode;
import org.apache.fluss.rpc.messages.PbTableMetadata;
import org.apache.fluss.rpc.messages.PbTablePath;
import org.apache.fluss.rpc.netty.server.Session;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.utils.concurrent.FutureUtils;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

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

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Adapts the existing Fluss metadata RPC to the Kafka Metadata backend contract. */
@Internal
public final class GatewayKafkaMetadataBackend implements KafkaMetadataBackend {

    private static final Logger LOG = LoggerFactory.getLogger(GatewayKafkaMetadataBackend.class);

    private final RpcGatewayService service;
    private final TabletServerGateway gateway;
    private final KafkaTopicMapper topicMapper;
    private final KafkaTopicSchemaResolver schemaResolver = new KafkaTopicSchemaResolver();

    /**
     * Creates a metadata backend backed by the local TabletServer gateway.
     *
     * @throws IllegalArgumentException if the service does not implement {@link
     *     TabletServerGateway}
     */
    public GatewayKafkaMetadataBackend(RpcGatewayService service, KafkaTopicMapper topicMapper) {
        this.service = checkNotNull(service);
        checkArgument(
                service instanceof TabletServerGateway,
                "Kafka protocol endpoints can only be enabled on TabletServers, but the service is %s",
                service.getClass().getSimpleName());
        this.gateway = (TabletServerGateway) service;
        this.topicMapper = checkNotNull(topicMapper);
    }

    @Override
    public CompletableFuture<KafkaClusterMetadata> getMetadata(KafkaMetadataQuery query) {
        if (query.allTopics() || containsTopicId(query.topics())) {
            return currentTablePaths(query)
                    .thenCompose(tablePaths -> requestFlussMetadata(query, tablePaths));
        }

        Set<TablePath> tablePaths = new LinkedHashSet<>();
        for (TopicReference topic : query.topics()) {
            if (topic.topicName() != null) {
                tablePaths.add(topicMapper.toTablePath(topic.topicName()));
            }
        }
        return requestFlussMetadata(query, tablePaths);
    }

    private CompletableFuture<KafkaClusterMetadata> requestFlussMetadata(
            KafkaMetadataQuery query, Set<TablePath> tablePaths) {
        return requestFlussMetadata(query, tablePaths, true);
    }

    private CompletableFuture<KafkaClusterMetadata> requestFlussMetadata(
            KafkaMetadataQuery query, Set<TablePath> tablePaths, boolean refreshAndRetry) {
        MetadataRequest request = new MetadataRequest();
        for (TablePath tablePath : tablePaths) {
            request.addAllTablePaths(
                    Collections.singletonList(
                            new PbTablePath()
                                    .setDatabaseName(tablePath.getDatabaseName())
                                    .setTableName(tablePath.getTableName())));
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
            return currentTablePaths(query)
                    .thenCompose(currentPaths -> requestFlussMetadata(query, currentPaths, false));
        }
        LOG.warn("Failed to load Kafka metadata from Fluss.", unwrap(failure));
        CompletableFuture<KafkaClusterMetadata> failed = new CompletableFuture<>();
        failed.completeExceptionally(unwrap(failure));
        return failed;
    }

    private CompletableFuture<Set<TablePath>> currentTablePaths(KafkaMetadataQuery query) {
        CompletableFuture<Set<String>> databasesFuture;
        if (query.allTopics() || containsTopicId(query.topics())) {
            setCurrentSession(query);
            databasesFuture =
                    gateway.listDatabases(new ListDatabasesRequest())
                            .thenApply(
                                    response ->
                                            new LinkedHashSet<>(response.getDatabaseNamesList()));
        } else {
            Set<String> databases = new LinkedHashSet<>();
            for (TopicReference topic : query.topics()) {
                if (topic.topicName() != null) {
                    databases.add(topicMapper.toTablePath(topic.topicName()).getDatabaseName());
                }
            }
            databasesFuture = CompletableFuture.completedFuture(databases);
        }
        return databasesFuture
                .thenCompose(databases -> listTablePaths(query, databases))
                .thenApply(
                        currentPaths -> {
                            if (!query.allTopics() && !containsTopicId(query.topics())) {
                                Set<TablePath> requestedTables = new HashSet<>();
                                for (TopicReference topic : query.topics()) {
                                    if (topic.topicName() != null) {
                                        requestedTables.add(
                                                topicMapper.toTablePath(topic.topicName()));
                                    }
                                }
                                // Canonical names and aliases share one table identity, including
                                // when a missing database triggers this discovery retry.
                                currentPaths.retainAll(requestedTables);
                            }
                            return currentPaths;
                        });
    }

    private CompletableFuture<Set<TablePath>> listTablePaths(
            KafkaMetadataQuery query, Set<String> databases) {
        CompletableFuture<Set<TablePath>> tablesFuture =
                CompletableFuture.completedFuture(new LinkedHashSet<>());
        for (String database : databases) {
            tablesFuture =
                    tablesFuture.thenCompose(
                            tablePaths ->
                                    listTables(query, database)
                                            .thenApply(
                                                    response -> {
                                                        for (String tableName :
                                                                response.getTableNamesList()) {
                                                            tablePaths.add(
                                                                    TablePath.of(
                                                                            database, tableName));
                                                        }
                                                        return tablePaths;
                                                    }));
        }
        return tablesFuture;
    }

    private CompletableFuture<ListTablesResponse> listTables(
            KafkaMetadataQuery query, String database) {
        setCurrentSession(query);
        CompletableFuture<ListTablesResponse> tablesFuture;
        try {
            tablesFuture = gateway.listTables(new ListTablesRequest().setDatabaseName(database));
        } catch (Exception failure) {
            tablesFuture = FutureUtils.completedExceptionally(failure);
        }
        return tablesFuture.exceptionally(
                failure -> {
                    Throwable cause = unwrap(failure);
                    if (cause instanceof DatabaseNotExistException) {
                        // A database may be missing or deleted after listDatabases. Keep loading
                        // the remaining topics and brokers through the normal metadata request.
                        return new ListTablesResponse();
                    }
                    throw new CompletionException(cause);
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

        Map<TablePath, Topic> topicsByTable = new HashMap<>();
        Map<Uuid, Topic> topicsById = new HashMap<>();
        for (PbTableMetadata table : response.getTableMetadatasList()) {
            TablePath tablePath =
                    TablePath.of(
                            table.getTablePath().getDatabaseName(),
                            table.getTablePath().getTableName());
            Topic topic = toKafkaTopic(table, tablePath, aliveBrokerIds);
            if (topic != null) {
                topicsByTable.put(tablePath, topic);
                topicsById.put(topic.topicId(), topic);
            }
        }

        List<Topic> topics = new ArrayList<>();
        if (query.allTopics()) {
            // Invalid enabled tables remain visible as errors. Only disabled tables are omitted.
            topics.addAll(topicsByTable.values());
            Collections.sort(topics, Comparator.comparing(Topic::name));
        } else {
            for (TopicReference reference : query.topics()) {
                Topic topic =
                        reference.hasTopicId()
                                ? topicsById.get(reference.topicId())
                                : topicsByTable.get(topicMapper.toTablePath(reference.topicName()));
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
            }
        }
        return new KafkaClusterMetadata(brokers, topics);
    }

    @Nullable
    private Topic toKafkaTopic(
            PbTableMetadata table, TablePath tablePath, Set<Integer> aliveBrokerIds) {
        String topicName = tablePath.toString();
        try {
            TableDescriptor descriptor = TableDescriptor.fromJsonBytes(table.getTableJson());
            if (!schemaResolver.isEnabled(descriptor)) {
                return null;
            }
            topicMapper.toTopicName(tablePath);
            schemaResolver.resolve(descriptor);
        } catch (IllegalArgumentException | InvalidTopicException e) {
            LOG.debug(
                    "Enabled table {} has an invalid Kafka contract: {}",
                    topicName,
                    e.getMessage());
            return new Topic(
                    topicName,
                    topicMapper.toTopicId(table.getTableId()),
                    TopicError.INVALID_TOPIC,
                    Collections.emptyList());
        }
        List<Partition> partitions = new ArrayList<>();
        for (PbBucketMetadata bucket : table.getBucketMetadatasList()) {
            boolean leaderAvailable =
                    bucket.hasLeaderId() && aliveBrokerIds.contains(bucket.getLeaderId());
            List<Integer> replicas = new ArrayList<>();
            List<Integer> isr = new ArrayList<>();
            List<Integer> offlineReplicas = new ArrayList<>();
            Set<Integer> isrIds = new HashSet<>();
            if (bucket.hasBucketEpoch()) {
                for (int isrId : bucket.getIsrs()) {
                    isrIds.add(isrId);
                }
            } else if (leaderAvailable) {
                // Legacy metadata has no authoritative ISR. Report only the available leader
                // as a conservative fallback; live followers may still be out of sync.
                isrIds.add(bucket.getLeaderId());
            }
            for (int replicaId : bucket.getReplicaIds()) {
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
                            leaderAvailable ? bucket.getLeaderId() : -1,
                            bucket.hasLeaderEpoch() ? bucket.getLeaderEpoch() : -1,
                            replicas,
                            isr,
                            offlineReplicas,
                            leaderAvailable));
        }
        Collections.sort(partitions, Comparator.comparingInt(Partition::partitionId));
        return new Topic(
                topicName, topicMapper.toTopicId(table.getTableId()), TopicError.NONE, partitions);
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
