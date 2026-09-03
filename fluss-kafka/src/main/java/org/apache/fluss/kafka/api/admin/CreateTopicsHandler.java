/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.kafka.api.admin;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.kafka.KafkaRequestContext;
import org.apache.fluss.kafka.backend.admin.KafkaTopicAdminBackend;
import org.apache.fluss.kafka.backend.admin.KafkaTopicAdminBackend.CreateTopic;
import org.apache.fluss.kafka.backend.admin.KafkaTopicAdminBackend.TopicResult;
import org.apache.fluss.kafka.dispatcher.KafkaApiHandler;
import org.apache.fluss.kafka.dispatcher.KafkaApiSpec;
import org.apache.fluss.kafka.format.KafkaDataFormat;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicConfig;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;

import javax.annotation.Nullable;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Implements Kafka CreateTopics by creating fixed-schema Arrow log tables in Fluss. */
@Internal
public final class CreateTopicsHandler implements KafkaApiHandler<CreateTopicsRequest> {

    private static final KafkaApiSpec API_SPEC =
            new KafkaApiSpec(
                    ApiKeys.CREATE_TOPICS,
                    ApiKeys.CREATE_TOPICS.oldestVersion(),
                    ApiKeys.CREATE_TOPICS.latestVersion(),
                    true);

    private final KafkaTopicAdminBackend backend;
    private final KafkaDataFormat defaultKeyFormat;
    private final KafkaDataFormat defaultValueFormat;

    /** Creates a CreateTopics handler. */
    public CreateTopicsHandler(KafkaTopicAdminBackend backend) {
        this(backend, KafkaDataFormat.RAW, KafkaDataFormat.RAW);
    }

    /** Creates a CreateTopics handler with formats used when a request omits format configs. */
    public CreateTopicsHandler(
            KafkaTopicAdminBackend backend,
            KafkaDataFormat defaultKeyFormat,
            KafkaDataFormat defaultValueFormat) {
        this.backend = checkNotNull(backend);
        this.defaultKeyFormat = checkNotNull(defaultKeyFormat);
        this.defaultValueFormat = checkNotNull(defaultValueFormat);
    }

    @Override
    public KafkaApiSpec apiSpec() {
        return API_SPEC;
    }

    @Override
    public CompletableFuture<? extends AbstractResponse> handle(
            KafkaRequestContext context, CreateTopicsRequest request) {
        List<CreateTopic> validTopics = new ArrayList<>();
        Map<String, TopicResult> localResults = new LinkedHashMap<>();
        for (CreatableTopic topic : request.data().topics()) {
            TopicResult invalid = validate(topic);
            if (invalid == null) {
                try {
                    FormatConfig formats = parseFormats(topic);
                    validTopics.add(
                            new CreateTopic(
                                    topic.name(),
                                    topic.numPartitions(),
                                    topic.replicationFactor(),
                                    formats.keyFormat,
                                    formats.valueFormat));
                } catch (IllegalArgumentException e) {
                    localResults.put(
                            topic.name(), invalid(topic, Errors.INVALID_CONFIG, e.getMessage()));
                }
            } else {
                localResults.put(topic.name(), invalid);
            }
        }

        return backend.createTopics(
                        validTopics,
                        request.data().validateOnly(),
                        context.listenerName(),
                        clientAddress(context.remoteAddress()),
                        context.principal())
                .thenApply(results -> toResponse(request, localResults, results));
    }

    private static @Nullable TopicResult validate(CreatableTopic topic) {
        if (!Topic.isValid(topic.name())) {
            return invalid(topic, Errors.INVALID_TOPIC_EXCEPTION, "Invalid Kafka topic name.");
        }
        if (topic.numPartitions() <= 0) {
            return invalid(
                    topic,
                    Errors.INVALID_PARTITIONS,
                    "A positive partition count is required for a Fluss topic table.");
        }
        if (topic.replicationFactor() == 0 || topic.replicationFactor() < -1) {
            return invalid(topic, Errors.INVALID_REPLICATION_FACTOR, "Invalid replication factor.");
        }
        if (!topic.assignments().isEmpty()) {
            return invalid(
                    topic,
                    Errors.INVALID_REPLICA_ASSIGNMENT,
                    "Explicit Kafka replica assignments are not supported by Fluss.");
        }
        return null;
    }

    private FormatConfig parseFormats(CreatableTopic topic) {
        KafkaDataFormat keyFormat = defaultKeyFormat;
        KafkaDataFormat valueFormat = defaultValueFormat;
        Map<String, String> configs = new LinkedHashMap<>();
        for (CreatableTopicConfig config : topic.configs()) {
            if (configs.containsKey(config.name())) {
                throw new IllegalArgumentException(
                        "Duplicate Kafka topic config '" + config.name() + "'.");
            }
            configs.put(config.name(), config.value());
        }
        for (Map.Entry<String, String> config : configs.entrySet()) {
            if (KafkaDataFormat.KEY_FORMAT_CONFIG.equals(config.getKey())) {
                keyFormat = KafkaDataFormat.parse(config.getValue());
            } else if (KafkaDataFormat.VALUE_FORMAT_CONFIG.equals(config.getKey())) {
                valueFormat = KafkaDataFormat.parse(config.getValue());
            } else {
                throw new IllegalArgumentException(
                        "Unsupported Kafka topic config '" + config.getKey() + "'.");
            }
        }
        return new FormatConfig(keyFormat, valueFormat);
    }

    private static TopicResult invalid(CreatableTopic topic, Errors error, String message) {
        return new TopicResult(
                topic.name(),
                Uuid.ZERO_UUID,
                error,
                message,
                topic.numPartitions(),
                topic.replicationFactor());
    }

    private static CreateTopicsResponse toResponse(
            CreateTopicsRequest request,
            Map<String, TopicResult> localResults,
            List<TopicResult> backendResults) {
        Map<String, TopicResult> results = new LinkedHashMap<>(localResults);
        for (TopicResult result : backendResults) {
            results.put(result.name(), result);
        }
        CreateTopicsResponseData response = new CreateTopicsResponseData().setThrottleTimeMs(0);
        for (CreatableTopic topic : request.data().topics()) {
            TopicResult result = results.get(topic.name());
            response.topics()
                    .add(
                            new CreateTopicsResponseData.CreatableTopicResult()
                                    .setName(result.name())
                                    .setTopicId(result.topicId())
                                    .setErrorCode(result.error().code())
                                    .setErrorMessage(result.errorMessage())
                                    .setNumPartitions(result.numPartitions())
                                    .setReplicationFactor(result.replicationFactor()));
        }
        return new CreateTopicsResponse(response);
    }

    private static @Nullable InetAddress clientAddress(SocketAddress remoteAddress) {
        if (remoteAddress instanceof InetSocketAddress) {
            return ((InetSocketAddress) remoteAddress).getAddress();
        }
        return null;
    }

    private static final class FormatConfig {
        private final KafkaDataFormat keyFormat;
        private final KafkaDataFormat valueFormat;

        private FormatConfig(KafkaDataFormat keyFormat, KafkaDataFormat valueFormat) {
            this.keyFormat = keyFormat;
            this.valueFormat = valueFormat;
        }
    }
}
