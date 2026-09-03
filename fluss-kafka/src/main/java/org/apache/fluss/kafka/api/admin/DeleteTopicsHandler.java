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
import org.apache.fluss.kafka.backend.admin.KafkaTopicAdminBackend.DeleteTopic;
import org.apache.fluss.kafka.backend.admin.KafkaTopicAdminBackend.TopicResult;
import org.apache.fluss.kafka.dispatcher.KafkaApiHandler;
import org.apache.fluss.kafka.dispatcher.KafkaApiSpec;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.DeleteTopicsRequestData.DeleteTopicState;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.DeleteTopicsRequest;
import org.apache.kafka.common.requests.DeleteTopicsResponse;

import javax.annotation.Nullable;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Implements Kafka DeleteTopics by deleting the corresponding Fluss tables. */
@Internal
public final class DeleteTopicsHandler implements KafkaApiHandler<DeleteTopicsRequest> {

    private static final short TOPIC_ID_VERSION = 6;
    private static final KafkaApiSpec API_SPEC =
            new KafkaApiSpec(
                    ApiKeys.DELETE_TOPICS,
                    ApiKeys.DELETE_TOPICS.oldestVersion(),
                    ApiKeys.DELETE_TOPICS.latestVersion(),
                    true);

    private final KafkaTopicAdminBackend backend;

    /** Creates a DeleteTopics handler. */
    public DeleteTopicsHandler(KafkaTopicAdminBackend backend) {
        this.backend = checkNotNull(backend);
    }

    @Override
    public KafkaApiSpec apiSpec() {
        return API_SPEC;
    }

    @Override
    public CompletableFuture<? extends AbstractResponse> handle(
            KafkaRequestContext context, DeleteTopicsRequest request) {
        List<DeleteTopic> topics = new ArrayList<>();
        if (request.version() < TOPIC_ID_VERSION) {
            for (String topicName : request.data().topicNames()) {
                topics.add(new DeleteTopic(topicName, Uuid.ZERO_UUID));
            }
        } else {
            for (DeleteTopicState topic : request.data().topics()) {
                topics.add(new DeleteTopic(topic.name(), topic.topicId()));
            }
        }
        return backend.deleteTopics(
                        topics,
                        context.listenerName(),
                        clientAddress(context.remoteAddress()),
                        context.principal())
                .thenApply(DeleteTopicsHandler::toResponse);
    }

    private static DeleteTopicsResponse toResponse(List<TopicResult> results) {
        DeleteTopicsResponseData response = new DeleteTopicsResponseData().setThrottleTimeMs(0);
        for (TopicResult result : results) {
            response.responses()
                    .add(
                            new DeleteTopicsResponseData.DeletableTopicResult()
                                    .setName(result.name())
                                    .setTopicId(result.topicId())
                                    .setErrorCode(result.error().code())
                                    .setErrorMessage(result.errorMessage()));
        }
        return new DeleteTopicsResponse(response);
    }

    private static @Nullable InetAddress clientAddress(SocketAddress remoteAddress) {
        if (remoteAddress instanceof InetSocketAddress) {
            return ((InetSocketAddress) remoteAddress).getAddress();
        }
        return null;
    }
}
