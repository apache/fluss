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

package org.apache.fluss.kafka.backend.admin;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.kafka.format.KafkaDataFormat;
import org.apache.fluss.security.acl.FlussPrincipal;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;

import javax.annotation.Nullable;

import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** Backend contract for mapping Kafka topic lifecycle operations to Fluss tables. */
@Internal
public interface KafkaTopicAdminBackend {

    /** Creates or validates the requested topics. */
    CompletableFuture<List<TopicResult>> createTopics(
            List<CreateTopic> topics,
            boolean validateOnly,
            String listenerName,
            @Nullable InetAddress clientAddress);

    /** Creates or validates topics on behalf of the authenticated Kafka principal. */
    default CompletableFuture<List<TopicResult>> createTopics(
            List<CreateTopic> topics,
            boolean validateOnly,
            String listenerName,
            @Nullable InetAddress clientAddress,
            FlussPrincipal principal) {
        return createTopics(topics, validateOnly, listenerName, clientAddress);
    }

    /** Deletes the requested topics. */
    CompletableFuture<List<TopicResult>> deleteTopics(
            List<DeleteTopic> topics, String listenerName, @Nullable InetAddress clientAddress);

    /** Deletes topics on behalf of the authenticated Kafka principal. */
    default CompletableFuture<List<TopicResult>> deleteTopics(
            List<DeleteTopic> topics,
            String listenerName,
            @Nullable InetAddress clientAddress,
            FlussPrincipal principal) {
        return deleteTopics(topics, listenerName, clientAddress);
    }

    /** A validated request to create one topic. */
    final class CreateTopic {
        private final String name;
        private final int numPartitions;
        private final short replicationFactor;
        private final KafkaDataFormat keyFormat;
        private final KafkaDataFormat valueFormat;

        /** Creates a topic specification. */
        public CreateTopic(
                String name,
                int numPartitions,
                short replicationFactor,
                KafkaDataFormat keyFormat,
                KafkaDataFormat valueFormat) {
            this.name = name;
            this.numPartitions = numPartitions;
            this.replicationFactor = replicationFactor;
            this.keyFormat = keyFormat;
            this.valueFormat = valueFormat;
        }

        /** Returns the Kafka topic name. */
        public String name() {
            return name;
        }

        /** Returns the requested partition count. */
        public int numPartitions() {
            return numPartitions;
        }

        /** Returns the requested replication factor, or {@code -1} for the Fluss default. */
        public short replicationFactor() {
            return replicationFactor;
        }

        /** Returns the interpretation of Kafka record keys. */
        public KafkaDataFormat keyFormat() {
            return keyFormat;
        }

        /** Returns the interpretation of Kafka record values. */
        public KafkaDataFormat valueFormat() {
            return valueFormat;
        }
    }

    /** A request to delete one topic by name or Kafka topic id. */
    final class DeleteTopic {
        private final @Nullable String name;
        private final Uuid topicId;

        /** Creates a topic deletion reference. */
        public DeleteTopic(@Nullable String name, Uuid topicId) {
            this.name = name;
            this.topicId = topicId;
        }

        /** Returns the topic name, if supplied. */
        public @Nullable String name() {
            return name;
        }

        /** Returns the Kafka topic id, or {@link Uuid#ZERO_UUID}. */
        public Uuid topicId() {
            return topicId;
        }
    }

    /** Result of one topic lifecycle operation. */
    final class TopicResult {
        private final @Nullable String name;
        private final Uuid topicId;
        private final Errors error;
        private final @Nullable String errorMessage;
        private final int numPartitions;
        private final short replicationFactor;

        /** Creates a topic operation result. */
        public TopicResult(
                @Nullable String name,
                Uuid topicId,
                Errors error,
                @Nullable String errorMessage,
                int numPartitions,
                short replicationFactor) {
            this.name = name;
            this.topicId = topicId;
            this.error = error;
            this.errorMessage = errorMessage;
            this.numPartitions = numPartitions;
            this.replicationFactor = replicationFactor;
        }

        /** Returns the topic name, if known. */
        public @Nullable String name() {
            return name;
        }

        /** Returns the Kafka topic id, if known. */
        public Uuid topicId() {
            return topicId;
        }

        /** Returns the Kafka protocol error. */
        public Errors error() {
            return error;
        }

        /** Returns the optional error detail. */
        public @Nullable String errorMessage() {
            return errorMessage;
        }

        /** Returns the created partition count. */
        public int numPartitions() {
            return numPartitions;
        }

        /** Returns the created replication factor. */
        public short replicationFactor() {
            return replicationFactor;
        }
    }
}
