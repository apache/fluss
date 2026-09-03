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

import org.apache.kafka.common.Uuid;

import javax.annotation.Nullable;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Domain query used by the Metadata API to access the Fluss adapter layer. */
@Internal
public final class KafkaMetadataQuery {

    private final boolean allTopics;
    private final List<TopicReference> topics;
    private final String listenerName;
    private final @Nullable InetAddress clientAddress;

    /** Creates a metadata query. */
    public KafkaMetadataQuery(
            boolean allTopics,
            List<TopicReference> topics,
            String listenerName,
            @Nullable InetAddress clientAddress) {
        this.allTopics = allTopics;
        this.topics = Collections.unmodifiableList(new ArrayList<>(checkNotNull(topics)));
        this.listenerName = checkNotNull(listenerName);
        this.clientAddress = clientAddress;
    }

    /** Returns whether all Kafka topics should be returned. */
    public boolean allTopics() {
        return allTopics;
    }

    /** Returns the explicitly requested topic identities. */
    public List<TopicReference> topics() {
        return topics;
    }

    /** Returns the Kafka listener used by the client connection. */
    public String listenerName() {
        return listenerName;
    }

    /** Returns the client address when it is available. */
    public @Nullable InetAddress clientAddress() {
        return clientAddress;
    }

    /** Kafka topic name and ID supplied by a Metadata request. */
    @Internal
    public static final class TopicReference {

        private final @Nullable String topicName;
        private final Uuid topicId;

        /** Creates a topic reference. */
        public TopicReference(@Nullable String topicName, Uuid topicId) {
            this.topicName = topicName;
            this.topicId = checkNotNull(topicId);
        }

        /** Returns the requested topic name, if present. */
        public @Nullable String topicName() {
            return topicName;
        }

        /** Returns the requested topic ID, or {@link Uuid#ZERO_UUID} when absent. */
        public Uuid topicId() {
            return topicId;
        }

        /** Returns whether this reference identifies a topic by ID. */
        public boolean hasTopicId() {
            return !Uuid.ZERO_UUID.equals(topicId);
        }
    }
}
