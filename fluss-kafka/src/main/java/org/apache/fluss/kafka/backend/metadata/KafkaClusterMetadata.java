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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Kafka-domain cluster metadata returned by a Fluss metadata backend. */
@Internal
public final class KafkaClusterMetadata {

    private final List<Broker> brokers;
    private final List<Topic> topics;

    /** Creates cluster metadata. */
    public KafkaClusterMetadata(List<Broker> brokers, List<Topic> topics) {
        this.brokers = immutableCopy(brokers);
        this.topics = immutableCopy(topics);
    }

    /** Returns Kafka-reachable brokers. */
    public List<Broker> brokers() {
        return brokers;
    }

    /** Returns topic metadata and topic-level errors. */
    public List<Topic> topics() {
        return topics;
    }

    private static <T> List<T> immutableCopy(List<T> values) {
        return Collections.unmodifiableList(new ArrayList<>(checkNotNull(values)));
    }

    /** Kafka-reachable broker information. */
    @Internal
    public static final class Broker {

        private final int id;
        private final String host;
        private final int port;
        private final @Nullable String rack;

        /** Creates broker information. */
        public Broker(int id, String host, int port, @Nullable String rack) {
            this.id = id;
            this.host = checkNotNull(host);
            this.port = port;
            this.rack = rack;
        }

        /** Returns the Kafka broker ID. */
        public int id() {
            return id;
        }

        /** Returns the Kafka listener host. */
        public String host() {
            return host;
        }

        /** Returns the Kafka listener port. */
        public int port() {
            return port;
        }

        /** Returns the broker rack, if configured. */
        public @Nullable String rack() {
            return rack;
        }
    }

    /** Topic-level error independent of a Kafka response schema version. */
    @Internal
    public enum TopicError {
        NONE,
        UNKNOWN_TOPIC_OR_PARTITION,
        UNKNOWN_TOPIC_ID,
        INVALID_TOPIC
    }

    /** Metadata for one Kafka topic. */
    @Internal
    public static final class Topic {

        private final @Nullable String name;
        private final Uuid topicId;
        private final TopicError error;
        private final List<Partition> partitions;

        /** Creates topic metadata. */
        public Topic(
                @Nullable String name, Uuid topicId, TopicError error, List<Partition> partitions) {
            this.name = name;
            this.topicId = checkNotNull(topicId);
            this.error = checkNotNull(error);
            this.partitions = immutableCopy(partitions);
        }

        /** Returns the Kafka topic name, if known. */
        public @Nullable String name() {
            return name;
        }

        /** Returns the stable Kafka topic ID. */
        public Uuid topicId() {
            return topicId;
        }

        /** Returns the topic-level domain error. */
        public TopicError error() {
            return error;
        }

        /** Returns the topic partitions. */
        public List<Partition> partitions() {
            return partitions;
        }
    }

    /** Metadata for one Kafka partition backed by a Fluss bucket. */
    @Internal
    public static final class Partition {

        private final int partitionId;
        private final int leaderId;
        private final int leaderEpoch;
        private final List<Integer> replicas;
        private final List<Integer> isr;
        private final List<Integer> offlineReplicas;
        private final boolean leaderAvailable;

        /** Creates partition metadata. */
        public Partition(
                int partitionId,
                int leaderId,
                int leaderEpoch,
                List<Integer> replicas,
                List<Integer> isr,
                List<Integer> offlineReplicas,
                boolean leaderAvailable) {
            this.partitionId = partitionId;
            this.leaderId = leaderId;
            this.leaderEpoch = leaderEpoch;
            this.replicas = immutableCopy(replicas);
            this.isr = immutableCopy(isr);
            this.offlineReplicas = immutableCopy(offlineReplicas);
            this.leaderAvailable = leaderAvailable;
        }

        /** Returns the Kafka partition ID. */
        public int partitionId() {
            return partitionId;
        }

        /** Returns the current leader ID, or {@code -1} when unavailable. */
        public int leaderId() {
            return leaderId;
        }

        /** Returns the leader epoch, or {@code -1} when unavailable. */
        public int leaderEpoch() {
            return leaderEpoch;
        }

        /** Returns assigned replica IDs. */
        public List<Integer> replicas() {
            return replicas;
        }

        /** Returns replica IDs currently visible as in-sync. */
        public List<Integer> isr() {
            return isr;
        }

        /** Returns assigned replicas whose TabletServers are unavailable. */
        public List<Integer> offlineReplicas() {
            return offlineReplicas;
        }

        /** Returns whether the partition has a reachable leader. */
        public boolean leaderAvailable() {
            return leaderAvailable;
        }
    }
}
