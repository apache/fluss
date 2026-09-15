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

package org.apache.fluss.kafka.backend.produce;

import org.apache.fluss.annotation.Internal;

import org.apache.kafka.common.protocol.Errors;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Result of a Kafka Produce backend invocation. */
@Internal
public final class KafkaProduceResult {
    private final List<TopicResult> topics;

    /** Creates a Produce result. */
    public KafkaProduceResult(List<TopicResult> topics) {
        this.topics = immutableCopy(topics);
    }

    /** Returns topic results in request order. */
    public List<TopicResult> topics() {
        return topics;
    }

    private static <T> List<T> immutableCopy(List<T> values) {
        return Collections.unmodifiableList(new ArrayList<>(checkNotNull(values)));
    }

    /** Results for one topic. */
    @Internal
    public static final class TopicResult {
        private final String topicName;
        private final List<PartitionResult> partitions;

        /** Creates the result for one topic. */
        public TopicResult(String topicName, List<PartitionResult> partitions) {
            this.topicName = checkNotNull(topicName);
            this.partitions = immutableCopy(partitions);
        }

        /** Returns the Kafka topic name. */
        public String topicName() {
            return topicName;
        }

        /** Returns the partition results in request order. */
        public List<PartitionResult> partitions() {
            return partitions;
        }
    }

    /** Result for one partition. */
    @Internal
    public static final class PartitionResult {
        private final int partitionId;
        private final Errors error;
        private final long baseOffset;
        private final @Nullable String errorMessage;

        /** Creates the result for one partition. */
        public PartitionResult(
                int partitionId, Errors error, long baseOffset, @Nullable String errorMessage) {
            this.partitionId = partitionId;
            this.error = checkNotNull(error);
            this.baseOffset = baseOffset;
            this.errorMessage = errorMessage;
        }

        /** Returns the Kafka partition ID. */
        public int partitionId() {
            return partitionId;
        }

        /** Returns the Kafka protocol error. */
        public Errors error() {
            return error;
        }

        /** Returns the first appended offset, or {@code -1} on failure. */
        public long baseOffset() {
            return baseOffset;
        }

        /** Returns an optional diagnostic error message. */
        public @Nullable String errorMessage() {
            return errorMessage;
        }
    }
}
