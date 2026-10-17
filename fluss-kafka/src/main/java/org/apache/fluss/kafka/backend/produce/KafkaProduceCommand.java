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

import javax.annotation.Nullable;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Protocol-independent write command used by the Kafka Produce backend. */
@Internal
public final class KafkaProduceCommand {

    private final short acks;
    private final int timeoutMs;
    private final List<TopicWrite> topics;
    private final String listenerName;
    private final @Nullable InetAddress clientAddress;

    /** Creates a Kafka write command. */
    public KafkaProduceCommand(
            short acks,
            int timeoutMs,
            List<TopicWrite> topics,
            String listenerName,
            @Nullable InetAddress clientAddress) {
        this.acks = acks;
        this.timeoutMs = timeoutMs;
        this.topics = immutableCopy(topics);
        this.listenerName = checkNotNull(listenerName);
        this.clientAddress = clientAddress;
    }

    /** Returns Kafka required acknowledgements. */
    public short acks() {
        return acks;
    }

    /** Returns the Produce timeout in milliseconds. */
    public int timeoutMs() {
        return timeoutMs;
    }

    /** Returns the topic writes in request order. */
    public List<TopicWrite> topics() {
        return topics;
    }

    /** Returns the listener that received the request. */
    public String listenerName() {
        return listenerName;
    }

    /** Returns the client network address when available. */
    public @Nullable InetAddress clientAddress() {
        return clientAddress;
    }

    private static <T> List<T> immutableCopy(List<T> values) {
        return Collections.unmodifiableList(new ArrayList<>(checkNotNull(values)));
    }

    /** Records addressed to one Kafka topic. */
    @Internal
    public static final class TopicWrite {
        private final String topicName;
        private final List<PartitionWrite> partitions;

        /** Creates the writes for one topic. */
        public TopicWrite(String topicName, List<PartitionWrite> partitions) {
            this.topicName = checkNotNull(topicName);
            this.partitions = immutableCopy(partitions);
        }

        /** Returns the original request topic name, including any default-database alias. */
        public String topicName() {
            return topicName;
        }

        /** Returns partition writes in request order. */
        public List<PartitionWrite> partitions() {
            return partitions;
        }
    }

    /** Records addressed to one Kafka partition. */
    @Internal
    public static final class PartitionWrite {
        private final int partitionId;
        private final List<Record> records;

        /** Creates the writes for one partition. */
        public PartitionWrite(int partitionId, List<Record> records) {
            this.partitionId = partitionId;
            this.records = immutableCopy(records);
        }

        /** Returns the Kafka partition ID. */
        public int partitionId() {
            return partitionId;
        }

        /** Returns copied records in append order. */
        public List<Record> records() {
            return records;
        }
    }

    /**
     * Timestamp provenance supplied by a batch, independent of whether a record has a timestamp.
     */
    @Internal
    public enum TimestampType {
        /** No batch timestamp type was supplied, including by the legacy record constructor. */
        UNKNOWN,
        /** The timestamp was assigned when the record was created. */
        CREATE_TIME,
        /** The timestamp was assigned when the batch was appended to a log. */
        LOG_APPEND_TIME
    }

    /** A copied Kafka record whose lifetime is independent of the network request buffer. */
    @Internal
    public static final class Record {
        /** Sentinel used when the input record has no timestamp. */
        public static final long NO_TIMESTAMP = -1L;

        private final long timestamp;
        private final TimestampType timestampType;
        private final @Nullable byte[] key;
        private final @Nullable byte[] value;
        private final List<RecordHeader> headers;

        /**
         * Creates a copied record without batch timestamp provenance.
         *
         * <p>The type is {@link TimestampType#UNKNOWN} even when a timestamp is present. Callers
         * that know the batch type should use the explicit constructor instead of inferring it from
         * the timestamp value.
         */
        public Record(
                long timestamp,
                @Nullable byte[] key,
                @Nullable byte[] value,
                List<RecordHeader> headers) {
            this(timestamp, TimestampType.UNKNOWN, key, value, headers);
        }

        /** Creates a copied record with the timestamp type supplied by its source batch. */
        public Record(
                long timestamp,
                TimestampType timestampType,
                @Nullable byte[] key,
                @Nullable byte[] value,
                List<RecordHeader> headers) {
            this.timestamp = timestamp;
            this.timestampType = checkNotNull(timestampType);
            this.key = copyNullable(key);
            this.value = copyNullable(value);
            this.headers = immutableCopy(headers);
        }

        /** Returns the record timestamp in milliseconds, or {@link #NO_TIMESTAMP} when absent. */
        public long timestamp() {
            return timestamp;
        }

        /** Returns whether the input record has a timestamp, independently of its batch type. */
        public boolean hasTimestamp() {
            return timestamp != NO_TIMESTAMP;
        }

        /** Returns the source batch's timestamp type, never inferred from the timestamp value. */
        public TimestampType timestampType() {
            return timestampType;
        }

        /** Returns a copy of the nullable Kafka record key. */
        public @Nullable byte[] key() {
            return copyNullable(key);
        }

        /** Returns a copy of the nullable Kafka record value. */
        public @Nullable byte[] value() {
            return copyNullable(value);
        }

        /** Returns the copied Kafka headers in record order. */
        public List<RecordHeader> headers() {
            return headers;
        }

        private static @Nullable byte[] copyNullable(@Nullable byte[] value) {
            return value == null ? null : value.clone();
        }
    }

    /** A copied Kafka record header. */
    @Internal
    public static final class RecordHeader {
        private final String name;
        private final @Nullable byte[] value;

        /** Creates a copied Kafka record header. */
        public RecordHeader(String name, @Nullable byte[] value) {
            this.name = checkNotNull(name);
            this.value = value == null ? null : value.clone();
        }

        /** Returns the header name. */
        public String name() {
            return name;
        }

        /** Returns a copy of the nullable header value. */
        public @Nullable byte[] value() {
            return value == null ? null : value.clone();
        }
    }
}
