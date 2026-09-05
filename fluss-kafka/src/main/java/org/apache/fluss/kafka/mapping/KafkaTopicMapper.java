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

package org.apache.fluss.kafka.mapping;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.TablePath;

import org.apache.kafka.common.Uuid;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Maps Kafka topic identities to tables in the configured Fluss Kafka database. */
@Internal
public final class KafkaTopicMapper {

    // ASCII "Fluss" followed by zero bytes. A dedicated namespace avoids Kafka-reserved UUIDs.
    private static final long TOPIC_ID_NAMESPACE = 0x466c757373000000L;

    private final String databaseName;

    /** Creates a topic mapper for one Fluss database. */
    public KafkaTopicMapper(String databaseName) {
        this.databaseName = checkNotNull(databaseName);
    }

    /** Maps a Kafka topic name to its Fluss table path. */
    public TablePath toTablePath(String topicName) {
        return TablePath.of(databaseName, topicName);
    }

    /** Maps a Fluss table ID to a stable Kafka topic ID. */
    public Uuid toTopicId(long tableId) {
        checkArgument(tableId >= 0, "Table ID must be non-negative, but was %s.", tableId);
        return new Uuid(TOPIC_ID_NAMESPACE, tableId);
    }

    /** Returns whether a Kafka topic ID can represent a Fluss table ID. */
    public boolean isMappedTopicId(Uuid topicId) {
        return topicId != null
                && topicId.getMostSignificantBits() == TOPIC_ID_NAMESPACE
                && topicId.getLeastSignificantBits() >= 0L;
    }

    /** Extracts the Fluss table ID encoded in a Kafka topic ID. */
    public long toTableId(Uuid topicId) {
        checkArgument(isMappedTopicId(topicId), "Topic ID %s is not a Fluss topic ID.", topicId);
        return topicId.getLeastSignificantBits();
    }
}
