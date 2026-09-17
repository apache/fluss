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
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.internals.Topic;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Maps fully qualified Kafka topic identities to Fluss tables. */
@Internal
public final class KafkaTopicMapper {

    // ASCII "Fluss" followed by zero bytes. A dedicated namespace avoids Kafka-reserved UUIDs.
    private static final long TOPIC_ID_NAMESPACE = 0x466c757373000000L;

    /** Maps a Kafka topic in database.table form to its Fluss table path. */
    public TablePath toTablePath(String topicName) {
        if (!isValidTopic(topicName)) {
            throw new InvalidTopicException(
                    "Kafka topic must be a valid database.table name: " + topicName);
        }
        int separator = topicName.indexOf('.');
        return TablePath.of(topicName.substring(0, separator), topicName.substring(separator + 1));
    }

    /** Returns the fully qualified Kafka name of a representable Fluss table. */
    public String toTopicName(TablePath tablePath) {
        if (!isMappedTable(tablePath)) {
            throw new InvalidTopicException(
                    "Fluss table cannot be represented as a Kafka database.table name: "
                            + tablePath);
        }
        return tablePath.toString();
    }

    /** Returns whether a Fluss user table has a valid, fully qualified Kafka topic name. */
    public boolean isMappedTable(TablePath tablePath) {
        return tablePath != null && tablePath.isValid() && isValidTopic(tablePath.toString());
    }

    /** Returns whether a name uniquely represents a user table in a Fluss database. */
    public static boolean isValidTopic(String topicName) {
        if (topicName == null || !Topic.isValid(topicName)) {
            return false;
        }
        int separator = topicName.indexOf('.');
        if (separator <= 0 || separator != topicName.lastIndexOf('.')) {
            return false;
        }
        String database = topicName.substring(0, separator);
        String table = topicName.substring(separator + 1);
        return TablePath.of(database, table).isValid()
                && TablePath.validatePrefix(database) == null
                && TablePath.validatePrefix(table) == null;
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
