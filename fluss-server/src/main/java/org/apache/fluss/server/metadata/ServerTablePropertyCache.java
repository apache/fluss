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

package org.apache.fluss.server.metadata;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Immutable cache of selected table properties, published with the routing snapshot.
 *
 * <p>For now each table retains only the boolean value of {@link
 * ConfigOptions#TABLE_KAFKA_ENABLED}. No property maps, strings, table descriptors or schemas are
 * retained. Reads never access ZK.
 */
@ThreadSafe
final class ServerTablePropertyCache {
    private final Map<Long, Boolean> kafkaEnabledByTableId;

    private ServerTablePropertyCache(Map<Long, Boolean> kafkaEnabledByTableId) {
        this.kafkaEnabledByTableId = Collections.unmodifiableMap(kafkaEnabledByTableId);
    }

    static ServerTablePropertyCache empty() {
        return new ServerTablePropertyCache(Collections.emptyMap());
    }

    @Nullable
    Boolean isKafkaEnabled(long tableId) {
        return kafkaEnabledByTableId.get(tableId);
    }

    Builder toBuilder() {
        return new Builder(this);
    }

    /** Accumulates updates before publishing the flag together with its routing metadata. */
    static final class Builder {
        private final Map<Long, Boolean> kafkaEnabledByTableId;

        private Builder(ServerTablePropertyCache cache) {
            kafkaEnabledByTableId = new HashMap<>(cache.kafkaEnabledByTableId);
        }

        void update(long tableId, Configuration properties) {
            // DDL validates this property. Historical invalid values must not enable Kafka or
            // prevent the remaining cluster metadata from being applied.
            kafkaEnabledByTableId.put(
                    tableId,
                    properties
                            .getRawValue(ConfigOptions.TABLE_KAFKA_ENABLED.key())
                            .map(value -> Boolean.parseBoolean(value.toString()))
                            .orElse(ConfigOptions.TABLE_KAFKA_ENABLED.defaultValue()));
        }

        void remove(long tableId) {
            kafkaEnabledByTableId.remove(tableId);
        }

        ServerTablePropertyCache build() {
            return new ServerTablePropertyCache(new HashMap<>(kafkaEnabledByTableId));
        }
    }
}
