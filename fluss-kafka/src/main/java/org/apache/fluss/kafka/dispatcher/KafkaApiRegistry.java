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

package org.apache.fluss.kafka.dispatcher;

import org.apache.fluss.annotation.Internal;

import org.apache.kafka.common.protocol.ApiKeys;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Immutable registry and single source of truth for Kafka APIs exposed by one server. */
@Internal
public final class KafkaApiRegistry {

    private final Map<ApiKeys, KafkaApiHandler<?>> handlers;

    /**
     * Creates a registry from a snapshot of the supplied handlers, rejecting duplicate API keys.
     */
    public KafkaApiRegistry(Collection<? extends KafkaApiHandler<?>> handlers) {
        Map<ApiKeys, KafkaApiHandler<?>> registeredHandlers = new HashMap<>();
        for (KafkaApiHandler<?> handler : checkNotNull(handlers)) {
            ApiKeys apiKey = checkNotNull(handler).apiSpec().apiKey();
            checkArgument(
                    !registeredHandlers.containsKey(apiKey),
                    "Kafka API %s is already registered.",
                    apiKey);
            registeredHandlers.put(apiKey, handler);
        }
        this.handlers = Collections.unmodifiableMap(registeredHandlers);
    }

    /** Returns a routable handler, or {@code null} when the API is not exposed by this server. */
    public KafkaApiHandler<?> lookup(ApiKeys apiKey) {
        KafkaApiHandler<?> handler = handlers.get(apiKey);
        if (handler == null || !handler.apiSpec().advertised()) {
            return null;
        }
        return handler;
    }

    /** Returns the sorted API specifications advertised by this server. */
    public List<KafkaApiSpec> advertisedApiSpecs() {
        List<KafkaApiSpec> specs = new ArrayList<>();
        for (KafkaApiHandler<?> handler : handlers.values()) {
            KafkaApiSpec spec = handler.apiSpec();
            if (spec.advertised()) {
                specs.add(spec);
            }
        }
        Collections.sort(specs, Comparator.comparingInt(spec -> spec.apiKey().id));
        return Collections.unmodifiableList(specs);
    }
}
