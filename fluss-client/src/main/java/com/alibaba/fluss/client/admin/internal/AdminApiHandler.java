/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.client.admin.internal;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * A handler to lookup server id for keys and then send request to these servers.
 *
 * @param <K>
 * @param <V>
 */
public interface AdminApiHandler<K, V> {

    /**
     * Get the lookup strategy that is responsible for finding the server id which will handle each
     * respective key.
     *
     * @return non-null lookup strategy
     */
    TabletServerLookupStrategy<K> lookupStrategy();

    /**
     * Handle the given keys by sending the respective requests to the given serverId. The handler
     * should parse the response, check for errors, and return a result which indicates which keys
     * (if any) have either been completed or failed with an unrecoverable error. It is also
     * possible that the response indicates an incorrect target serverId (e. g. in the case of a
     * NotLeader error when the request is bound for a partition leader). In this case the key will
     * be "unmapped" from the target serverId and lookup will be retried. Note that keys which
     * received a retriable error should be left out of the result. They will be retried
     * automatically.
     *
     * @param node the node to send the requests to
     * @param keys the keys to handle
     * @return result indicating key completion, failure, and unmapping
     */
    CompletableFuture<ApiResult<K, V>> handle(int node, Set<K> keys);

    class ApiResult<K, V> {
        public final Map<K, V> completedKeys;
        public final Map<K, Throwable> failedKeys;
        public final Set<K> unmappedKeys;

        public ApiResult(
                Map<K, V> completedKeys, Map<K, Throwable> failedKeys, Set<K> unmappedKeys) {
            this.completedKeys = Collections.unmodifiableMap(completedKeys);
            this.failedKeys = Collections.unmodifiableMap(failedKeys);
            this.unmappedKeys = Collections.unmodifiableSet(unmappedKeys);
        }
    }
}
