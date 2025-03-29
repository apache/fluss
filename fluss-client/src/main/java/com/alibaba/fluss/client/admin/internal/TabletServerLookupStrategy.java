/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
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
 * Defines an interface for a strategy to lookup tablet servers. This interface specifies how to
 * find or select tablet servers for subsequent data processing or operations.
 *
 * @param <K> Represents the key type used in the lookup strategy. This allows different
 *     implementations to look up tablet servers based on different key types.
 */
public interface TabletServerLookupStrategy<K> {

    /**
     * Looks up the tablet server IDs for the given keys. The handler should parse the response,
     * check for errors, and return a result indicating which keys were successfully mapped to a
     * tablet server ID and which keys encountered a fatal error.
     *
     * <p>- Keys with retriable errors should be omitted from the result; they will be automatically
     * retried.
     *
     * <p>- For example, if the response of a `ListOffset` request indicates no leader is ready, the
     * key should be excluded from the result to allow for retry.
     *
     * @param keys the set of keys that require lookup
     * @return a CompletableFuture containing a result indicating which keys mapped successfully to
     *     a tablet server id and which encountered a fatal error
     */
    CompletableFuture<LookupServerResult<K>> lookup(Set<K> keys);

    class LookupServerResult<K> {

        // This is the set of keys that have been mapped to a specific server for
        // fulfillment of the API request.
        public final Map<K, Integer> mappedKeys;

        // This is the set of keys that have encountered a fatal error during the lookup
        // phase. The driver will not attempt lookup or fulfillment for failed keys.
        public final Map<K, Throwable> failedKeys;

        public LookupServerResult(Map<K, Integer> mappedKeys, Map<K, Throwable> failedKeys) {
            this.mappedKeys = Collections.unmodifiableMap(mappedKeys);
            this.failedKeys = Collections.unmodifiableMap(failedKeys);
        }
    }
}
