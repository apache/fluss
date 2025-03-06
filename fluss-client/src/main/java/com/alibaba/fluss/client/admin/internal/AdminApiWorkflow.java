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

import com.alibaba.fluss.annotation.Internal;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * A workflow for multi-stage request workflows, such as those seen with the ListOffsets API or any
 * request that needs to be sent to a bucket leader. These APIs typically have two main stages:
 *
 * <p>1. **Lookup**: Find the tablet server that can fulfill the request (e.g., bucket leader or
 * coordinator).
 *
 * <p>2. **Fulfillment**: Send the request to the servers identified in the Lookup stage.
 *
 * <p>This process is complicated by the fact that `Admin` APIs are often batched. The Lookup stage
 * may result in a set of servers. For example, a `ListOffsets` request for multiple table buckets
 * will involve finding the bucket leader servers for these buckets in the Lookup stage. In the
 * Fulfillment stage, requests are grouped according to the IDs of the discovered leaders.
 *
 * <p>Additionally, the flow between these two stages is bidirectional. After sending a
 * `ListOffsets` request to an expected bucket leader sever, a leader change might be detected,
 * causing a table bucket to be sent back to the Lookup stage for re-evaluation.
 *
 * @param <K> The key type, which determines the granularity of request routing. For example, this
 *     could be `TableBucket` for requests intended for a bucket leader in`ListOffsets`
 * @param <V> The fulfillment type for each key. For example, this could be the offset for each
 *     bucket in`ListOffsets`.
 */
@Internal
public class AdminApiWorkflow<K, V> {
    private static final long RETRY_DELAY_MS = 100L;

    private final Set<K> lookupKeys;
    private final BiMultimap<Integer, K> fulfillmentMap;
    private final AdminApiFuture<K, V> future;
    private final AdminApiHandler<K, V> handler;

    public AdminApiWorkflow(AdminApiFuture<K, V> future, AdminApiHandler<K, V> handler) {
        this.future = future;
        this.handler = handler;
        this.lookupKeys = new HashSet<>();
        this.fulfillmentMap = new BiMultimap<>();
        retryLookup(future.lookupKeys());
    }

    public void maybeSendRequests() {
        if (!lookupKeys.isEmpty()) {
            handler.lookupStrategy()
                    .lookup(Collections.unmodifiableSet(lookupKeys))
                    .thenAcceptAsync(
                            lookupResult -> {
                                completeLookup(lookupResult.mappedKeys);
                                completeLookupExceptionally(lookupResult.failedKeys);
                                try {
                                    Thread.sleep(RETRY_DELAY_MS);
                                } catch (InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                                // maybe send requests again if not finished.
                                maybeSendRequests();
                            });
        }

        for (Map.Entry<Integer, Set<K>> entry : fulfillmentMap.entrySet()) {
            handler.handle(entry.getKey(), entry.getValue())
                    .thenAcceptAsync(
                            apiResult -> {
                                complete(apiResult.completedKeys);
                                completeExceptionally(apiResult.failedKeys);
                                retryLookup(apiResult.unmappedKeys);
                                // maybe send requests again if not finished.
                                maybeSendRequests();
                            });
        }
    }

    private void retryLookup(Collection<K> keys) {
        keys.forEach(this::unmap);
    }

    private void completeLookup(Map<K, Integer> nodeIdMapping) {
        if (!nodeIdMapping.isEmpty()) {
            future.completeLookup(nodeIdMapping);
            nodeIdMapping.forEach(this::map);
        }
    }

    private void completeLookupExceptionally(Map<K, Throwable> errors) {
        if (!errors.isEmpty()) {
            future.completeLookupExceptionally(errors);
            clear(errors.keySet());
        }
    }

    /**
     * Complete the future associated with the given key. After this is called, all keys will be
     * taken out of both the Lookup and Fulfillment stages so that request are not retried.
     */
    private void complete(Map<K, V> values) {
        if (!values.isEmpty()) {
            future.complete(values);
            clear(values.keySet());
        }
    }

    /**
     * Complete the future associated with the given key exceptionally. After is called, the key
     * will be taken out of both the Lookup and Fulfillment stages so that request are not retried.
     */
    private void completeExceptionally(Map<K, Throwable> errors) {
        if (!errors.isEmpty()) {
            future.completeExceptionally(errors);
            clear(errors.keySet());
        }
    }

    /**
     * Associate a key with a server id. This is called after a response in the Lookup stage reveals
     * the mapping.
     *
     * <p>For example, when handling a <code>listOffsets</code> request, this method would then
     * associate the <code>bucket</code> (as the key) with the <code>tablet server node</code> (as
     * the serverId).
     *
     * @param lookupKey The key to be associated, typically used to identify a specific resource or
     *     request. For instance, a bucket identifier.
     * @param serverId The identifier of the server, used to establish an association between the
     *     key and the server. For instance, the tablet server node identifier.
     */
    private void map(K lookupKey, Integer serverId) {
        lookupKeys.remove(lookupKey);
        fulfillmentMap.put(serverId, lookupKey);
    }

    /**
     * Disassociates a key from its currently mapped tablet server ID. This action sends the key
     * back to the Lookup stage, enabling another attempt at lookup.
     */
    private void unmap(K lookupKey) {
        fulfillmentMap.remove(lookupKey);
        lookupKeys.add(lookupKey);
    }

    private void clear(Collection<K> keys) {
        keys.forEach(
                key -> {
                    lookupKeys.remove(key);
                    fulfillmentMap.remove(key);
                });
    }

    private static class BiMultimap<K, V> {
        private final Map<V, K> reverseMap = new HashMap<>();
        private final Map<K, Set<V>> map = new HashMap<>();

        void put(K key, V value) {
            remove(value);
            reverseMap.put(value, key);
            map.computeIfAbsent(key, k -> new HashSet<>()).add(value);
        }

        void remove(V value) {
            K key = reverseMap.remove(value);
            if (key != null) {
                Set<V> set = map.get(key);
                if (set != null) {
                    set.remove(value);
                    if (set.isEmpty()) {
                        map.remove(key);
                    }
                }
            }
        }

        Set<Map.Entry<K, Set<V>>> entrySet() {
            return Collections.unmodifiableMap(map).entrySet();
        }
    }
}
