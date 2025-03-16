/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.client.admin;

import com.alibaba.fluss.annotation.PublicEvolving;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Result of list offsets request.
 *
 * @since 0.2
 */
@PublicEvolving
public class ListOffsetsResult {
    private final Map<Integer, CompletableFuture<Long>> futures;

    public ListOffsetsResult(Map<Integer, CompletableFuture<Long>> futures) {
        this.futures = futures;
    }

    public CompletableFuture<Long> bucketResult(int bucket) {
        return futures.get(bucket);
    }

    public CompletableFuture<Map<Integer, Long>> all() {
        return CompletableFuture.allOf(futures.values().toArray(new CompletableFuture[0]))
                .thenApply(
                        v ->
                                futures.entrySet().stream()
                                        .collect(
                                                Collectors.toMap(
                                                        Map.Entry::getKey,
                                                        e -> e.getValue().join())));
    }
}
