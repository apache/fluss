/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.client.admin;

import org.apache.fluss.annotation.PublicEvolving;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Per-group result of deleting consumer groups. */
@PublicEvolving
public final class DeleteConsumerGroupsResult {
    private final Map<String, CompletableFuture<Void>> futures;

    public DeleteConsumerGroupsResult(Map<String, CompletableFuture<Void>> futures) {
        this.futures =
                Collections.unmodifiableMap(
                        new HashMap<>(Objects.requireNonNull(futures, "futures")));
    }

    public CompletableFuture<Void> deletedGroup(String groupId) {
        return futures.get(groupId);
    }

    public Map<String, CompletableFuture<Void>> perGroupFutures() {
        return futures;
    }

    public CompletableFuture<Void> all() {
        return CompletableFuture.allOf(futures.values().toArray(new CompletableFuture[0]));
    }
}
