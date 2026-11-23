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

package org.apache.fluss.client.admin;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.metadata.MvccReadBounds;
import org.apache.fluss.metadata.TableBucket;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Independently completing results of a batch query for MVCC read bounds. */
@PublicEvolving
public final class ListMvccReadBoundsResult {
    private final Map<TableBucket, CompletableFuture<MvccReadBounds>> futures;

    /** Creates a result from a copy of the supplied bucket-to-future mapping. */
    public ListMvccReadBoundsResult(Map<TableBucket, CompletableFuture<MvccReadBounds>> futures) {
        Map<TableBucket, CompletableFuture<MvccReadBounds>> copy = new HashMap<>();
        futures.forEach(
                (bucket, future) ->
                        copy.put(Objects.requireNonNull(bucket), Objects.requireNonNull(future)));
        this.futures = Collections.unmodifiableMap(copy);
    }

    /** Returns the future for a requested bucket, or null for an unknown bucket. */
    public CompletableFuture<MvccReadBounds> bucketResult(TableBucket bucket) {
        return futures.get(bucket);
    }

    /** Returns all bounds, or completes exceptionally if any bucket fails. */
    public CompletableFuture<Map<TableBucket, MvccReadBounds>> all() {
        return CompletableFuture.allOf(futures.values().toArray(new CompletableFuture<?>[0]))
                .thenApply(
                        ignored -> {
                            Map<TableBucket, MvccReadBounds> bounds = new HashMap<>();
                            futures.forEach((bucket, future) -> bounds.put(bucket, future.join()));
                            return Collections.unmodifiableMap(bounds);
                        });
    }
}
