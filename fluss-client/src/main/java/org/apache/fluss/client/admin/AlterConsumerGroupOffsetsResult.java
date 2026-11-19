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
import org.apache.fluss.metadata.TableBucket;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Per-bucket result of altering committed consumer offsets. */
@PublicEvolving
public final class AlterConsumerGroupOffsetsResult {
    private final Map<TableBucket, CompletableFuture<Void>> futures;

    /** Creates a result from per-bucket futures. */
    public AlterConsumerGroupOffsetsResult(Map<TableBucket, CompletableFuture<Void>> futures) {
        this.futures =
                Collections.unmodifiableMap(
                        new HashMap<>(Objects.requireNonNull(futures, "futures")));
    }

    /** Returns the future for the given bucket, or {@code null} if it was not altered. */
    public CompletableFuture<Void> bucketResult(TableBucket tableBucket) {
        return futures.get(tableBucket);
    }

    /** Returns a future that completes when all bucket alterations succeed. */
    public CompletableFuture<Void> all() {
        return CompletableFuture.allOf(futures.values().toArray(new CompletableFuture[0]));
    }
}
