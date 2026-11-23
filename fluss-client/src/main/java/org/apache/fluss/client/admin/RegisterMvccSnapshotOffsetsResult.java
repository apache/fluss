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
import org.apache.fluss.exception.ApiException;
import org.apache.fluss.metadata.TableBucket;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Completed outcomes of a batch registration. */
@PublicEvolving
public final class RegisterMvccSnapshotOffsetsResult {
    private final Set<TableBucket> registeredBuckets;
    private final Set<TableBucket> conflictingBuckets;
    private final Map<TableBucket, ApiException> errors;

    /** Creates a result with immutable, mutually exclusive outcomes. */
    public RegisterMvccSnapshotOffsetsResult(
            Set<TableBucket> registeredBuckets,
            Set<TableBucket> conflictingBuckets,
            Map<TableBucket, ApiException> errors) {
        this.registeredBuckets = Collections.unmodifiableSet(new HashSet<>(registeredBuckets));
        this.conflictingBuckets = Collections.unmodifiableSet(new HashSet<>(conflictingBuckets));
        this.errors = Collections.unmodifiableMap(new HashMap<>(errors));
        Set<TableBucket> buckets = new HashSet<>(registeredBuckets);
        buckets.forEach(Objects::requireNonNull);
        for (TableBucket bucket : this.conflictingBuckets) {
            if (!buckets.add(Objects.requireNonNull(bucket))) {
                throw new IllegalArgumentException("Duplicate outcome for bucket " + bucket);
            }
        }
        this.errors.forEach(
                (bucket, error) -> {
                    Objects.requireNonNull(error);
                    if (!buckets.add(Objects.requireNonNull(bucket))) {
                        throw new IllegalArgumentException(
                                "Duplicate outcome for bucket " + bucket);
                    }
                });
    }

    /** Returns successful outcomes. */
    public Set<TableBucket> getRegisteredBuckets() {
        return registeredBuckets;
    }

    /** Returns buckets that were in conflict. */
    public Set<TableBucket> getConflictingBuckets() {
        return conflictingBuckets;
    }

    /** Returns failures keyed by physical bucket. */
    public Map<TableBucket, ApiException> getErrors() {
        return errors;
    }
}
