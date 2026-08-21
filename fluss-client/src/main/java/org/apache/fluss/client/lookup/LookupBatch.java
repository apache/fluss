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

package org.apache.fluss.client.lookup;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.TableBucket;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** A batch of lookup operations accumulated for the same table bucket and lookup kind. */
@Internal
final class LookupBatch {

    private final TableBucket tableBucket;
    private final LookupType lookupType;
    private final boolean historical;
    private final long createdNanos;
    private final List<AbstractLookupQuery<?>> lookups;

    private boolean completed;

    LookupBatch(AbstractLookupQuery<?> firstLookup, long createdNanos) {
        this.tableBucket = firstLookup.tableBucket();
        this.lookupType = firstLookup.lookupType();
        this.historical = firstLookup.originalPartitionName() != null;
        this.createdNanos = createdNanos;
        this.lookups = new ArrayList<>();
        this.lookups.add(firstLookup);
    }

    void addLookup(AbstractLookupQuery<?> lookup) {
        lookups.add(lookup);
    }

    TableBucket tableBucket() {
        return tableBucket;
    }

    LookupType lookupType() {
        return lookupType;
    }

    boolean historical() {
        return historical;
    }

    List<AbstractLookupQuery<?>> lookups() {
        return Collections.unmodifiableList(lookups);
    }

    int size() {
        return lookups.size();
    }

    long waitedNanos(long nowNanos) {
        return nowNanos - createdNanos;
    }

    boolean markCompleted() {
        if (completed) {
            return false;
        }
        completed = true;
        return true;
    }
}
