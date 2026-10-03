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

import org.apache.fluss.metadata.TableBucket;

import java.util.Objects;

/** Identifies lookup operations that can share one queue and one RPC request. */
final class LookupQueueKey {
    private final TableBucket tableBucket;
    private final LookupType lookupType;
    private final boolean historical;

    private LookupQueueKey(TableBucket tableBucket, LookupType lookupType, boolean historical) {
        this.tableBucket = tableBucket;
        this.lookupType = lookupType;
        this.historical = historical;
    }

    static LookupQueueKey of(TableBucket tableBucket, LookupType lookupType, boolean historical) {
        return new LookupQueueKey(tableBucket, lookupType, historical);
    }

    static LookupQueueKey fromLookup(AbstractLookupQuery<?> lookup) {
        return of(
                lookup.tableBucket(), lookup.lookupType(), lookup.originalPartitionName() != null);
    }

    TableBucket tableBucket() {
        return tableBucket;
    }

    LookupType lookupType() {
        return lookupType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LookupQueueKey)) {
            return false;
        }
        LookupQueueKey that = (LookupQueueKey) o;
        return historical == that.historical
                && tableBucket.equals(that.tableBucket)
                && lookupType == that.lookupType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableBucket, lookupType, historical);
    }

    @Override
    public String toString() {
        return "LookupQueueKey{"
                + "tableBucket="
                + tableBucket
                + ", lookupType="
                + lookupType
                + ", historical="
                + historical
                + '}';
    }
}
