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
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/** Lookup operations encoded in one bucket entry of a lookup RPC. */
@Internal
public class LookupRequestBatch {

    private final LookupBatchKey lookupBatchKey;

    private final List<LookupQuery> lookups;

    LookupRequestBatch(LookupBatchKey lookupBatchKey) {
        this.lookupBatchKey = lookupBatchKey;
        this.lookups = new ArrayList<>();
    }

    /** Adds a lookup operation to this RPC bucket entry. */
    public void addLookup(LookupQuery lookup) {
        lookups.add(lookup);
    }

    /** Returns the lookup operations in this RPC bucket entry. */
    public List<LookupQuery> lookups() {
        return lookups;
    }

    /** Returns the table bucket targeted by this RPC bucket entry. */
    public TableBucket tableBucket() {
        return lookupBatchKey.tableBucket();
    }

    /** Returns the original partition name for a historical lookup, or null otherwise. */
    public @Nullable String originalPartitionName() {
        return lookupBatchKey.originalPartitionName();
    }

    LookupBatchKey lookupBatchKey() {
        return lookupBatchKey;
    }

    /** Complete the lookup operations using given values. */
    public void complete(List<byte[]> values) {
        if (values.size() != lookups.size()) {
            completeExceptionally(
                    new FlussRuntimeException(
                            String.format(
                                    "The number of return values of lookup operation is not equal to the number of "
                                            + "lookups. Return %d values, but expected %d.",
                                    values.size(), lookups.size())));
        } else {
            for (int i = 0; i < values.size(); i++) {
                AbstractLookupQuery<byte[]> lookup = lookups.get(i);
                lookup.future().complete(values.get(i));
            }
        }
    }

    /** Complete the lookup operations with given exception. */
    public void completeExceptionally(Exception exception) {
        for (LookupQuery lookup : lookups) {
            lookup.future().completeExceptionally(exception);
        }
    }
}
