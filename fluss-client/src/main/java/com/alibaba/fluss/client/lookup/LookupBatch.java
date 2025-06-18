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

package com.alibaba.fluss.client.lookup;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.metadata.TableBucket;

import java.util.ArrayList;
import java.util.List;

/** A batch that contains the lookup operations that send to same tablet bucket together. */
@Internal
public class LookupBatch {

    /** The table bucket that the lookup operations should fall into. */
    private final TableBucket tableBucket;

    private final List<LookupQuery> lookups;

    public LookupBatch(TableBucket tableBucket) {
        this.tableBucket = tableBucket;
        this.lookups = new ArrayList<>();
    }

    public void addLookup(LookupQuery lookup) {
        lookups.add(lookup);
    }

    public List<LookupQuery> lookups() {
        return lookups;
    }

    public TableBucket tableBucket() {
        return tableBucket;
    }

    /** Complete the lookup operations using given values . */
    public void complete(List<byte[]> values) {
        // if the size of return values of lookup operation are not equal to the number of lookups,
        // should complete an exception.
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
                // single value.
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
