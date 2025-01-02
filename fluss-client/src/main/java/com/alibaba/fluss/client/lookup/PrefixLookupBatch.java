/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.client.lookup;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.metadata.TableBucket;

import java.util.ArrayList;
import java.util.List;

/**
 * A batch that contains the prefix lookup operations that send to same destination and some table
 * together.
 */
@Internal
public class PrefixLookupBatch {

    /** The table bucket that the lookup operations should fall into. */
    private final TableBucket tableBucket;

    private final List<PrefixLookup> prefixLookups;

    public PrefixLookupBatch(TableBucket tableBucket) {
        this.tableBucket = tableBucket;
        this.prefixLookups = new ArrayList<>();
    }

    public void addLookup(PrefixLookup lookup) {
        prefixLookups.add(lookup);
    }

    public List<PrefixLookup> lookups() {
        return prefixLookups;
    }

    public TableBucket tableBucket() {
        return tableBucket;
    }

    public void complete(List<List<byte[]>> values) {
        if (values.size() != prefixLookups.size()) {
            completeExceptionally(
                    new FlussRuntimeException(
                            String.format(
                                    "The number of values return by prefix lookup request is not equal to the number of "
                                            + "prefix lookups send. Got %d values, but expected %d.",
                                    values.size(), prefixLookups.size())));
        } else {
            for (int i = 0; i < values.size(); i++) {
                AbstractLookup<List<byte[]>> lookup = prefixLookups.get(i);
                lookup.future().complete(values.get(i));
            }
        }
    }

    /** Complete the get operations with given exception. */
    public void completeExceptionally(Exception exception) {
        for (PrefixLookup prefixLookup : prefixLookups) {
            prefixLookup.future().completeExceptionally(exception);
        }
    }
}
