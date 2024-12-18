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
import com.alibaba.fluss.metadata.TableBucket;

import java.util.ArrayList;
import java.util.List;

/** An abstract lookup batch. */
@Internal
public abstract class AbstractLookupBatch<T> {

    protected final List<AbstractLookup<T>> lookups;
    private final TableBucket tableBucket;

    public AbstractLookupBatch(TableBucket tableBucket) {
        this.lookups = new ArrayList<>();
        this.tableBucket = tableBucket;
    }

    /** Complete the lookup operations using given values . */
    public abstract void complete(List<T> values);

    public void addLookup(AbstractLookup<T> lookup) {
        lookups.add(lookup);
    }

    public List<AbstractLookup<T>> lookups() {
        return lookups;
    }

    public TableBucket tableBucket() {
        return tableBucket;
    }

    /** Complete the get operations with given exception. */
    public void completeExceptionally(Exception exception) {
        for (AbstractLookup<T> lookup : lookups) {
            lookup.future().completeExceptionally(exception);
        }
    }
}
