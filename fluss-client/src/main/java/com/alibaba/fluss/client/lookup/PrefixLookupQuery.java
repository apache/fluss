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

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Class to represent a prefix lookup operation, it contains the table id, bucketNums and related
 * CompletableFuture.
 */
@Internal
public class PrefixLookupQuery extends AbstractLookupQuery<List<byte[]>> {
    private final CompletableFuture<List<byte[]>> future;

    PrefixLookupQuery(TableBucket tableBucket, byte[] prefixKey) {
        super(tableBucket, prefixKey);
        this.future = new CompletableFuture<>();
    }

    @Override
    public CompletableFuture<List<byte[]>> future() {
        return future;
    }

    @Override
    public LookupType lookupType() {
        return LookupType.PREFIX_LOOKUP;
    }
}
