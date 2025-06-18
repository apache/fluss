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
import com.alibaba.fluss.metadata.TableBucket;

import java.util.concurrent.CompletableFuture;

/**
 * Class to represent a Lookup operation, it contains the table bucket that the key should lookup
 * from, the bytes of the key, and a future for the lookup operation.
 */
@Internal
public class LookupQuery extends AbstractLookupQuery<byte[]> {

    private final CompletableFuture<byte[]> future;

    LookupQuery(TableBucket tableBucket, byte[] key) {
        super(tableBucket, key);
        this.future = new CompletableFuture<>();
    }

    @Override
    public LookupType lookupType() {
        return LookupType.LOOKUP;
    }

    @Override
    public CompletableFuture<byte[]> future() {
        return future;
    }
}
