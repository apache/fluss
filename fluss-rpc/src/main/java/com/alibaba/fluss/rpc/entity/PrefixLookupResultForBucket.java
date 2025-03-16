/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.rpc.entity;

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.rpc.messages.PrefixLookupRequest;
import com.alibaba.fluss.rpc.protocol.ApiError;

import java.util.List;

/** The Result of {@link PrefixLookupRequest} for each table bucket. */
public class PrefixLookupResultForBucket extends ResultForBucket {

    private final List<List<byte[]>> values;

    public PrefixLookupResultForBucket(TableBucket tableBucket, List<List<byte[]>> values) {
        this(tableBucket, values, ApiError.NONE);
    }

    public PrefixLookupResultForBucket(TableBucket tableBucket, ApiError error) {
        this(tableBucket, null, error);
    }

    private PrefixLookupResultForBucket(
            TableBucket tableBucket, List<List<byte[]>> values, ApiError error) {
        super(tableBucket, error);
        this.values = values;
    }

    public List<List<byte[]>> prefixLookupValues() {
        return values;
    }
}
