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

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.rpc.messages.PutKvRequest;
import com.alibaba.fluss.rpc.protocol.ApiError;
import com.alibaba.fluss.rpc.protocol.Errors;

/** Result of {@link PutKvRequest} for each table bucket. */
@Internal
public class PutKvResultForBucket extends WriteResultForBucket {
    public PutKvResultForBucket(TableBucket tableBucket, long changeLogEndOffset) {
        super(tableBucket, changeLogEndOffset, ApiError.NONE);
    }

    public PutKvResultForBucket(TableBucket tableBucket, ApiError error) {
        super(tableBucket, -1L, error);
    }

    @Override
    public <T extends WriteResultForBucket> T copy(Errors newError) {
        //noinspection unchecked
        return (T) new PutKvResultForBucket(tableBucket, newError.toApiError());
    }
}
