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

package com.alibaba.fluss.rpc.entity;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.rpc.messages.ProduceLogRequest;
import com.alibaba.fluss.rpc.protocol.ApiError;
import com.alibaba.fluss.rpc.protocol.Errors;

/** Result of {@link ProduceLogRequest} for each table bucket. */
@Internal
public class ProduceLogResultForBucket extends WriteResultForBucket {
    private final long baseOffset;

    public ProduceLogResultForBucket(TableBucket tableBucket, long baseOffset, long endOffset) {
        this(tableBucket, baseOffset, endOffset, ApiError.NONE);
    }

    public ProduceLogResultForBucket(TableBucket tableBucket, ApiError error) {
        this(tableBucket, -1L, -1L, error);
    }

    private ProduceLogResultForBucket(
            TableBucket tableBucket, long baseOffset, long endOffset, ApiError error) {
        super(tableBucket, endOffset, error);
        this.baseOffset = baseOffset;
    }

    public long getBaseOffset() {
        return baseOffset;
    }

    @Override
    public <T extends WriteResultForBucket> T copy(Errors newError) {
        //noinspection unchecked
        return (T)
                new ProduceLogResultForBucket(
                        tableBucket, baseOffset, getWriteLogEndOffset(), newError.toApiError());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        ProduceLogResultForBucket that = (ProduceLogResultForBucket) o;
        return baseOffset == that.baseOffset;
    }
}
