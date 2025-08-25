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

package org.apache.fluss.rpc.entity;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.rpc.protocol.Errors;

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
