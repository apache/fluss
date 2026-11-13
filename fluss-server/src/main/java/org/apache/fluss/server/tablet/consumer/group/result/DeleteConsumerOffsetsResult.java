/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.tablet.consumer.group.result;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.protocol.Errors;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Result of deleting committed offsets for selected table buckets. */
@Internal
public final class DeleteConsumerOffsetsResult {

    private final Errors error;
    private final Map<TableBucket, Errors> bucketErrors;

    private DeleteConsumerOffsetsResult(Errors error, Map<TableBucket, Errors> bucketErrors) {
        this.error = checkNotNull(error, "error must not be null");
        this.bucketErrors =
                Collections.unmodifiableMap(
                        new LinkedHashMap<>(
                                checkNotNull(bucketErrors, "bucketErrors must not be null")));
    }

    public static DeleteConsumerOffsetsResult success(Map<TableBucket, Errors> bucketErrors) {
        return new DeleteConsumerOffsetsResult(Errors.NONE, bucketErrors);
    }

    public static DeleteConsumerOffsetsResult failure(Errors error) {
        return new DeleteConsumerOffsetsResult(error, Collections.emptyMap());
    }

    public Errors getError() {
        return error;
    }

    public Map<TableBucket, Errors> getBucketErrors() {
        return bucketErrors;
    }
}
