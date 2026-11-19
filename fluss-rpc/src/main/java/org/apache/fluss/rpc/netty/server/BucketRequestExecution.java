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

package org.apache.fluss.rpc.netty.server;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.messages.ApiMessage;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/** Local steps and response assembly for one accepted RPC. */
public interface BucketRequestExecution {
    /** Returns the ordered buckets validated during request preparation. */
    List<TableBucket> buckets();

    /** Returns whether all steps are pure reads. */
    default boolean isRead() {
        return false;
    }

    /** Executes one local bucket operation and records its result. */
    void execute(TableBucket bucket);

    /** Copies remaining borrowed inputs before the worker releases the original request. */
    void detachInputs();

    /** Returns whether the bucket still has an input to execute. */
    boolean hasInput(TableBucket bucket);

    /** Completes the original response after every bucket has a local outcome. */
    CompletableFuture<? extends ApiMessage> finish();
}
