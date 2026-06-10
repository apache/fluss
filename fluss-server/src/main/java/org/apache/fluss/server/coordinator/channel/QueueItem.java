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

package org.apache.fluss.server.coordinator.channel;

import org.apache.fluss.metadata.TableBucketReplica;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.protocol.ApiKeys;

import javax.annotation.Nullable;

import java.util.Set;
import java.util.function.BiConsumer;

/** An immutable item placed on the per-tablet-server sender queue. */
public final class QueueItem {

    private final ApiKeys apiKey;
    private final ApiMessage request;
    @Nullable private final BiConsumer<ApiMessage, Throwable> callback;
    private final int coordinatorEpoch;
    private final long enqueueTimeMs;
    @Nullable private final Set<TableBucketReplica> deletionReplicas;

    @SuppressWarnings("unchecked")
    public QueueItem(
            ApiKeys apiKey,
            ApiMessage request,
            @Nullable BiConsumer<? extends ApiMessage, ? super Throwable> callback,
            int coordinatorEpoch,
            long enqueueTimeMs,
            @Nullable Set<TableBucketReplica> deletionReplicas) {
        this.apiKey = apiKey;
        this.request = request;
        this.callback = (BiConsumer<ApiMessage, Throwable>) callback;
        this.coordinatorEpoch = coordinatorEpoch;
        this.enqueueTimeMs = enqueueTimeMs;
        this.deletionReplicas = deletionReplicas;
    }

    public ApiKeys getApiKey() {
        return apiKey;
    }

    public ApiMessage getRequest() {
        return request;
    }

    @Nullable
    public BiConsumer<ApiMessage, Throwable> getCallback() {
        return callback;
    }

    public int getCoordinatorEpoch() {
        return coordinatorEpoch;
    }

    public long getEnqueueTimeMs() {
        return enqueueTimeMs;
    }
}
