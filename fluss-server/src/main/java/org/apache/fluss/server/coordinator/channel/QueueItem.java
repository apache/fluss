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

import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.protocol.ApiKeys;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;

/** An immutable item placed on the per-tablet-server sender queue. */
public final class QueueItem<ResponseT extends ApiMessage> {

    private final ApiKeys apiKey;
    private final Function<TabletServerGateway, CompletableFuture<ResponseT>> requestSender;
    @Nullable private final BiConsumer<ResponseT, ? super Throwable> callback;
    private final int coordinatorEpoch;
    private final long enqueueTimeMs;

    public QueueItem(
            ApiKeys apiKey,
            Function<TabletServerGateway, CompletableFuture<ResponseT>> requestSender,
            @Nullable BiConsumer<ResponseT, ? super Throwable> callback,
            int coordinatorEpoch,
            long enqueueTimeMs) {
        this.apiKey = apiKey;
        this.requestSender = requestSender;
        this.callback = callback;
        this.coordinatorEpoch = coordinatorEpoch;
        this.enqueueTimeMs = enqueueTimeMs;
    }

    public ApiKeys getApiKey() {
        return apiKey;
    }

    public CompletableFuture<ResponseT> send(TabletServerGateway gateway) {
        return requestSender.apply(gateway);
    }

    @Nullable
    public BiConsumer<ResponseT, ? super Throwable> getCallback() {
        return callback;
    }

    public int getCoordinatorEpoch() {
        return coordinatorEpoch;
    }

    public long getEnqueueTimeMs() {
        return enqueueTimeMs;
    }
}
