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

package org.apache.fluss.server.coordinator.event;

import org.apache.fluss.rpc.messages.ApiMessage;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

/** A compaction request or callback belonging to one active Coordinator event processor. */
public final class CompactionEvent implements CoordinatorEvent {
    @Nullable private final ApiMessage request;
    @Nullable private final CompletableFuture<ApiMessage> response;
    @Nullable private final Runnable callback;

    /** Creates a public RPC request event. */
    public CompactionEvent(ApiMessage request, CompletableFuture<ApiMessage> response) {
        this.request = request;
        this.response = response;
        this.callback = null;
    }

    /** Creates a manager completion callback for the owning processor's event queue. */
    public CompactionEvent(Runnable callback) {
        this.request = null;
        this.response = null;
        this.callback = callback;
    }

    /** Returns the RPC request, or null for a callback. */
    @Nullable
    public ApiMessage request() {
        return request;
    }

    /** Returns the RPC response future. */
    @Nullable
    public CompletableFuture<ApiMessage> response() {
        return response;
    }

    /** Returns the manager callback, or null for an RPC request. */
    @Nullable
    public Runnable callback() {
        return callback;
    }
}
