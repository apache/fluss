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

import org.apache.fluss.rpc.messages.ListRebalanceProgressResponse;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

/** The event for listing rebalance progress. */
public class ListRebalanceProgressEvent implements CoordinatorEvent {

    private final @Nullable String rebalanceId;
    private final CompletableFuture<ListRebalanceProgressResponse> respCallback;

    public ListRebalanceProgressEvent(
            @Nullable String rebalanceId,
            CompletableFuture<ListRebalanceProgressResponse> respCallback) {
        this.rebalanceId = rebalanceId;
        this.respCallback = respCallback;
    }

    public @Nullable String getRabalanceId() {
        return rebalanceId;
    }

    public CompletableFuture<ListRebalanceProgressResponse> getRespCallback() {
        return respCallback;
    }
}
