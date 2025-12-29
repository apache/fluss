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

import org.apache.fluss.cluster.rebalance.ServerTag;
import org.apache.fluss.rpc.messages.RemoveServerTagResponse;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/** An event for remove server tag. */
public class RemoveServerTagEvent implements CoordinatorEvent {
    private final List<Integer> serverIds;
    private final ServerTag serverTag;
    private final CompletableFuture<RemoveServerTagResponse> respCallback;

    public RemoveServerTagEvent(
            List<Integer> serverIds,
            ServerTag serverTag,
            CompletableFuture<RemoveServerTagResponse> respCallback) {
        this.serverIds = serverIds;
        this.serverTag = serverTag;
        this.respCallback = respCallback;
    }

    public List<Integer> getServerIds() {
        return serverIds;
    }

    public ServerTag getServerTag() {
        return serverTag;
    }

    public CompletableFuture<RemoveServerTagResponse> getRespCallback() {
        return respCallback;
    }
}
