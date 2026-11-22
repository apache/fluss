/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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
import org.apache.fluss.rpc.messages.AddServerTagByRackResponse;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/** An event for adding a server tag to tablet servers in the specified racks. */
public class AddServerTagByRackEvent implements CoordinatorEvent {
    private final List<String> racks;
    private final ServerTag serverTag;
    private final CompletableFuture<AddServerTagByRackResponse> respCallback;

    public AddServerTagByRackEvent(
            List<String> racks,
            ServerTag serverTag,
            CompletableFuture<AddServerTagByRackResponse> respCallback) {
        this.racks = racks;
        this.serverTag = serverTag;
        this.respCallback = respCallback;
    }

    public List<String> getRacks() {
        return racks;
    }

    public ServerTag getServerTag() {
        return serverTag;
    }

    public CompletableFuture<AddServerTagByRackResponse> getRespCallback() {
        return respCallback;
    }
}
