/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.coordinator.event;

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.rpc.messages.AdjustIsrResponse;
import com.alibaba.fluss.server.zk.data.LeaderAndIsr;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** An event for receive the request of adjust isr to coordinator server. */
public class AdjustIsrReceivedEvent implements CoordinatorEvent {
    private final Map<TableBucket, LeaderAndIsr> leaderAndIsrMap;
    private final CompletableFuture<AdjustIsrResponse> respCallback;

    public AdjustIsrReceivedEvent(
            Map<TableBucket, LeaderAndIsr> leaderAndIsrMap,
            CompletableFuture<AdjustIsrResponse> respCallback) {
        this.leaderAndIsrMap = leaderAndIsrMap;
        this.respCallback = respCallback;
    }

    public Map<TableBucket, LeaderAndIsr> getLeaderAndIsrMap() {
        return leaderAndIsrMap;
    }

    public CompletableFuture<AdjustIsrResponse> getRespCallback() {
        return respCallback;
    }
}
