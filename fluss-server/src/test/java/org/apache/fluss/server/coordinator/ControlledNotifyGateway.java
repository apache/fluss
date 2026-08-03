/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.coordinator;

import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrRequest;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrResponse;
import org.apache.fluss.server.tablet.TestTabletServerGateway;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

final class CountingFailingNotifyGateway extends TestTabletServerGateway {
    private final AtomicInteger notifyLeaderAndIsrCount = new AtomicInteger();

    CountingFailingNotifyGateway() {
        super(true, Collections.emptySet());
    }

    int getNotifyLeaderAndIsrCount() {
        return notifyLeaderAndIsrCount.get();
    }

    @Override
    public CompletableFuture<NotifyLeaderAndIsrResponse> notifyLeaderAndIsr(
            NotifyLeaderAndIsrRequest request) {
        notifyLeaderAndIsrCount.incrementAndGet();
        return super.notifyLeaderAndIsr(request);
    }
}

final class ControlledNotifyGateway extends TestTabletServerGateway {
    private volatile boolean controlMode;
    private final int responseServerId;
    private final ConcurrentLinkedDeque<ControlledNotifyTrigger> pendingTriggers;

    ControlledNotifyGateway(
            int responseServerId, ConcurrentLinkedDeque<ControlledNotifyTrigger> pendingTriggers) {
        super(false, Collections.emptySet());
        this.responseServerId = responseServerId;
        this.pendingTriggers = pendingTriggers;
    }

    void enableControlMode() {
        controlMode = true;
    }

    @Override
    public CompletableFuture<NotifyLeaderAndIsrResponse> notifyLeaderAndIsr(
            NotifyLeaderAndIsrRequest request) {
        if (!controlMode) {
            return super.notifyLeaderAndIsr(request);
        }
        NotifyLeaderAndIsrResponse response = super.notifyLeaderAndIsr(request).join();
        ControlledNotifyTrigger trigger = new ControlledNotifyTrigger(responseServerId);
        pendingTriggers.add(trigger);
        return trigger.getFuture().thenApply(ignored -> response);
    }
}

final class ControlledNotifyTrigger {
    private final int responseServerId;
    private final CompletableFuture<Void> future = new CompletableFuture<>();

    ControlledNotifyTrigger(int responseServerId) {
        this.responseServerId = responseServerId;
    }

    int getResponseServerId() {
        return responseServerId;
    }

    CompletableFuture<Void> getFuture() {
        return future;
    }

    void complete(Void value) {
        future.complete(value);
    }
}
