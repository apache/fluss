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

package org.apache.fluss.server.coordinator.rebalance.executor;

import org.apache.fluss.cluster.rebalance.RebalancePlanForBucket;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.messages.RebalanceResponse;
import org.apache.fluss.server.coordinator.event.EventManager;
import org.apache.fluss.server.coordinator.event.ExecuteRebalanceTaskEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/** An executor service that executes the rebalance actions. */
public class RebalanceActionExecutorService implements ActionExecutorService, Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(RebalanceActionExecutorService.class);

    private final Supplier<EventManager> eventManagerSupplier;
    private final BlockingQueue<Task> actionQueue = new ArrayBlockingQueue<>(5);
    private volatile boolean shutdown;

    public RebalanceActionExecutorService(Supplier<EventManager> eventManagerSupplier) {
        this.eventManagerSupplier = eventManagerSupplier;
    }

    @Override
    public void start() {
        this.shutdown = false;
        LOG.info("Starting rebalance action executor service.");
    }

    @Override
    public void shutdown() {
        this.shutdown = true;
        LOG.info("Shutting down rebalance action executor service.");
    }

    @Override
    public CompletableFuture<RebalanceResponse> execute(
            Map<TableBucket, RebalancePlanForBucket> actions) {
        CompletableFuture<RebalanceResponse> cf = new CompletableFuture<>();
        if (!actionQueue.isEmpty()) {
            cf.completeExceptionally(
                    new IllegalStateException(
                            "Rebalance action executor service is busy, Currently, we only support one task in progress."));
        } else {
            try {
                actionQueue.put(new Task(actions, cf));
            } catch (InterruptedException e) {
                LOG.error("Failed to put rebalance action into action queue.", e);
                cf.completeExceptionally(e);
            }
        }
        return cf;
    }

    @Override
    public void run() {
        while (!shutdown) {
            try {
                doReassign(actionQueue.take());
            } catch (InterruptedException e) {
                // Do nothing.
            }
        }
    }

    private void doReassign(Task task) {
        LOG.info("Trigger Executing rebalance task");
        ExecuteRebalanceTaskEvent executeRebalanceTaskEvent =
                new ExecuteRebalanceTaskEvent(task.getActions(), task.getFuture());
        eventManagerSupplier.get().put(executeRebalanceTaskEvent);
    }

    private static class Task {
        private final Map<TableBucket, RebalancePlanForBucket> actions;
        private final CompletableFuture<RebalanceResponse> future;

        public Task(
                Map<TableBucket, RebalancePlanForBucket> actions,
                CompletableFuture<RebalanceResponse> future) {
            this.actions = actions;
            this.future = future;
        }

        public Map<TableBucket, RebalancePlanForBucket> getActions() {
            return actions;
        }

        public CompletableFuture<RebalanceResponse> getFuture() {
            return future;
        }
    }
}
