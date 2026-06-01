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

package org.apache.fluss.server.replica.delay;

import org.apache.fluss.annotation.Internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Default implementation of {@link ActionQueue} that collects actions into a concurrent queue and
 * executes them when {@link #tryCompleteActions()} is called.
 *
 * <p>Uses {@link ConcurrentLinkedQueue} for lock-free enqueue. Actions are executed and removed
 * from the queue when {@link #tryCompleteActions()} is called.
 */
@Internal
public class DelayedActionQueue implements ActionQueue {
    private static final Logger LOG = LoggerFactory.getLogger(DelayedActionQueue.class);

    private final ConcurrentLinkedQueue<Runnable> queue = new ConcurrentLinkedQueue<>();

    @Override
    public void add(Runnable action) {
        queue.add(action);
    }

    @Override
    public void tryCompleteActions() {
        int maxToComplete = queue.size();
        int count = 0;
        while (count < maxToComplete) {
            Runnable action = queue.poll();
            if (action == null) {
                break;
            }
            try {
                action.run();
            } catch (Throwable t) {
                LOG.error("Failed to complete delayed action.", t);
            }
            count++;
        }
    }
}
