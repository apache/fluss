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
 * Thread-safe {@link ActionQueue} backed by a concurrent queue.
 *
 * <p>Each drain bounds its work using the queue's weakly consistent size at the start. Actions
 * added while draining may remain available for a later drain. A failing action is logged and does
 * not prevent the remaining bounded set from running.
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
        int actionsToComplete = queue.size();
        for (int completed = 0; completed < actionsToComplete; completed++) {
            Runnable action = queue.poll();
            if (action == null) {
                return;
            }
            try {
                action.run();
            } catch (Exception e) {
                LOG.error("Failed to complete delayed action.", e);
            }
        }
    }
}
