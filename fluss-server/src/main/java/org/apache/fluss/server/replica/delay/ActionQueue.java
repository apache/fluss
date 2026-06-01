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

/**
 * A queue for collecting actions which need to be executed later.
 *
 * <p>This is used to decouple the enqueuing of delayed operation completions from their execution.
 * For example, after appending records, we enqueue actions to complete delayed fetch operations,
 * then execute them after the write path is fully finished.
 *
 * @see DelayedActionQueue
 */
@Internal
public interface ActionQueue {

    /** Adds an action to this queue. */
    void add(Runnable action);

    /** Tries to complete all pending actions in the queue. */
    void tryCompleteActions();
}
