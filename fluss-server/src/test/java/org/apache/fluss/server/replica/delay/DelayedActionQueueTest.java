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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DelayedActionQueue}. */
class DelayedActionQueueTest {

    @Test
    void testActionsExecuteExactlyOnce() {
        DelayedActionQueue actionQueue = new DelayedActionQueue();
        AtomicInteger executions = new AtomicInteger();
        actionQueue.add(executions::incrementAndGet);
        actionQueue.add(executions::incrementAndGet);

        actionQueue.tryCompleteActions();
        actionQueue.tryCompleteActions();

        assertThat(executions).hasValue(2);
    }

    @Test
    void testActionFailureDoesNotPreventLaterActions() {
        DelayedActionQueue actionQueue = new DelayedActionQueue();
        AtomicInteger executions = new AtomicInteger();
        actionQueue.add(
                () -> {
                    throw new RuntimeException("expected test failure");
                });
        actionQueue.add(executions::incrementAndGet);

        actionQueue.tryCompleteActions();
        actionQueue.tryCompleteActions();

        assertThat(executions).hasValue(1);
    }

    @Test
    void testDrainUsesPendingActionSnapshot() {
        DelayedActionQueue actionQueue = new DelayedActionQueue();
        List<Integer> executions = new ArrayList<>();
        actionQueue.add(
                () -> {
                    executions.add(1);
                    actionQueue.add(() -> executions.add(2));
                });

        actionQueue.tryCompleteActions();
        assertThat(executions).containsExactly(1);

        actionQueue.tryCompleteActions();
        assertThat(executions).isEqualTo(Arrays.asList(1, 2));
    }
}
