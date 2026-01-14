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

package org.apache.fluss.server.utils.timer;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link org.apache.fluss.server.utils.timer.TimerTaskEntry}. */
public class TimerTaskEntryTest {

    @Test
    void testRemoveEnsuresCurrentListNullSafety() throws InterruptedException {
        // Create two lists to reproduce the values that we are working
        // with being added/removed. We will oscillate between adding
        // and removing these elements until we encounter a NPE
        AtomicInteger sharedTaskCounter = new AtomicInteger(0);
        TimerTaskList primaryList = new TimerTaskList(sharedTaskCounter);
        TimerTaskList secondaryList = new TimerTaskList(sharedTaskCounter);

        // Set up our initial task that will handle coordinating this
        // reproduction behavior
        TestTask task = new TestTask(0L);
        TimerTaskEntry entry = new TimerTaskEntry(task, 10L);
        primaryList.add(entry);

        // Container for any NullPointerException caught during remove()
        AtomicReference<NullPointerException> thrownException = new AtomicReference<>();

        // Latch to handle coordinating addition/removal threads
        CountDownLatch latch = new CountDownLatch(1);

        // Create a thread responsible for continually removing entries, which
        // will be responsible for triggering the exception
        Thread removalThread =
                new Thread(
                        () -> {
                            try {
                                latch.await();
                                // Continually remove elements from the task (forward-oscillation)
                                for (int i = 0; i < 10000; i++) {
                                    entry.remove();
                                }
                            } catch (NullPointerException e) {
                                thrownException.set(e);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        });

        // Create a separate thread for adding the entry while the removal thread is
        // still executing which results in the expected null reference
        Thread additionThread =
                new Thread(
                        () -> {
                            try {
                                // Wait for the initial removal to complete
                                latch.await();
                                // Add the entry to our separate list while the removal thread is
                                // still verifying the condition (resulting in our null list within
                                // the internal removal call, and our exception)
                                for (int i = 0; i < 10000; i++) {
                                    // Determine which list to add to the task
                                    // (backwards-oscillation)
                                    TimerTaskList currentList = entry.list;
                                    if (currentList == null || currentList == primaryList) {
                                        // If the entry is not in any list or in the primary list,
                                        // move it to the secondary list
                                        secondaryList.add(entry);
                                    } else if (currentList == secondaryList) {
                                        // If the entry is in the secondary list, move it to the
                                        // primary list
                                        primaryList.add(entry);
                                    }
                                    Thread.yield();
                                }
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        });

        // Start both threads
        removalThread.start();
        additionThread.start();

        // Release both threads to trigger our race condition
        latch.countDown();

        // Wait for threads to complete
        removalThread.join();
        additionThread.join();

        // Attempt to remove the last entry (to ensure empty list)
        entry.remove();

        // Verify the list is empty after entry removal and ensure
        // counter reflects the correct state
        assertThat(entry.list).isNull();
        assertThat(sharedTaskCounter.get()).isEqualTo(0);

        // Assert that no exception was originated
        assertThat(thrownException.get()).isNull();
    }

    private static class TestTask extends TimerTask {
        public TestTask(long delayMs) {
            super(delayMs);
        }

        @Override
        public void run() {}
    }
}
