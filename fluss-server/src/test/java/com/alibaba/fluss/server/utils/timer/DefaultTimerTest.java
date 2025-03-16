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

package com.alibaba.fluss.server.utils.timer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link com.alibaba.fluss.server.utils.timer.DefaultTimer}. */
public class DefaultTimerTest {
    private DefaultTimer timer;

    @BeforeEach
    void setup() {
        timer = new DefaultTimer("test", 1, 3, TimeUnit.NANOSECONDS.toMillis(System.nanoTime()));
    }

    @AfterEach
    void tearDown() {
        timer.shutdown();
    }

    @Test
    void testAlreadyExpiredTask() throws InterruptedException {
        List<Integer> output = new ArrayList<>();

        List<CountDownLatch> latches = new ArrayList<>();
        for (int i = -5; i < 0; i++) {
            CountDownLatch latch = new CountDownLatch(1);
            timer.add(new TestTask(i, i, output, latch));
            latches.add(latch);
        }

        timer.advanceClock(0);

        for (CountDownLatch latch : latches) {
            // Already expired tasks should run immediately.
            assertThat(latch.await(3, TimeUnit.SECONDS)).isTrue();
        }

        // output of already expired tasks.
        assertThat(output).containsExactly(-5, -4, -3, -2, -1);
    }

    @Test
    void testTaskExpiration() throws Exception {
        List<Integer> output = new ArrayList<>();

        List<TestTask> tasks = new ArrayList<>();
        List<Integer> ids = new ArrayList<>();

        List<CountDownLatch> latches = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            CountDownLatch latch = new CountDownLatch(1);
            tasks.add(new TestTask(i, i, output, latch));
            ids.add(i);
            latches.add(latch);
        }

        for (int i = 10; i < 100; i++) {
            CountDownLatch latch = new CountDownLatch(2);
            tasks.add(new TestTask(i, i, output, latch));
            tasks.add(new TestTask(i, i, output, latch));
            ids.add(i);
            ids.add(i);
            latches.add(latch);
        }

        for (int i = 100; i < 500; i++) {
            CountDownLatch latch = new CountDownLatch(1);
            tasks.add(new TestTask(i, i, output, latch));
            ids.add(i);
            latches.add(latch);
        }

        // randomly submit request.
        tasks.forEach(task -> timer.add(task));

        while (timer.advanceClock(2000)) {
            // do nothing.
        }

        for (CountDownLatch latch : latches) {
            latch.await();
        }

        assertThat(output).containsExactly(ids.toArray(new Integer[0]));
    }

    private static class TestTask extends TimerTask {
        private final int id;
        private final List<Integer> output;
        private final CountDownLatch latch;
        private final AtomicBoolean completed;

        public TestTask(long delayMs, int id, List<Integer> output, CountDownLatch latch) {
            super(delayMs);
            this.id = id;
            this.output = output;
            this.latch = latch;
            this.completed = new AtomicBoolean(false);
        }

        @Override
        public void run() {
            if (completed.compareAndSet(false, true)) {
                synchronized (output) {
                    output.add(id);
                }
                latch.countDown();
            }
        }
    }
}
