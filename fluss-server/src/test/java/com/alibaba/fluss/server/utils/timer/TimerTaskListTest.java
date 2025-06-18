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

package com.alibaba.fluss.server.utils.timer;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TimerTaskList}. */
public class TimerTaskListTest {

    @Test
    void testAll() {
        AtomicInteger sharedCounter = new AtomicInteger(0);
        TimerTaskList list1 = new TimerTaskList(sharedCounter);
        TimerTaskList list2 = new TimerTaskList(sharedCounter);
        TimerTaskList list3 = new TimerTaskList(sharedCounter);

        List<TestTask> tasks = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            TestTask task = new TestTask(0L);
            list1.add(new TimerTaskEntry(task, 10L));
            assertThat(sharedCounter.get()).isEqualTo(i);
            tasks.add(task);
        }

        assertThat(sharedCounter.get()).isEqualTo(tasks.size());

        // reinserting the existing tasks shouldn't change the task count.
        for (int i = 0; i < 4; i++) {
            TestTask testTask = tasks.get(i);
            int prevCount = sharedCounter.get();
            // new TimerTaskEntry(task) will remove the existing entry from list1
            list2.add(new TimerTaskEntry(testTask, 10L));
            assertThat(sharedCounter.get()).isEqualTo(prevCount);
        }
        assertThat(TestTask.size(list1)).isEqualTo(10 - 4);
        assertThat(TestTask.size(list2)).isEqualTo(4);

        assertThat(sharedCounter.get()).isEqualTo(tasks.size());

        /// Reinserting the existing tasks shouldn't change the task count.
        for (int i = 4; i < tasks.size(); i++) {
            TestTask testTask = tasks.get(i);
            int prevCount = sharedCounter.get();
            // new TimerTaskEntry(task) will remove the existing entry from list1
            list3.add(new TimerTaskEntry(testTask, 10L));
            assertThat(sharedCounter.get()).isEqualTo(prevCount);
        }
        assertThat(TestTask.size(list1)).isEqualTo(0);
        assertThat(TestTask.size(list2)).isEqualTo(4);
        assertThat(TestTask.size(list3)).isEqualTo(6);

        assertThat(sharedCounter.get()).isEqualTo(tasks.size());

        // cancel tasks in lists.
        list1.forEach(TimerTask::cancel);
        assertThat(TestTask.size(list1)).isEqualTo(0);
        assertThat(TestTask.size(list2)).isEqualTo(4);
        assertThat(TestTask.size(list3)).isEqualTo(6);

        list2.forEach(TimerTask::cancel);
        assertThat(TestTask.size(list1)).isEqualTo(0);
        assertThat(TestTask.size(list2)).isEqualTo(0);
        assertThat(TestTask.size(list3)).isEqualTo(6);

        list3.forEach(TimerTask::cancel);
        assertThat(TestTask.size(list1)).isEqualTo(0);
        assertThat(TestTask.size(list2)).isEqualTo(0);
        assertThat(TestTask.size(list3)).isEqualTo(0);
    }

    private static class TestTask extends TimerTask {
        public TestTask(long delayMs) {
            super(delayMs);
        }

        @Override
        public void run() {}

        private static int size(TimerTaskList list) {
            AtomicInteger count = new AtomicInteger();
            list.forEach(f -> count.addAndGet(1));
            return count.get();
        }
    }
}
