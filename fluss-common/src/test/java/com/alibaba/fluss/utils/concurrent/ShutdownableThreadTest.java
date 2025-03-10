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

package com.alibaba.fluss.utils.concurrent;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link com.alibaba.fluss.utils.concurrent.ShutdownableThread} . */
class ShutdownableThreadTest {

    @Test
    void testShutdownAfterStartThread() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        ShutdownableThread thread =
                new ShutdownableThread("shutdownable-thread-test") {
                    @Override
                    public void doWork() {
                        latch.countDown();
                    }
                };
        thread.start();
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();

        thread.shutdown();
        assertThat(thread.isRunning()).isFalse();
    }
}
