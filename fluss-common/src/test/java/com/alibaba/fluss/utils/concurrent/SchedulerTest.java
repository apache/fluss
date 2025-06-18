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

package com.alibaba.fluss.utils.concurrent;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static com.alibaba.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link Scheduler}. */
final class SchedulerTest {

    private final AtomicInteger counter1 = new AtomicInteger(0);
    private final Scheduler scheduler = new FlussScheduler(1);

    @BeforeEach
    public void beforeEach() {
        scheduler.startup();
    }

    @AfterEach
    public void afterEach() throws InterruptedException {
        scheduler.shutdown();
    }

    @Test
    void testNonPeriodicTask() throws Exception {
        scheduler.scheduleOnce("test1", counter1::getAndIncrement);
        retry(Duration.ofSeconds(30), () -> assertThat(counter1.get()).isEqualTo(1));
    }

    @Test
    void testNonPeriodicTaskWhenPeriodIsZero() throws Exception {
        scheduler.schedule("test1", counter1::getAndIncrement, 0, 0);
        retry(Duration.ofSeconds(30), () -> assertThat(counter1.get()).isEqualTo(1));
    }

    @Test
    void testPeriodicTask() throws Exception {
        scheduler.schedule("test1", counter1::getAndIncrement, 0, 5);
        retry(Duration.ofSeconds(30), () -> assertThat(counter1.get()).isGreaterThan(20));
    }
}
