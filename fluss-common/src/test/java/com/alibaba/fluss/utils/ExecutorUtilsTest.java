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

package com.alibaba.fluss.utils;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link com.alibaba.fluss.utils.ExecutorUtils}. */
public class ExecutorUtilsTest {

    @Test
    void testGracefulShutdown() {
        ExecutorService executorService1 = Executors.newFixedThreadPool(1);
        ExecutorService executorService2 = Executors.newFixedThreadPool(2);
        ExecutorUtils.gracefulShutdown(
                1000, TimeUnit.MILLISECONDS, executorService1, executorService2);
        assertThat(executorService1.isShutdown()).isTrue();
        assertThat(executorService2.isShutdown()).isTrue();
    }

    @Test
    void testNoBlockingShutdown() throws Exception {
        ExecutorService executorService1 = Executors.newFixedThreadPool(1);
        ExecutorService executorService2 = Executors.newFixedThreadPool(2);
        ExecutorUtils.gracefulShutdown(
                1000, TimeUnit.MILLISECONDS, executorService1, executorService2);
        ExecutorUtils.nonBlockingShutdown(
                        1000, TimeUnit.MILLISECONDS, executorService1, executorService2)
                .get();
        assertThat(executorService1.isShutdown()).isTrue();
        assertThat(executorService2.isShutdown()).isTrue();
    }
}
