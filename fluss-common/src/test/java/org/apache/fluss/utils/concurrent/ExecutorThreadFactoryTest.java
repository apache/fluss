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

package org.apache.fluss.utils.concurrent;

import org.junit.jupiter.api.Test;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link org.apache.fluss.utils.concurrent.ExecutorThreadFactory}. */
public class ExecutorThreadFactoryTest {

    @Test
    void testThreadWithWithCustomExceptionHandler() throws Exception {
        CompletableFuture<Throwable> errorFuture = new CompletableFuture<>();
        ExecutorThreadFactory executorThreadFactory =
                new ExecutorThreadFactory.Builder()
                        .setPoolName("test-executor-thread-factory-pool-custom")
                        .setThreadPriority(1)
                        .setExceptionHandler(new TestExceptionHandler(errorFuture))
                        .build();
        Thread thread =
                executorThreadFactory.newThread(
                        () -> {
                            throw new RuntimeException("throw exception");
                        });
        thread.start();

        Throwable throwable = errorFuture.get(1, TimeUnit.MINUTES);
        assertThat(throwable).isInstanceOf(RuntimeException.class).hasMessage("throw exception");
    }

    @Test
    void testThreadWithWithCustomClassloader() {
        ClassLoader customClassloader = new URLClassLoader(new URL[0], null);
        ExecutorThreadFactory executorThreadFactory =
                new ExecutorThreadFactory.Builder()
                        .setPoolName("test-executor-thread-factory-pool-custom")
                        .setThreadPriority(1)
                        .setThreadContextClassloader(customClassloader)
                        .build();
        Thread thread =
                executorThreadFactory.newThread(
                        () -> {
                            // do nothing
                        });
        assertThat(thread.getContextClassLoader()).isEqualTo(customClassloader);
    }

    private static class TestExceptionHandler implements Thread.UncaughtExceptionHandler {

        private final CompletableFuture<Throwable> errorFuture;

        public TestExceptionHandler(CompletableFuture<Throwable> errorFuture) {
            this.errorFuture = errorFuture;
        }

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            errorFuture.complete(e);
        }
    }
}
