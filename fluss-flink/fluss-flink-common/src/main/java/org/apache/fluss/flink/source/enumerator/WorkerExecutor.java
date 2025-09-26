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

package org.apache.fluss.flink.source.enumerator;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * A worker executor that extends {@link SplitEnumeratorContext} with fixed delay scheduling
 * capabilities for asynchronous tasks.
 *
 * <p>{@link SplitEnumeratorContext} natively only supports fixed rate scheduling for asynchronous
 * calls, which can lead to task accumulation if individual calls take too long to complete.
 *
 * <p>This executor wraps a single-threaded {@link ScheduledExecutorService} to handle async
 * operations and route their results back to the coordinator thread through the {@link
 * SplitEnumeratorContext#callAsync} methods.
 *
 * <p>TODO: This class is a workaround and should be removed once FLINK-38335 is completed.
 */
@Internal
public class WorkerExecutor implements AutoCloseable {
    protected final SplitEnumeratorContext<SourceSplitBase> context;
    private final ScheduledExecutorService scheduledExecutor;

    public WorkerExecutor(SplitEnumeratorContext<SourceSplitBase> context) {
        this.context = context;
        this.scheduledExecutor =
                Executors.newScheduledThreadPool(
                        1, new ExecutorThreadFactory("SplitEnumeratorContextWrapper"));
    }

    public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler) {
        context.callAsync(callable, handler);
    }

    public <T> void callAsyncAtFixedDelay(
            Callable<T> callable, BiConsumer<T, Throwable> handler, long initialDelay, long delay) {
        scheduledExecutor.scheduleWithFixedDelay(
                () -> {
                    final CountDownLatch latch = new CountDownLatch(1);
                    context.callAsync(
                            () -> {
                                try {
                                    return callable.call();
                                } finally {
                                    latch.countDown();
                                }
                            },
                            handler);
                    // wait for the call to complete
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        throw new FlussRuntimeException(
                                "Interrupted while waiting for async call to complete", e);
                    }
                },
                initialDelay,
                delay,
                TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() throws Exception {
        scheduledExecutor.shutdownNow();
    }
}
