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
import org.apache.fluss.flink.source.split.SourceSplitBase;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * A worker executor that is used to schedule asynchronous tasks to extend {@link
 * SplitEnumeratorContext} with fixed delay capabilities. This class serves as a workaround until
 * {@link SplitEnumeratorContext} natively supports asynchronous calls with fixed delay scheduling.
 *
 * <p>This executor wraps a single-threaded {@link ScheduledExecutorService} to handle async
 * operations and route their results back to the coordinator thread through the {@link
 * SplitEnumeratorContext#callAsync} methods.
 */
@Internal
public class WorkerExecutor {
    protected final SplitEnumeratorContext<SourceSplitBase> context;
    private final ScheduledExecutorService workerExecutor;

    public WorkerExecutor(SplitEnumeratorContext<SourceSplitBase> context) {
        this.context = context;
        this.workerExecutor = Executors.newScheduledThreadPool(1);
    }

    public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler) {
        workerExecutor.execute(
                () -> {
                    try {
                        T result = callable.call();
                        // reuse the context async call to notify coordinator thread.
                        context.callAsync(() -> result, handler);
                    } catch (Throwable t) {
                        context.callAsync(
                                () -> {
                                    throw t;
                                },
                                handler);
                    }
                });
    }

    public <T> void callAsyncAtFixedDelay(
            Callable<T> callable, BiConsumer<T, Throwable> handler, long initialDelay, long delay) {
        workerExecutor.scheduleWithFixedDelay(
                () -> {
                    try {
                        T result = callable.call();
                        // reuse the context async call to notify coordinator thread.
                        context.callAsync(() -> result, handler);
                    } catch (Throwable t) {
                        context.callAsync(
                                () -> {
                                    throw t;
                                },
                                handler);
                    }
                },
                initialDelay,
                delay,
                TimeUnit.MILLISECONDS);
    }
}
