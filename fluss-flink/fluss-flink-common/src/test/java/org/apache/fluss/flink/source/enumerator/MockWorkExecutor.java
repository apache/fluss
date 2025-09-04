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

import org.apache.fluss.flink.source.split.SourceSplitBase;

import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;

/** A mock implementation of WorkerExecutor which separate task submit and run. */
public class MockWorkExecutor extends WorkerExecutor implements AutoCloseable {
    private final List<Callable<Void>> periodicCallables;
    private final BlockingQueue<Callable<Future<?>>> oneTimeCallables;

    public MockWorkExecutor(MockSplitEnumeratorContext<SourceSplitBase> context) {
        super(context);
        this.periodicCallables = new ArrayList<>();
        this.oneTimeCallables = new ArrayBlockingQueue<>(100);
    }

    @Override
    public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler) {
        this.oneTimeCallables.add(
                () -> {
                    try {
                        T result = callable.call();
                        context.callAsync(() -> result, handler);
                    } catch (Throwable t) {
                        context.callAsync(
                                () -> {
                                    throw t;
                                },
                                handler);
                    }
                    return null;
                });
    }

    @Override
    public <T> void callAsyncAtFixedDelay(
            Callable<T> callable, BiConsumer<T, Throwable> handler, long initialDelay, long delay) {
        this.periodicCallables.add(
                () -> {
                    try {
                        T result = callable.call();
                        context.callAsync(() -> result, handler);
                    } catch (Throwable t) {
                        context.callAsync(
                                () -> {
                                    throw t;
                                },
                                handler);
                    }
                    return null;
                });
    }

    public void runPeriodicCallable(int index) throws Throwable {
        this.periodicCallables.get(index).call();
        ((MockSplitEnumeratorContext<?>) context).runNextOneTimeCallable();
    }

    public void runNextOneTimeCallable() throws Throwable {
        this.oneTimeCallables.take().call();
        ((MockSplitEnumeratorContext<?>) context).runNextOneTimeCallable();
    }

    public BlockingQueue<Callable<Future<?>>> getOneTimeCallables() {
        return oneTimeCallables;
    }

    @Override
    public void close() throws Exception {
        this.periodicCallables.clear();
        ((MockSplitEnumeratorContext<?>) context).close();
    }
}
