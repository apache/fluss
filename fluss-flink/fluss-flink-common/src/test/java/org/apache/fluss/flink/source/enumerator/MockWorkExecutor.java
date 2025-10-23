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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;

/** A mock implementation of WorkerExecutor which separate task submit and run. */
public class MockWorkExecutor extends WorkerExecutor implements AutoCloseable {
    private final List<Callable<?>> periodicCallables;

    public MockWorkExecutor(MockSplitEnumeratorContext<SourceSplitBase> context) {
        super(context);
        this.periodicCallables = new ArrayList<>();
    }

    @Override
    public <T> void callAsyncAtFixedDelay(
            Callable<T> callable, BiConsumer<T, Throwable> handler, long initialDelay, long delay) {
        periodicCallables.add(
                () -> {
                    CountDownLatch latch = new CountDownLatch(1);
                    context.callAsync(
                            () -> {
                                try {
                                    return callable.call();
                                } finally {
                                    latch.countDown();
                                }
                            },
                            handler);
                    try {
                        ((MockSplitEnumeratorContext<?>) context).runNextOneTimeCallable();
                    } catch (Throwable e) {
                        throw new RuntimeException(e);
                    }
                    // wait for the call to complete
                    latch.await();
                    return null;
                });
    }

    public void runPeriodicCallable(int index) throws Throwable {
        periodicCallables.get(index).call();
    }

    public void runNextOneTimeCallable() throws Throwable {
        ((MockSplitEnumeratorContext<?>) context).runNextOneTimeCallable();
    }

    public BlockingQueue<Callable<Future<?>>> getOneTimeCallables() {
        return ((MockSplitEnumeratorContext<?>) context).getOneTimeCallables();
    }

    @Override
    public void close() throws Exception {
        this.periodicCallables.clear();
        ((MockSplitEnumeratorContext<?>) context).close();
    }
}
