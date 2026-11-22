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

package org.apache.fluss.trino;

import io.trino.spi.Plugin;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorPageSourceProviderFactory;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.testing.DistributedQueryRunner;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.trino.testing.TestingSession.testSessionBuilder;

/** Distributed SQL fixture delegating all connector behavior to the production plugin. */
final class FlussQueryRunner {
    private FlussQueryRunner() {}

    static DistributedQueryRunner create(String bootstrapServers, TestingHooks hooks)
            throws Exception {
        DistributedQueryRunner runner =
                DistributedQueryRunner.builder(
                                testSessionBuilder().setCatalog("fluss").setSchema("fluss").build())
                        .setWorkerCount(1)
                        .addCoordinatorProperty("node-scheduler.include-coordinator", "false")
                        .build();
        try {
            runner.installPlugin(testingPlugin(hooks));
            runner.createCatalog(
                    "fluss",
                    "fluss",
                    Collections.singletonMap("bootstrap.servers", bootstrapServers));
            return runner;
        } catch (Exception | Error failure) {
            try {
                runner.close();
            } catch (Exception | Error cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    private static Plugin testingPlugin(TestingHooks hooks) {
        ConnectorFactory factory = new FlussPlugin().getConnectorFactories().iterator().next();
        return new Plugin() {
            @Override
            public Iterable<ConnectorFactory> getConnectorFactories() {
                return Collections.singletonList(
                        new ConnectorFactory() {
                            @Override
                            public String getName() {
                                return factory.getName();
                            }

                            @Override
                            public Connector create(
                                    String name,
                                    Map<String, String> properties,
                                    ConnectorContext context) {
                                return observeConnector(
                                        factory.create(name, properties, context), hooks);
                            }
                        });
            }
        };
    }

    private static Connector observeConnector(Connector connector, TestingHooks hooks) {
        return proxy(
                Connector.class,
                (ignored, method, args) -> {
                    Object result = invoke(method, connector, args);
                    if (method.getName().equals("getSplitManager")) {
                        return observeSplits((ConnectorSplitManager) result, hooks);
                    }
                    if (method.getName().equals("getPageSourceProvider")) {
                        return observeProvider((ConnectorPageSourceProvider) result, hooks);
                    }
                    if (method.getName().equals("getPageSourceProviderFactory")) {
                        ConnectorPageSourceProviderFactory factory =
                                (ConnectorPageSourceProviderFactory) result;
                        return (ConnectorPageSourceProviderFactory)
                                () -> observeProvider(factory.createPageSourceProvider(), hooks);
                    }
                    return result;
                });
    }

    private static ConnectorSplitManager observeSplits(
            ConnectorSplitManager splits, TestingHooks hooks) {
        return proxy(
                ConnectorSplitManager.class,
                (ignored, method, args) -> {
                    Object source = invoke(method, splits, args);
                    if (method.getName().equals("getSplits")) {
                        hooks.afterPlanning((FlussTableHandle) args[2]);
                    }
                    return source;
                });
    }

    private static ConnectorPageSourceProvider observeProvider(
            ConnectorPageSourceProvider provider, TestingHooks hooks) {
        return proxy(
                ConnectorPageSourceProvider.class,
                (ignored, method, args) -> {
                    Object result = invoke(method, provider, args);
                    if (!method.getName().equals("createPageSource")) {
                        return result;
                    }
                    ConnectorPageSource source = (ConnectorPageSource) result;
                    String table = ((FlussTableHandle) args[3]).getFlussTableName();
                    hooks.activeSources.incrementAndGet();
                    hooks.createdSources.incrementAndGet();
                    AtomicBoolean finished = new AtomicBoolean();
                    return proxy(
                            ConnectorPageSource.class,
                            (sourceProxy, sourceMethod, sourceArgs) -> {
                                try {
                                    Object page = invoke(sourceMethod, source, sourceArgs);
                                    if (sourceMethod.getName().equals("getNextSourcePage")
                                            && page != null) {
                                        hooks.afterRead(table);
                                    }
                                    return page;
                                } finally {
                                    if (source.isFinished()
                                            && finished.compareAndSet(false, true)) {
                                        hooks.activeSources.decrementAndGet();
                                    }
                                }
                            });
                });
    }

    private static <T> T proxy(Class<T> contract, InvocationHandler handler) {
        return contract.cast(
                Proxy.newProxyInstance(
                        contract.getClassLoader(), new Class<?>[] {contract}, handler));
    }

    private static Object invoke(Method method, Object target, Object[] arguments)
            throws Throwable {
        try {
            return method.invoke(target, arguments);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    /** Test synchronization and observation only; never changes offsets or replaces readers. */
    static final class TestingHooks {
        private final AtomicReference<Barrier> planning = new AtomicReference<>();
        private final AtomicReference<Barrier> reading = new AtomicReference<>();
        private final AtomicInteger activeSources = new AtomicInteger();
        private final AtomicInteger createdSources = new AtomicInteger();

        Barrier pause(String table) {
            return arm(planning, table);
        }

        Barrier pauseAfterRead(String table) {
            return arm(reading, table);
        }

        int activeSources() {
            return activeSources.get();
        }

        int createdSources() {
            return createdSources.get();
        }

        private static Barrier arm(AtomicReference<Barrier> slot, String table) {
            Barrier next = new Barrier(table, slot);
            if (!slot.compareAndSet(null, next)) {
                throw new IllegalStateException("Test gate already armed");
            }
            return next;
        }

        private void afterPlanning(FlussTableHandle table) throws InterruptedException {
            await(planning, table.getFlussTableName());
        }

        private void afterRead(String table) throws InterruptedException {
            await(reading, table);
        }

        private static void await(AtomicReference<Barrier> slot, String table)
                throws InterruptedException {
            Barrier current = slot.get();
            if (current != null
                    && current.table.equals(table)
                    && slot.compareAndSet(current, null)) {
                current.reached.countDown();
                if (!current.release.await(30, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Timed out waiting for test gate release");
                }
            }
        }
    }

    static final class Barrier implements AutoCloseable {
        private final String table;
        private final AtomicReference<Barrier> slot;
        private final CountDownLatch reached = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        private Barrier(String table, AtomicReference<Barrier> slot) {
            this.table = table;
            this.slot = slot;
        }

        void awaitPlanned() throws InterruptedException {
            awaitReached();
        }

        void awaitReached() throws InterruptedException {
            if (!reached.await(30, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Query did not reach test gate for " + table);
            }
        }

        @Override
        public void close() {
            slot.compareAndSet(this, null);
            release.countDown();
        }
    }
}
