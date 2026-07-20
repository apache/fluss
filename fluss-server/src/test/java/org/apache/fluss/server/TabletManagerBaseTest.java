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

package org.apache.fluss.server;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link TabletManagerBase}. */
final class TabletManagerBaseTest {

    @Test
    void testCloseTabletsConcurrentlyWaitsForAllTasksAndShutsDownPoolOnFailure() throws Exception {
        TestingTabletManager tabletManager = new TestingTabletManager(2);
        CountDownLatch failedCloseAttempted = new CountDownLatch(1);
        CountDownLatch blockedCloseStarted = new CountDownLatch(1);
        CountDownLatch finishBlockedClose = new CountDownLatch(1);
        AtomicBoolean blockedCloseCompleted = new AtomicBoolean();

        CompletableFuture<Void> closingFuture =
                tabletManager.closeTablets(
                        Arrays.asList(0, 1),
                        tablet -> {
                            if (tablet == 0) {
                                failedCloseAttempted.countDown();
                                throw new FlussRuntimeException("Expected close failure.");
                            }

                            blockedCloseStarted.countDown();
                            await(finishBlockedClose);
                            blockedCloseCompleted.set(true);
                        });

        try {
            assertThat(failedCloseAttempted.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(blockedCloseStarted.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(closingFuture.isDone()).isFalse();
        } finally {
            finishBlockedClose.countDown();
        }

        assertThatThrownBy(() -> closingFuture.get(10, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(FlussRuntimeException.class);
        assertThat(blockedCloseCompleted.get()).isTrue();
        assertThat(tabletManager.getClosingPool().isShutdown()).isTrue();
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FlussRuntimeException("Interrupted while waiting to finish tablet close.", e);
        }
    }

    private static final class TestingTabletManager extends TabletManagerBase {
        private ExecutorService closingPool;

        private TestingTabletManager(int closingThreads) {
            super(TabletType.KV, Collections.emptyList(), new Configuration(), closingThreads);
        }

        private CompletableFuture<Void> closeTablets(
                List<Integer> tablets, Consumer<Integer> closeAction) {
            return closeTabletsConcurrently(tablets, "testing-tablet-closing", closeAction);
        }

        @Override
        protected ExecutorService createThreadPool(String poolName) {
            closingPool = super.createThreadPool(poolName);
            return closingPool;
        }

        private ExecutorService getClosingPool() {
            return closingPool;
        }
    }
}
