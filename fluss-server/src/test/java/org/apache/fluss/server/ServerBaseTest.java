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
import org.apache.fluss.server.authorizer.Authorizer;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ServerBase}. */
final class ServerBaseTest {

    @Test
    void testFatalErrorTriggersShutdownOnDedicatedThread() throws Exception {
        CountDownLatch shutdownStarted = new CountDownLatch(1);
        CountDownLatch finishShutdown = new CountDownLatch(1);
        AtomicReference<Thread> fatalErrorCaller = new AtomicReference<>();
        AtomicReference<Thread> shutdownThread = new AtomicReference<>();
        TestingServer server = new TestingServer(shutdownStarted, finishShutdown, shutdownThread);
        ExecutorService callerExecutor = Executors.newSingleThreadExecutor();
        Future<?> fatalErrorCall =
                callerExecutor.submit(
                        () -> {
                            fatalErrorCaller.set(Thread.currentThread());
                            server.onFatalError(new FlussRuntimeException("Expected fatal error."));
                        });

        try {
            fatalErrorCall.get(5, TimeUnit.SECONDS);
            assertThat(shutdownStarted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(server.getTerminationFuture().isDone()).isFalse();
            assertThat(shutdownThread.get()).isNotEqualTo(fatalErrorCaller.get());

            server.onFatalError(new FlussRuntimeException("Another expected fatal error."));
            assertThat(server.getShutdownCalls()).isOne();

            finishShutdown.countDown();
            assertThat(server.getTerminationFuture().get(5, TimeUnit.SECONDS))
                    .isEqualTo(ServerBase.Result.FAILURE);
        } finally {
            finishShutdown.countDown();
            callerExecutor.shutdownNow();
        }
    }

    @Test
    void testOutOfMemoryErrorTerminatesJvmWithoutStartingShutdown() {
        TestingServer server = createTestingServer();

        server.onFatalError(new OutOfMemoryError("Expected out-of-memory error."));

        assertThat(server.getJvmExitCalls()).isOne();
        assertThat(server.getShutdownCalls()).isZero();
    }

    @Test
    void testShutdownThreadStartFailureTerminatesJvm() {
        TestingServer server = createTestingServer();
        server.failFatalErrorShutdownThreadStart();

        try {
            server.onFatalError(new FlussRuntimeException("Expected fatal error."));

            assertThat(server.getJvmExitCalls()).isOne();
            assertThat(server.getShutdownCalls()).isZero();
        } finally {
            server.completeTermination();
        }
    }

    private static TestingServer createTestingServer() {
        return new TestingServer(
                new CountDownLatch(1), new CountDownLatch(0), new AtomicReference<>());
    }

    private static final class TestingServer extends ServerBase {
        private final CountDownLatch shutdownStarted;
        private final CountDownLatch finishShutdown;
        private final AtomicReference<Thread> shutdownThread;
        private final CompletableFuture<Result> terminationFuture = new CompletableFuture<>();
        private final AtomicInteger shutdownCalls = new AtomicInteger();
        private final AtomicInteger jvmExitCalls = new AtomicInteger();
        private boolean failFatalErrorShutdownThreadStart;

        private TestingServer(
                CountDownLatch shutdownStarted,
                CountDownLatch finishShutdown,
                AtomicReference<Thread> shutdownThread) {
            super(new Configuration());
            this.shutdownStarted = shutdownStarted;
            this.finishShutdown = finishShutdown;
            this.shutdownThread = shutdownThread;
        }

        @Override
        protected void startServices() {}

        @Override
        protected CompletableFuture<Result> closeAsync(Result result) {
            shutdownCalls.incrementAndGet();
            shutdownThread.set(Thread.currentThread());
            shutdownStarted.countDown();
            await(finishShutdown);
            terminationFuture.complete(result);
            return terminationFuture;
        }

        @Override
        protected CompletableFuture<Result> getTerminationFuture() {
            return terminationFuture;
        }

        @Override
        protected String getServerName() {
            return "testing-server";
        }

        @Override
        void startFatalErrorShutdownThread() {
            if (failFatalErrorShutdownThreadStart) {
                throw new OutOfMemoryError("unable to create new native thread");
            }
            super.startFatalErrorShutdownThread();
        }

        @Override
        void exitJvm() {
            jvmExitCalls.incrementAndGet();
        }

        @Override
        public @Nullable Authorizer getAuthorizer() {
            return null;
        }

        private int getShutdownCalls() {
            return shutdownCalls.get();
        }

        private int getJvmExitCalls() {
            return jvmExitCalls.get();
        }

        private void failFatalErrorShutdownThreadStart() {
            failFatalErrorShutdownThreadStart = true;
        }

        private void completeTermination() {
            terminationFuture.complete(Result.FAILURE);
        }

        private static void await(CountDownLatch latch) {
            try {
                latch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new FlussRuntimeException("Interrupted while waiting to finish shutdown.", e);
            }
        }
    }
}
