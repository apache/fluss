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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.exception.NotCoordinatorLeaderException;
import org.apache.fluss.exception.OperationNotAttemptedException;
import org.apache.fluss.exception.UnknownServerException;
import org.apache.fluss.metadata.TablePath;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests modifying DDL exclusion and permit ownership. */
class DdlAdmissionTest {
    @Test
    void testSameTableConflictsAndOtherTablesProceed() throws Exception {
        DdlAdmission admission = new DdlAdmission();
        try (DdlAdmission.Permit first = admission.tryAcquire("db", "t1")) {
            first.checkTable(TablePath.of("db", "t1"));
            CompletableFuture.runAsync(
                            () -> {
                                assertThatThrownBy(() -> admission.tryAcquire("db", "t1"))
                                        .isInstanceOf(OperationNotAttemptedException.class);
                                assertThatThrownBy(() -> admission.tryAcquire("db", null))
                                        .isInstanceOf(OperationNotAttemptedException.class);
                                try (DdlAdmission.Permit other = admission.tryAcquire("db", "t2")) {
                                    other.checkTable(TablePath.of("db", "t2"));
                                }
                                try (DdlAdmission.Permit other =
                                        admission.tryAcquire("other", null)) {
                                    other.checkDatabase("other");
                                }
                            })
                    .get();
        }
        try (DdlAdmission.Permit database = admission.tryAcquire("db", null)) {
            database.checkDatabase("db");
            database.checkTable(TablePath.of("db", "t1"));
            assertThatThrownBy(() -> admission.tryAcquire("db", "t2"))
                    .isInstanceOf(OperationNotAttemptedException.class);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testWaitingAdmissionCanBeInterrupted(boolean databaseDdl) throws Exception {
        DdlAdmission admission = new DdlAdmission();
        String requestedTable = databaseDdl ? null : "t";
        FutureTask<Boolean> interrupted =
                new FutureTask<>(
                        () -> {
                            try (DdlAdmission.Permit ignored =
                                    admission.acquire("db", requestedTable)) {
                                return false;
                            } catch (InterruptedException expected) {
                                return true;
                            }
                        });
        Thread request = new Thread(interrupted);
        try (DdlAdmission.Permit held = admission.tryAcquire("db", "t")) {
            request.start();
            try {
                waitUntil(
                        () -> request.getState() == Thread.State.WAITING || interrupted.isDone(),
                        Duration.ofSeconds(10),
                        "DDL request did not wait for admission");
                assertThat(interrupted).isNotDone();
                request.interrupt();
                assertThat(interrupted.get()).isTrue();
                held.checkTable(TablePath.of("db", "t"));
                assertThatThrownBy(() -> admission.tryAcquire("db", requestedTable))
                        .isInstanceOf(OperationNotAttemptedException.class);
            } finally {
                request.interrupt();
                request.join();
            }
        }
        try (DdlAdmission.Permit permit = admission.acquire("db", null)) {
            permit.checkDatabase("db");
        }
    }

    @Test
    void testDuplicateCloseCannotReleaseLaterOwner() {
        DdlAdmission admission = new DdlAdmission();
        DdlAdmission.Permit old = admission.tryAcquire("db", "t");
        old.close();
        try (DdlAdmission.Permit current = admission.tryAcquire("db", "t")) {
            old.close();
            assertThatThrownBy(() -> admission.tryAcquire("db", "t"))
                    .isInstanceOf(OperationNotAttemptedException.class);
            current.checkTable(TablePath.of("db", "t"));
        }
        assertThatThrownBy(() -> old.checkTable(TablePath.of("db", "t")))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testFailureReleasesScopeAndPreventsUpgrade() {
        DdlAdmission admission = new DdlAdmission();
        assertThatThrownBy(
                        () -> {
                            try (DdlAdmission.Permit table = admission.tryAcquire("db", "t")) {
                                table.checkDatabase("db");
                            }
                        })
                .isInstanceOf(IllegalStateException.class);
        try (DdlAdmission.Permit database = admission.tryAcquire("db", null)) {
            assertThat(database).isNotNull();
            assertThatThrownBy(() -> database.checkTable(TablePath.of("other", "t")))
                    .isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void testWaitingRequestsDoNotEnterExecutorAndUnrelatedTablesProceed() throws Exception {
        DdlAdmission admission = new DdlAdmission();
        Deque<Runnable> tasks = new ArrayDeque<>();
        CompletableFuture<String> waiting;
        try (DdlAdmission.Permit held = admission.tryAcquire("db", "hot")) {
            waiting = admission.submit("db", "hot", tasks::add, permit -> "hot");
            CompletableFuture<String> other =
                    admission.submit("db", "cold", tasks::add, permit -> "cold");
            assertThat(tasks).hasSize(1);
            tasks.remove().run();
            assertThat(other.get()).isEqualTo("cold");
            assertThat(waiting).isNotDone();
        }
        tasks.remove().run();
        assertThat(waiting.get()).isEqualTo("hot");
    }

    @Test
    void testQueuedDatabaseDdlPrecedesLaterChildTablesWithoutBlockingOtherDatabases()
            throws Exception {
        DdlAdmission admission = new DdlAdmission();
        List<String> order = new ArrayList<>();
        CompletableFuture<String> database;
        CompletableFuture<String> table;
        try (DdlAdmission.Permit held = admission.tryAcquire("db", "first")) {
            database =
                    admission.submit(
                            "db",
                            null,
                            Runnable::run,
                            permit -> {
                                order.add("database");
                                return "database";
                            });
            table =
                    admission.submit(
                            "db",
                            "second",
                            Runnable::run,
                            permit -> {
                                order.add("table");
                                return "table";
                            });
            assertThat(database).isNotDone();
            assertThat(table).isNotDone();
            assertThat(admission.submit("other", "table", Runnable::run, permit -> "other").get())
                    .isEqualTo("other");
        }
        assertThat(database.get()).isEqualTo("database");
        assertThat(table.get()).isEqualTo("table");
        assertThat(order).containsExactly("database", "table");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testCancelledQueuedTaskRetainsCapacityUntilExecutorReleasesIt(boolean closeLeader)
            throws Exception {
        DdlAdmission admission = new DdlAdmission(1);
        Deque<Runnable> tasks = new ArrayDeque<>();
        AtomicBoolean executed = new AtomicBoolean();
        CompletableFuture<Boolean> queued =
                admission.submit(
                        "db",
                        "t",
                        tasks::add,
                        permit -> {
                            executed.set(true);
                            return true;
                        });
        if (closeLeader) {
            admission.close();
            assertThatThrownBy(queued::get)
                    .hasRootCauseInstanceOf(NotCoordinatorLeaderException.class);
        } else {
            queued.cancel(false);
            assertThat(queued).isCancelled();
        }
        DdlAdmission next = admission.newLeadership();
        CompletableFuture<String> rejected = next.submit("db", "t", Runnable::run, permit -> "new");
        assertThatThrownBy(rejected::get)
                .hasRootCauseInstanceOf(OperationNotAttemptedException.class);
        tasks.remove().run();
        assertThat(executed).isFalse();
        assertThat(next.submit("db", "t", Runnable::run, permit -> "new").get()).isEqualTo("new");
    }

    @Test
    void testClosingLeaderRejectsWaitersAndPreventsLaterWork() throws Exception {
        DdlAdmission admission = new DdlAdmission(1);
        try (DdlAdmission.Permit held = admission.tryAcquire("db", "t")) {
            CompletableFuture<String> waiter =
                    admission.submit("db", "t", Runnable::run, permit -> "old");
            admission.close();
            assertThatThrownBy(waiter::get)
                    .hasRootCauseInstanceOf(NotCoordinatorLeaderException.class);
            assertThatThrownBy(() -> admission.acquire("db", "t"))
                    .isInstanceOf(NotCoordinatorLeaderException.class);
            assertThatThrownBy(
                            () ->
                                    admission
                                            .submit("db", "t", Runnable::run, permit -> "late")
                                            .get())
                    .hasRootCauseInstanceOf(NotCoordinatorLeaderException.class);
            assertThat(
                            admission
                                    .newLeadership()
                                    .submit("db", "t", Runnable::run, permit -> "new")
                                    .get())
                    .isEqualTo("new");
        }
    }

    @Test
    void testRunningOperationKeepsItsBudgetAndOriginalCompletionAfterLeadershipEnds()
            throws Exception {
        DdlAdmission admission = new DdlAdmission(1);
        ExecutorService io = Executors.newSingleThreadExecutor();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try {
            CompletableFuture<String> running =
                    admission.submit(
                            "db",
                            "t",
                            io,
                            permit -> {
                                entered.countDown();
                                try {
                                    release.await();
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    throw new RuntimeException(e);
                                }
                                return "completed";
                            });
            assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
            admission.close();
            assertThat(running).isNotDone();
            DdlAdmission next = admission.newLeadership();
            assertThatThrownBy(() -> next.submit("db", "t", Runnable::run, permit -> "new").get())
                    .hasRootCauseInstanceOf(OperationNotAttemptedException.class);
            release.countDown();
            assertThat(running.get(10, TimeUnit.SECONDS)).isEqualTo("completed");
            io.submit(() -> {}).get(10, TimeUnit.SECONDS);
            assertThat(next.submit("db", "t", Runnable::run, permit -> "new").get())
                    .isEqualTo("new");
        } finally {
            release.countDown();
            io.shutdownNow();
        }
    }

    @Test
    void testCancellingWaiterImmediatelyReleasesItsCapacity() throws Exception {
        DdlAdmission admission = new DdlAdmission(1);
        try (DdlAdmission.Permit held = admission.tryAcquire("db", "t")) {
            CompletableFuture<String> waiter =
                    admission.submit("db", "t", Runnable::run, permit -> "waiting");
            waiter.cancel(false);
            assertThat(admission.submit("db", "other", Runnable::run, permit -> "other").get())
                    .isEqualTo("other");
        }
    }

    @Test
    void testSubmissionFailureReleasesPermitAndCapacity() throws Exception {
        DdlAdmission admission = new DdlAdmission(1);
        CompletableFuture<String> failed =
                admission.submit(
                        "db",
                        "t",
                        command -> {
                            throw new RejectedExecutionException("stopped");
                        },
                        permit -> "not executed");
        assertThatThrownBy(failed::get).hasRootCauseInstanceOf(RejectedExecutionException.class);
        assertThat(admission.submit("db", "t", Runnable::run, permit -> "retry").get())
                .isEqualTo("retry");
    }

    @Test
    void testCompletionCallbacksReleasePermitButStillOwnExecutionCapacity() throws Exception {
        DdlAdmission admission = new DdlAdmission(1);
        Deque<Runnable> tasks = new ArrayDeque<>();
        CompletableFuture<String> result =
                admission.submit("db", "t", tasks::add, permit -> "done");
        CompletableFuture<Void> callback =
                result.thenRun(
                        () -> {
                            CompletableFuture<Void> independent =
                                    CompletableFuture.runAsync(
                                            () -> {
                                                try (DdlAdmission.Permit next =
                                                        admission.tryAcquire("db", "t")) {
                                                    next.checkTable(TablePath.of("db", "t"));
                                                }
                                            });
                            assertThatCode(() -> independent.get(10, TimeUnit.SECONDS))
                                    .doesNotThrowAnyException();
                            assertThatThrownBy(
                                            () ->
                                                    admission
                                                            .newLeadership()
                                                            .submit(
                                                                    "db",
                                                                    "t",
                                                                    Runnable::run,
                                                                    permit -> "new")
                                                            .get())
                                    .hasRootCauseInstanceOf(OperationNotAttemptedException.class);
                        });
        tasks.remove().run();
        callback.get(10, TimeUnit.SECONDS);
        assertThat(admission.submit("db", "t", Runnable::run, permit -> "new").get())
                .isEqualTo("new");
    }

    @Test
    void testExecutionFailureIsNotConvertedToSafeToRetryLeaderRejection() {
        DdlAdmission admission = new DdlAdmission();
        CompletableFuture<String> failed =
                admission.submit(
                        "db",
                        "t",
                        Runnable::run,
                        permit -> {
                            admission.close();
                            throw new UnknownServerException(
                                    "Result unknown after execution began");
                        });
        assertThatThrownBy(failed::get).hasRootCauseInstanceOf(UnknownServerException.class);
    }

    @Test
    void testPreparationAndDatabaseFallbackShareOneRequestCapacity() throws Exception {
        DdlAdmission admission = new DdlAdmission(1);
        Deque<Runnable> tasks = new ArrayDeque<>();
        List<String> stages = new ArrayList<>();
        CompletableFuture<String> result =
                admission.withRequest(
                        context ->
                                admission
                                        .submit(
                                                context,
                                                null,
                                                null,
                                                tasks::add,
                                                ignored -> {
                                                    stages.add("prepare");
                                                    return "prepared";
                                                })
                                        .thenCompose(
                                                prepared ->
                                                        admission.submit(
                                                                context,
                                                                "db",
                                                                "t",
                                                                tasks::add,
                                                                permit -> {
                                                                    permit.checkTable(
                                                                            TablePath.of(
                                                                                    "db", "t"));
                                                                    stages.add("table");
                                                                    return prepared;
                                                                }))
                                        .thenCompose(
                                                prepared ->
                                                        admission.submit(
                                                                context,
                                                                "db",
                                                                null,
                                                                tasks::add,
                                                                permit -> {
                                                                    permit.checkDatabase("db");
                                                                    stages.add("database");
                                                                    return prepared;
                                                                })));
        for (int stage = 0; stage < 3; stage++) {
            assertThatThrownBy(
                            () ->
                                    admission
                                            .newLeadership()
                                            .submit("other", "t", Runnable::run, permit -> "extra")
                                            .get())
                    .hasRootCauseInstanceOf(OperationNotAttemptedException.class);
            assertThat(tasks).hasSize(1);
            tasks.remove().run();
        }
        assertThat(result.get()).isEqualTo("prepared");
        assertThat(stages).containsExactly("prepare", "table", "database");
        assertThat(
                        admission
                                .newLeadership()
                                .submit("other", "t", Runnable::run, permit -> "new")
                                .get())
                .isEqualTo("new");
    }

    @Test
    void testCancellationDuringPreparationKeepsQueuedWrapperButPreventsFollowingDdl()
            throws Exception {
        DdlAdmission admission = new DdlAdmission(1);
        Deque<Runnable> tasks = new ArrayDeque<>();
        AtomicBoolean executed = new AtomicBoolean();
        CompletableFuture<String> result =
                admission.withRequest(
                        context ->
                                admission
                                        .submit(
                                                context,
                                                null,
                                                null,
                                                tasks::add,
                                                ignored -> "prepared")
                                        .thenCompose(
                                                prepared ->
                                                        admission.submit(
                                                                context,
                                                                "db",
                                                                "t",
                                                                tasks::add,
                                                                permit -> {
                                                                    executed.set(true);
                                                                    return prepared;
                                                                })));
        result.cancel(false);
        assertThatThrownBy(
                        () ->
                                admission
                                        .newLeadership()
                                        .submit("db", "t", Runnable::run, permit -> "new")
                                        .get())
                .hasRootCauseInstanceOf(OperationNotAttemptedException.class);
        tasks.remove().run();
        assertThat(executed).isFalse();
        assertThat(tasks).isEmpty();
        assertThat(
                        admission
                                .newLeadership()
                                .submit("db", "t", Runnable::run, permit -> "new")
                                .get())
                .isEqualTo("new");
    }
}
