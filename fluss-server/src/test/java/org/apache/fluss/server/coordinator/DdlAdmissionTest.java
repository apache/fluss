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

import org.apache.fluss.exception.OperationNotAttemptedException;
import org.apache.fluss.metadata.TablePath;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.FutureTask;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
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
}
