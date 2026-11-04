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

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests modifying DDL exclusion and permit ownership. */
class DdlAdmissionTest {
    @Test
    void testSameTableConflictsAndOtherTablesProceed() throws Exception {
        DdlAdmission admission = new DdlAdmission();
        try (DdlAdmission.Permit first = admission.acquire("db", "t1")) {
            first.checkTable(TablePath.of("db", "t1"));
            CompletableFuture.runAsync(
                            () -> {
                                assertThatThrownBy(() -> admission.acquire("db", "t1"))
                                        .isInstanceOf(OperationNotAttemptedException.class);
                                assertThatThrownBy(() -> admission.acquire("db", null))
                                        .isInstanceOf(OperationNotAttemptedException.class);
                                try (DdlAdmission.Permit other = admission.acquire("db", "t2")) {
                                    other.checkTable(TablePath.of("db", "t2"));
                                }
                                try (DdlAdmission.Permit other = admission.acquire("other", null)) {
                                    other.checkDatabase("other");
                                }
                            })
                    .get();
        }
        try (DdlAdmission.Permit database = admission.acquire("db", null)) {
            database.checkDatabase("db");
            database.checkTable(TablePath.of("db", "t1"));
            assertThatThrownBy(() -> admission.acquire("db", "t2"))
                    .isInstanceOf(OperationNotAttemptedException.class);
        }
    }

    @Test
    void testDuplicateCloseCannotReleaseLaterOwner() {
        DdlAdmission admission = new DdlAdmission();
        DdlAdmission.Permit old = admission.acquire("db", "t");
        old.close();
        try (DdlAdmission.Permit current = admission.acquire("db", "t")) {
            old.close();
            assertThatThrownBy(() -> admission.acquire("db", "t"))
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
                            try (DdlAdmission.Permit table = admission.acquire("db", "t")) {
                                table.checkDatabase("db");
                            }
                        })
                .isInstanceOf(IllegalStateException.class);
        try (DdlAdmission.Permit database = admission.acquire("db", null)) {
            assertThat(database).isNotNull();
            assertThatThrownBy(() -> database.checkTable(TablePath.of("other", "t")))
                    .isInstanceOf(IllegalStateException.class);
        }
    }
}
