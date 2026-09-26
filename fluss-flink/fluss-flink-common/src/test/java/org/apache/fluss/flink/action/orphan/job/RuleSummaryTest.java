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

package org.apache.fluss.flink.action.orphan.job;

import org.apache.fluss.flink.action.orphan.audit.ResultAuditLogger;
import org.apache.fluss.flink.action.orphan.rule.Decision;
import org.apache.fluss.flink.action.orphan.rule.RuleId;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RuleSummaryTest {
    @Test
    void retainsEveryDecisionWhenTasksAreCombined() {
        RuleSummary sum = new RuleSummary();
        for (Decision decision : Decision.values()) {
            RuleSummary task = new RuleSummary();
            task.record(RuleId.KV_SHARED_SST, decision, 7);
            sum.add(task);
        }
        assertThat(sum.total(0)).isEqualTo(5);
        assertThat(sum.total(1)).isEqualTo(35);
        assertThat(sum.total(6)).isEqualTo(1);
        assertThat(sum.consistent(new CleanupCounters(5, 1, 0, 7, 0, 0, 0, 0))).isTrue();
        assertThat(sum.consistent(new CleanupCounters(5, 2, 0, 14, 0, 0, 0, 0))).isFalse();
    }

    @Test
    void refusesDryRunDeletionAndMissingTaskResults() {
        ResultAuditLogger audit = new ResultAuditLogger(Collections.emptyMap());
        ScopeCoverageStats scope = ScopeCoverageStats.empty();
        assertThatThrownBy(
                        () ->
                                audit.summary(
                                        new CleanupCounters(0, 0, 0, 0, 0, 1, 0, 0),
                                        scope,
                                        new RuleSummary(),
                                        0,
                                        true))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(
                        () ->
                                audit.summary(
                                        CleanupCounters.empty(),
                                        scope,
                                        new RuleSummary(),
                                        1,
                                        false))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void rejectsAuditIdentifiersThatCouldInjectFields() {
        assertThatThrownBy(
                        () ->
                                new ResultAuditLogger(
                                        Collections.singletonMap(
                                                "audit.run-id", "run action=summary")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageNotContaining("run action=summary");
    }
}
