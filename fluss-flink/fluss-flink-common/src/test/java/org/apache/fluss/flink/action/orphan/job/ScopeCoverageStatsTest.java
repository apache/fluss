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

import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.TableNotExistException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ScopeCoverageStatsTest {

    @Test
    void bucketsWithConfirmedEmptyLogReferencesCompleteCoverage() {
        ScopeCoverageStats stats = new ScopeCoverageStats();
        ScopeTargetCoverage target = ScopeTargetCoverage.forTarget(1L, false);
        target.logNoManifest();
        target.taskEmitted();
        stats.add(target.finish());

        assertThat(stats.logNoManifestBuckets()).isEqualTo(1L);
        assertThat(stats.tasksEmitted()).isEqualTo(1L);
        assertThat(stats.countersConsistent()).isTrue();
        assertThat(stats.coverageComplete()).isTrue();
    }

    @Test
    void distinguishesMetadataReadsFromRpcFailures() {
        ScopeCoverageStats stats = new ScopeCoverageStats();
        stats.recordSnapshotReadFailure();
        assertThat(stats.metadataFailures()).isEqualTo(1);
        assertThat(stats.rpcFailures()).isZero();
        assertThat(stats.coverageComplete()).isFalse();
        stats.recordMetadataFailure();
        assertThat(stats.metadataFailures()).isEqualTo(2);
        assertThat(stats.rpcFailures()).isEqualTo(1);
    }

    @Test
    void disappearedPartitionCompletesLogAndKvCoverage() {
        ScopeCoverageStats stats = new ScopeCoverageStats();
        ScopeTargetCoverage target = ScopeTargetCoverage.forTarget(3L, true);

        target.disappeared(new PartitionNotExistException("gone"));
        stats.add(target.finish());

        assertThat(stats.expectedTargets()).isEqualTo(1L);
        assertThat(stats.expectedLogBuckets()).isEqualTo(3L);
        assertThat(stats.expectedKvBuckets()).isEqualTo(3L);
        assertThat(stats.disappearedTargets()).isEqualTo(1L);
        assertThat(stats.disappearedPartitionTargets()).isEqualTo(1L);
        assertThat(stats.disappearedLogBuckets()).isEqualTo(3L);
        assertThat(stats.disappearedKvBuckets()).isEqualTo(3L);
        assertThat(stats.incompleteTargets()).isZero();
        assertThat(stats.countersConsistent()).isTrue();
        assertThat(stats.coverageComplete()).isTrue();
    }

    @Test
    void disappearedLogOnlyTableDoesNotInventKvCoverage() {
        ScopeCoverageStats stats = new ScopeCoverageStats();
        ScopeTargetCoverage target = ScopeTargetCoverage.forTarget(2L, false);

        target.disappeared(new TableNotExistException("gone"));
        stats.add(target.finish());

        assertThat(stats.expectedLogBuckets()).isEqualTo(2L);
        assertThat(stats.expectedKvBuckets()).isZero();
        assertThat(stats.disappearedTableTargets()).isEqualTo(1L);
        assertThat(stats.disappearedLogBuckets()).isEqualTo(2L);
        assertThat(stats.disappearedKvBuckets()).isZero();
        assertThat(stats.countersConsistent()).isTrue();
        assertThat(stats.coverageComplete()).isTrue();
    }

    @Test
    void unavailableMetadataIsConservedButLeavesCoverageIncomplete() {
        ScopeCoverageStats stats = new ScopeCoverageStats();
        ScopeTargetCoverage target = ScopeTargetCoverage.forTarget(2L, true);

        target.logUnavailable();
        target.kvUnavailable();
        stats.add(target.finish());

        assertThat(stats.logUnavailableBuckets()).isEqualTo(2L);
        assertThat(stats.kvUnavailableBuckets()).isEqualTo(2L);
        assertThat(stats.incompleteTargets()).isEqualTo(1L);
        assertThat(stats.countersConsistent()).isTrue();
        assertThat(stats.coverageComplete()).isFalse();
    }

    @Test
    void failedManifestReadsConserveCountersButLeaveCoverageIncomplete() {
        ScopeCoverageStats stats = new ScopeCoverageStats();
        ScopeTargetCoverage target = ScopeTargetCoverage.forTarget(3L, true);

        target.logResolved();
        target.logNoManifest();
        target.logReadFailed();
        target.kvActive();
        target.kvEmpty();
        target.kvEmpty();
        target.taskEmitted();
        target.taskEmitted();
        stats.add(target.finish());

        assertThat(stats.logResolvedBuckets()).isEqualTo(1L);
        assertThat(stats.logNoManifestBuckets()).isEqualTo(1L);
        assertThat(stats.logReadFailedBuckets()).isEqualTo(1L);
        assertThat(stats.kvActiveBuckets()).isEqualTo(1L);
        assertThat(stats.kvEmptyBuckets()).isEqualTo(2L);
        assertThat(stats.tasksEmitted()).isEqualTo(2L);
        assertThat(stats.countersConsistent()).isTrue();
        assertThat(stats.coverageComplete()).isFalse();
    }

    @Test
    void unresolvedMetadataScopePreventsCompleteCoverage() {
        ScopeCoverageStats stats = new ScopeCoverageStats();

        stats.recordMetadataFailure();

        assertThat(stats.metadataFailures()).isEqualTo(1L);
        assertThat(stats.countersConsistent()).isTrue();
        assertThat(stats.coverageComplete()).isFalse();
    }
}
