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

import org.apache.fluss.annotation.Internal;

import java.io.Serializable;

/** Fixed-size aggregate of scope-enumeration coverage. */
@Internal
public final class ScopeCoverageStats implements Serializable {

    private static final long serialVersionUID = 1L;

    private long snapshotReadFailures;

    public void recordSnapshotReadFailure() {
        snapshotReadFailures++;
    }

    public long snapshotReadFailures() {
        return snapshotReadFailures;
    }

    private long bucketTasks;
    private long orphanDirTasks;

    public void recordTask(CleanTask task) {
        if (task instanceof BucketCleanTask) {
            bucketTasks++;
        } else if (task instanceof OrphanDirCleanTask) {
            orphanDirTasks++;
        }
    }

    public long bucketTasks() {
        return bucketTasks;
    }

    public long orphanDirTasks() {
        return orphanDirTasks;
    }

    private long expectedTargets;
    private long expectedLogBuckets;
    private long expectedKvBuckets;
    private long coveredLogBuckets;
    private long coveredKvBuckets;
    private long disappearedTargets;
    private long disappearedPartitionTargets;
    private long disappearedTableTargets;
    private long disappearedLogBuckets;
    private long disappearedKvBuckets;
    private long logResolvedBuckets;
    private long logNoManifestBuckets;
    private long logReadFailedBuckets;
    private long logUnavailableBuckets;
    private long kvActiveBuckets;
    private long kvEmptyBuckets;
    private long kvUnavailableBuckets;
    private long outOfScopeLogBuckets;
    private long outOfScopeKvBuckets;
    private long tasksEmitted;
    private long incompleteTargets;
    private long metadataFailures;

    public static ScopeCoverageStats empty() {
        return new ScopeCoverageStats();
    }

    public void add(ScopeTargetCoverage target) {
        expectedTargets++;
        expectedLogBuckets += target.expectedBuckets();
        expectedKvBuckets += target.kvApplicable() ? target.expectedBuckets() : 0L;
        coveredLogBuckets += target.coveredLogBuckets();
        coveredKvBuckets += target.coveredKvBuckets();
        logResolvedBuckets += target.logResolvedBuckets();
        logNoManifestBuckets += target.logNoManifestBuckets();
        logReadFailedBuckets += target.logReadFailedBuckets();
        logUnavailableBuckets += target.logUnavailableBuckets();
        kvActiveBuckets += target.kvActiveBuckets();
        kvEmptyBuckets += target.kvEmptyBuckets();
        kvUnavailableBuckets += target.kvUnavailableBuckets();
        outOfScopeLogBuckets += target.outOfScopeLogBuckets();
        outOfScopeKvBuckets += target.outOfScopeKvBuckets();
        tasksEmitted += target.tasksEmitted();
        if (target.disappeared()) {
            disappearedTargets++;
            disappearedLogBuckets += target.expectedBuckets();
            disappearedKvBuckets += target.kvApplicable() ? target.expectedBuckets() : 0L;
            if (target.partitionDisappeared()) {
                disappearedPartitionTargets++;
            } else {
                disappearedTableTargets++;
            }
        }
        if (!target.complete()) {
            incompleteTargets++;
        }
    }

    public void add(ScopeCoverageStats other) {
        snapshotReadFailures += other.snapshotReadFailures;
        bucketTasks += other.bucketTasks;
        orphanDirTasks += other.orphanDirTasks;
        expectedTargets += other.expectedTargets;
        expectedLogBuckets += other.expectedLogBuckets;
        expectedKvBuckets += other.expectedKvBuckets;
        coveredLogBuckets += other.coveredLogBuckets;
        coveredKvBuckets += other.coveredKvBuckets;
        disappearedTargets += other.disappearedTargets;
        disappearedPartitionTargets += other.disappearedPartitionTargets;
        disappearedTableTargets += other.disappearedTableTargets;
        disappearedLogBuckets += other.disappearedLogBuckets;
        disappearedKvBuckets += other.disappearedKvBuckets;
        logResolvedBuckets += other.logResolvedBuckets;
        logNoManifestBuckets += other.logNoManifestBuckets;
        logReadFailedBuckets += other.logReadFailedBuckets;
        logUnavailableBuckets += other.logUnavailableBuckets;
        kvActiveBuckets += other.kvActiveBuckets;
        kvEmptyBuckets += other.kvEmptyBuckets;
        kvUnavailableBuckets += other.kvUnavailableBuckets;
        outOfScopeLogBuckets += other.outOfScopeLogBuckets;
        outOfScopeKvBuckets += other.outOfScopeKvBuckets;
        tasksEmitted += other.tasksEmitted;
        incompleteTargets += other.incompleteTargets;
        metadataFailures += other.metadataFailures;
    }

    public void recordMetadataFailure() {
        metadataFailures++;
    }

    public long expectedTargets() {
        return expectedTargets;
    }

    public long expectedLogBuckets() {
        return expectedLogBuckets;
    }

    public long expectedKvBuckets() {
        return expectedKvBuckets;
    }

    public long disappearedTargets() {
        return disappearedTargets;
    }

    public long disappearedPartitionTargets() {
        return disappearedPartitionTargets;
    }

    public long disappearedTableTargets() {
        return disappearedTableTargets;
    }

    public long disappearedLogBuckets() {
        return disappearedLogBuckets;
    }

    public long disappearedKvBuckets() {
        return disappearedKvBuckets;
    }

    public long logResolvedBuckets() {
        return logResolvedBuckets;
    }

    public long logNoManifestBuckets() {
        return logNoManifestBuckets;
    }

    public long logReadFailedBuckets() {
        return logReadFailedBuckets;
    }

    public long logUnavailableBuckets() {
        return logUnavailableBuckets;
    }

    public long kvActiveBuckets() {
        return kvActiveBuckets;
    }

    public long kvEmptyBuckets() {
        return kvEmptyBuckets;
    }

    public long kvUnavailableBuckets() {
        return kvUnavailableBuckets;
    }

    public long outOfScopeLogBuckets() {
        return outOfScopeLogBuckets;
    }

    public long outOfScopeKvBuckets() {
        return outOfScopeKvBuckets;
    }

    public long tasksEmitted() {
        return tasksEmitted;
    }

    public long incompleteTargets() {
        return incompleteTargets;
    }

    public long metadataFailures() {
        return metadataFailures + snapshotReadFailures + logReadFailedBuckets;
    }

    public long rpcFailures() {
        return metadataFailures + logUnavailableBuckets + kvUnavailableBuckets;
    }

    public boolean countersConsistent() {
        return expectedLogBuckets == coveredLogBuckets && expectedKvBuckets == coveredKvBuckets;
    }

    public boolean coverageComplete() {
        return incompleteTargets == 0L
                && metadataFailures == 0L
                && snapshotReadFailures == 0L
                && countersConsistent();
    }
}
