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
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.TableNotExistException;

/** Mutable coverage for one table or partition target during scope enumeration. */
@Internal
public final class ScopeTargetCoverage {

    private final long expectedBuckets;
    private final boolean kvApplicable;
    private long coveredLogBuckets;
    private long coveredKvBuckets;
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
    private boolean disappeared;
    private boolean partitionDisappeared;
    private boolean complete;

    private ScopeTargetCoverage(long expectedBuckets, boolean kvApplicable) {
        if (expectedBuckets < 0L) {
            throw new IllegalArgumentException("expectedBuckets must be non-negative");
        }
        this.expectedBuckets = expectedBuckets;
        this.kvApplicable = kvApplicable;
    }

    public static ScopeTargetCoverage forTarget(long expectedBuckets, boolean kvApplicable) {
        return new ScopeTargetCoverage(expectedBuckets, kvApplicable);
    }

    public void disappeared(Throwable cause) {
        if (!(cause instanceof PartitionNotExistException)
                && !(cause instanceof TableNotExistException)) {
            throw new IllegalArgumentException("cause must identify a disappeared target");
        }
        disappearedDuringEnumeration(cause instanceof PartitionNotExistException);
    }

    public void disappearedDuringEnumeration(boolean partition) {
        disappeared = true;
        partitionDisappeared = partition;
        coveredLogBuckets = expectedBuckets;
        coveredKvBuckets = kvApplicable ? expectedBuckets : 0L;
        complete = true;
    }

    public void logResolved() {
        logResolvedBuckets++;
    }

    public void logNoManifest() {
        logNoManifestBuckets++;
    }

    public void logReadFailed() {
        logReadFailedBuckets++;
    }

    public void logUnavailable() {
        logUnavailableBuckets = expectedBuckets;
    }

    public void kvActive() {
        kvActiveBuckets++;
    }

    public void kvEmpty() {
        kvEmptyBuckets++;
    }

    public void kvUnavailable() {
        if (kvApplicable) {
            kvUnavailableBuckets = expectedBuckets;
        }
    }

    public void taskEmitted() {
        tasksEmitted++;
    }

    public void outOfScope() {
        outOfScopeLogBuckets = expectedBuckets;
        if (kvApplicable) {
            outOfScopeKvBuckets = expectedBuckets;
        }
    }

    public ScopeTargetCoverage finish() {
        if (!disappeared) {
            coveredLogBuckets =
                    logResolvedBuckets
                            + logNoManifestBuckets
                            + logReadFailedBuckets
                            + logUnavailableBuckets
                            + outOfScopeLogBuckets;
            coveredKvBuckets =
                    kvActiveBuckets + kvEmptyBuckets + kvUnavailableBuckets + outOfScopeKvBuckets;
            complete =
                    coveredLogBuckets == expectedBuckets
                            && coveredKvBuckets == (kvApplicable ? expectedBuckets : 0L)
                            && logReadFailedBuckets == 0L
                            && logUnavailableBuckets == 0L
                            && kvUnavailableBuckets == 0L
                            && outOfScopeLogBuckets == 0L
                            && outOfScopeKvBuckets == 0L;
        }
        return this;
    }

    long expectedBuckets() {
        return expectedBuckets;
    }

    boolean kvApplicable() {
        return kvApplicable;
    }

    long coveredLogBuckets() {
        return coveredLogBuckets;
    }

    long coveredKvBuckets() {
        return coveredKvBuckets;
    }

    long logResolvedBuckets() {
        return logResolvedBuckets;
    }

    long logNoManifestBuckets() {
        return logNoManifestBuckets;
    }

    long logReadFailedBuckets() {
        return logReadFailedBuckets;
    }

    long logUnavailableBuckets() {
        return logUnavailableBuckets;
    }

    long kvActiveBuckets() {
        return kvActiveBuckets;
    }

    long kvEmptyBuckets() {
        return kvEmptyBuckets;
    }

    long kvUnavailableBuckets() {
        return kvUnavailableBuckets;
    }

    long outOfScopeLogBuckets() {
        return outOfScopeLogBuckets;
    }

    long outOfScopeKvBuckets() {
        return outOfScopeKvBuckets;
    }

    long tasksEmitted() {
        return tasksEmitted;
    }

    boolean disappeared() {
        return disappeared;
    }

    boolean partitionDisappeared() {
        return partitionDisappeared;
    }

    boolean complete() {
        return complete;
    }
}
