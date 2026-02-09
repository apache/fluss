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

package org.apache.fluss.client.admin;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.client.metadata.AcquireKvSnapshotLeaseResult;
import org.apache.fluss.metadata.TableBucket;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Represents a lease for managing KV snapshots. A lease allows acquiring, renewing, releasing, and
 * dropping snapshot references under a time-bound agreement.
 *
 * @since 0.9
 */
@PublicEvolving
public interface KvSnapshotLease {

    /** Returns the unique identifier of this lease. */
    String leaseId();

    /** Returns the lease duration in milliseconds. */
    long leaseDurationMs();

    /**
     * Acquires snapshots for the specified table buckets under this lease.
     *
     * @param snapshotIds mapping from table buckets to desired snapshot IDs
     * @return a future containing the result of the acquisition
     */
    CompletableFuture<AcquireKvSnapshotLeaseResult> acquireSnapshots(
            Map<TableBucket, Long> snapshotIds);

    /**
     * Renews the lease, extending its validity period.
     *
     * @return a future that completes when the renewal is acknowledged
     */
    CompletableFuture<Void> renew();

    /**
     * Releases snapshots for the specified table buckets, freeing associated resources.
     *
     * @param bucketsToRelease the set of table buckets whose snapshots should be released
     * @return a future that completes when the release is processed
     */
    CompletableFuture<Void> releaseSnapshots(Set<TableBucket> bucketsToRelease);

    /**
     * Drops the entire lease, releasing all held snapshots and invalidating the lease.
     *
     * @return a future that completes when the lease is fully dropped
     */
    CompletableFuture<Void> dropLease();
}
