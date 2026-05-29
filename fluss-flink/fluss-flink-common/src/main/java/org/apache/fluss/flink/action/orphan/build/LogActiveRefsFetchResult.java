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

package org.apache.fluss.flink.action.orphan.build;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.flink.action.orphan.rule.BucketActiveRefs;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Result of log active-refs fetch for one {@code (tableId, partitionId|null)} target.
 *
 * <p>The result is split along two orthogonal axes so each axis can be queried independently:
 *
 * <ul>
 *   <li><b>Per-target</b>: {@link #listOk()} reports whether the {@code LIST_REMOTE_LOG_MANIFESTS}
 *       RPC succeeded. When it fails the per-bucket axis is meaningless and the caller should emit
 *       a single per-target skip and bypass the per-bucket loop entirely.
 *   <li><b>Per-bucket</b>: {@link #statusFor(int)} reports one of {@link
 *       ManifestReadStatus#RESOLVED}, {@link ManifestReadStatus#READ_FAILED}, or {@link
 *       ManifestReadStatus#NOT_LISTED} for every bucket enumerated from table metadata. Only
 *       meaningful when {@link #listOk()} is true.
 * </ul>
 */
@Internal
public final class LogActiveRefsFetchResult {

    /** Per-bucket outcome (only meaningful when {@link #listOk()} is true). */
    public enum ManifestReadStatus {
        /** The RPC returned an entry for this bucket and its manifest was read successfully. */
        RESOLVED,
        /**
         * Per-bucket manifest second-read failed (FileNotFound from manifest upsert race, or other
         * IO failure). The failing bucket is skipped for this round; recovery is by the next
         * cleanup round.
         */
        READ_FAILED,
        /**
         * Table metadata enumerates the bucket, but the {@code LIST_REMOTE_LOG_MANIFESTS} response
         * did not include an entry for it — typically because the bucket has not yet committed any
         * remote manifest (e.g. log tiering has not produced one), or an occasional server-side
         * underreport (e.g. partial ZK read). Cleanup has nothing to clean for this bucket.
         */
        NOT_LISTED
    }

    private final RpcListStatus list;
    private final Map<Integer, BucketActiveRefs> resolved;
    private final Map<Integer, String> readFailures;

    private LogActiveRefsFetchResult(
            RpcListStatus list,
            Map<Integer, BucketActiveRefs> resolved,
            Map<Integer, String> readFailures) {
        this.list = list;
        this.resolved = Collections.unmodifiableMap(new HashMap<>(resolved));
        this.readFailures = Collections.unmodifiableMap(new HashMap<>(readFailures));
    }

    /**
     * Result for a target whose {@code LIST_REMOTE_LOG_MANIFESTS} RPC failed and exhausted retries.
     */
    public static LogActiveRefsFetchResult listFailed(String reason) {
        return new LogActiveRefsFetchResult(
                RpcListStatus.listFailed(reason), Collections.emptyMap(), Collections.emptyMap());
    }

    /**
     * Result for a target whose {@code LIST_REMOTE_LOG_MANIFESTS} RPC succeeded. {@code resolved}
     * carries the per-bucket active refs for RESOLVED buckets; {@code readFailures} carries the
     * per-bucket failure reasons for READ_FAILED buckets. Any bucket not present in either map is
     * reported as {@link ManifestReadStatus#NOT_LISTED}.
     */
    static LogActiveRefsFetchResult ofPerBucket(
            Map<Integer, BucketActiveRefs> resolved, Map<Integer, String> readFailures) {
        return new LogActiveRefsFetchResult(RpcListStatus.ok(), resolved, readFailures);
    }

    /** Whether the per-target {@code LIST_REMOTE_LOG_MANIFESTS} RPC succeeded. */
    public boolean listOk() {
        return list.isOk();
    }

    /** Reason the per-target RPC failed; {@code null} when {@link #listOk()} is true. */
    @Nullable
    public String listFailureReason() {
        return list.reason();
    }

    /**
     * Per-bucket manifest read status for a bucket enumerated from table metadata. Callers must
     * first check {@link #listOk()} and skip the per-bucket loop entirely when it is false.
     */
    public ManifestReadStatus statusFor(int bucketId) {
        if (!list.isOk()) {
            throw new IllegalStateException("Per-bucket status is not available when listOk=false");
        }
        if (resolved.containsKey(bucketId)) {
            return ManifestReadStatus.RESOLVED;
        }
        if (readFailures.containsKey(bucketId)) {
            return ManifestReadStatus.READ_FAILED;
        }
        return ManifestReadStatus.NOT_LISTED;
    }

    /** Active refs for a RESOLVED bucket. */
    public BucketActiveRefs activeRefsOf(int bucketId) {
        BucketActiveRefs activeRefs = resolved.get(bucketId);
        if (activeRefs == null) {
            throw new IllegalStateException("Bucket " + bucketId + " is not RESOLVED");
        }
        return activeRefs;
    }

    /** Failure reason for a READ_FAILED bucket. */
    public String readFailureReason(int bucketId) {
        String reason = readFailures.get(bucketId);
        if (reason == null) {
            throw new IllegalStateException("Bucket " + bucketId + " is not READ_FAILED");
        }
        return reason;
    }

    /**
     * Bucket ids for which the RPC returned an entry (i.e. RESOLVED or READ_FAILED). Buckets
     * enumerated from table metadata but absent from this set are {@link
     * ManifestReadStatus#NOT_LISTED}.
     */
    public Set<Integer> respondedBucketIds() {
        Set<Integer> ids = new HashSet<>(resolved.keySet());
        ids.addAll(readFailures.keySet());
        return Collections.unmodifiableSet(ids);
    }
}
