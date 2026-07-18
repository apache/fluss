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

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Result of fetching active remote shared SST object names for a single bucket by second-reading
 * the active snapshots' {@code _METADATA} files.
 *
 * <p>Two outcomes:
 *
 * <ul>
 *   <li><b>OK</b>: All (or a safe subset of) metadata reads succeeded; {@link
 *       #sharedSstFileNames()} contains the union of remote shared SST object basenames referenced
 *       by active snapshots.
 *   <li><b>Failed</b>: A non-transient IO error prevented safe determination of the active set; the
 *       caller must skip shared SST cleanup for this bucket to uphold the "may leak, must not
 *       mis-delete" invariant.
 * </ul>
 */
@Internal
public final class KvSharedSstFetchResult {

    private final boolean ok;
    private final Set<String> sharedSstFileNames;
    @Nullable private final String failureReason;

    private KvSharedSstFetchResult(
            boolean ok, Set<String> sharedSstFileNames, @Nullable String failureReason) {
        this.ok = ok;
        this.sharedSstFileNames = Collections.unmodifiableSet(new HashSet<>(sharedSstFileNames));
        this.failureReason = failureReason;
    }

    /** All active snapshot metadata reads succeeded; the active set is complete. */
    public static KvSharedSstFetchResult ok(Set<String> sharedSstFileNames) {
        return new KvSharedSstFetchResult(true, sharedSstFileNames, null);
    }

    /**
     * A non-transient IO error prevented safe determination of the active set. Shared SST cleanup
     * must be skipped for this bucket.
     */
    public static KvSharedSstFetchResult failed(String reason) {
        return new KvSharedSstFetchResult(false, Collections.emptySet(), reason);
    }

    /** Whether all metadata reads completed successfully. */
    public boolean allMetadataReadOk() {
        return ok;
    }

    /**
     * Union of remote shared SST object basenames referenced by active snapshots. Empty when {@link
     * #allMetadataReadOk()} is false.
     */
    public Set<String> sharedSstFileNames() {
        return sharedSstFileNames;
    }

    /** Failure reason for audit logging; {@code null} when {@link #allMetadataReadOk()} is true. */
    @Nullable
    public String failureReason() {
        return failureReason;
    }
}
