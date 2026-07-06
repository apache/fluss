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

package org.apache.fluss.flink.action.orphan.rule;

import org.apache.fluss.annotation.Internal;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/** Immutable view of all active references for a single bucket / table partition. */
@Internal
public final class BucketActiveRefs {

    private static final BucketActiveRefs EMPTY =
            new BucketActiveRefs(
                    Collections.emptySet(),
                    Collections.emptySet(),
                    Collections.emptySet(),
                    Collections.emptySet());

    private final Set<String> logSegmentRelativePaths;
    private final Set<String> kvActiveSnapDirs;
    private final Set<String> logActiveManifestPaths;
    private final Set<String> kvSharedSstFileNames;

    public BucketActiveRefs(
            Set<String> logSegmentRelativePaths,
            Set<String> kvActiveSnapDirs,
            Set<String> logActiveManifestPaths) {
        this(
                logSegmentRelativePaths,
                kvActiveSnapDirs,
                logActiveManifestPaths,
                Collections.emptySet());
    }

    public BucketActiveRefs(
            Set<String> logSegmentRelativePaths,
            Set<String> kvActiveSnapDirs,
            Set<String> logActiveManifestPaths,
            Set<String> kvSharedSstFileNames) {
        this.logSegmentRelativePaths =
                Collections.unmodifiableSet(new HashSet<>(logSegmentRelativePaths));
        this.kvActiveSnapDirs = Collections.unmodifiableSet(new HashSet<>(kvActiveSnapDirs));
        this.logActiveManifestPaths =
                Collections.unmodifiableSet(new HashSet<>(logActiveManifestPaths));
        this.kvSharedSstFileNames =
                Collections.unmodifiableSet(new HashSet<>(kvSharedSstFileNames));
    }

    public static BucketActiveRefs empty() {
        return EMPTY;
    }

    public Set<String> logSegmentRelativePaths() {
        return logSegmentRelativePaths;
    }

    /**
     * Returns the set of active {@code snap-<id>} directory names for the bucket.
     *
     * <p>The set is the union of two server-side categories the {@code ListKvSnapshots} RPC emits
     * as one flat list (client does not distinguish):
     *
     * <ul>
     *   <li>RETAINED — the most recent N completed snapshots kept per the retention window.
     *   <li>STILL_IN_USE — snapshots pinned by an active lease; emitted unconditionally even when
     *       the corresponding ZK znode has been removed, on the principle "may over-count active,
     *       must never under-count."
     * </ul>
     *
     * <p>A KV snap-private file is preserved iff its parent directory's name is in this set.
     */
    public Set<String> kvActiveSnapDirs() {
        return kvActiveSnapDirs;
    }

    /**
     * Returns the set of active log manifest paths reported by {@code ListRemoteLogManifests}. The
     * "current" manifest for a bucket is always also a member of this set, so {@link
     * LogManifestRule} only needs to check this single collection.
     */
    public Set<String> logActiveManifestPaths() {
        return logActiveManifestPaths;
    }

    /**
     * Returns the set of active shared SST file names (basenames only, e.g. {@code
     * "abc-def-0.sst"}) for the bucket. Built from the union of {@code
     * shared_file_handles[*].local_path} across all active snapshots' {@code _METADATA} files.
     *
     * <p>An empty set means no shared SST active set could be determined (either all active
     * snapshots have zero shared files — which is unusual for PK tables — or the metadata read
     * failed). Rules must treat an empty set conservatively (keep all files).
     */
    public Set<String> kvSharedSstFileNames() {
        return kvSharedSstFileNames;
    }
}
