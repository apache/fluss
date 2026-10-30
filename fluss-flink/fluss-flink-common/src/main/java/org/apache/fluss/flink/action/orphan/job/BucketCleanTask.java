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

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.Set;

/**
 * Work item for a single bucket's file-level cleanup. Carries everything needed to execute cleanup
 * without coordinator interaction: FS paths, manifest locations for second-read, and the
 * already-resolved KV active snapshot directory names.
 */
@Internal
public final class BucketCleanTask implements CleanTask {

    private static final long serialVersionUID = 1L;

    @Nullable private final String logTabletDir;
    @Nullable private final String kvTabletDir;
    private final Set<String> logSegmentRelativePaths;
    private final Set<String> logActiveManifestPaths;
    private final Set<String> kvActiveSnapDirs;
    private final Set<String> kvSharedSstFileNames;
    private final boolean kvSharedSstRefsComplete;
    private final long cutoffMillis;
    private final boolean dryRun;
    private final boolean allowDeleteManifest;

    public BucketCleanTask(
            @Nullable String logTabletDir,
            @Nullable String kvTabletDir,
            Set<String> logSegmentRelativePaths,
            Set<String> logActiveManifestPaths,
            Set<String> kvActiveSnapDirs,
            Set<String> kvSharedSstFileNames,
            boolean kvSharedSstRefsComplete,
            long cutoffMillis,
            boolean dryRun,
            boolean allowDeleteManifest) {
        this.logTabletDir = logTabletDir;
        this.kvTabletDir = kvTabletDir;
        this.logSegmentRelativePaths = new HashSet<>(logSegmentRelativePaths);
        this.logActiveManifestPaths = new HashSet<>(logActiveManifestPaths);
        this.kvActiveSnapDirs = new HashSet<>(kvActiveSnapDirs);
        this.kvSharedSstFileNames = new HashSet<>(kvSharedSstFileNames);
        this.kvSharedSstRefsComplete = kvSharedSstRefsComplete;
        this.cutoffMillis = cutoffMillis;
        this.dryRun = dryRun;
        this.allowDeleteManifest = allowDeleteManifest;
    }

    @Nullable
    public String logTabletDir() {
        return logTabletDir;
    }

    @Nullable
    public String kvTabletDir() {
        return kvTabletDir;
    }

    /** Active log segment relative paths (already resolved from manifests in Stage 1). */
    public Set<String> logSegmentRelativePaths() {
        return logSegmentRelativePaths;
    }

    /** Active manifest paths (already resolved from RPC in Stage 1). */
    public Set<String> logActiveManifestPaths() {
        return logActiveManifestPaths;
    }

    /**
     * KV active snapshot directory names (already resolved from RPC, no further FS read needed).
     */
    public Set<String> kvActiveSnapDirs() {
        return kvActiveSnapDirs;
    }

    /**
     * Active remote shared SST object names (basenames) resolved from active snapshots' {@code
     * _METADATA} files.
     */
    public Set<String> kvSharedSstFileNames() {
        return kvSharedSstFileNames;
    }

    /** Whether the shared SST reference set is authoritative, including a proven-empty set. */
    public boolean kvSharedSstRefsComplete() {
        return kvSharedSstRefsComplete;
    }

    public long cutoffMillis() {
        return cutoffMillis;
    }

    public boolean dryRun() {
        return dryRun;
    }

    public boolean allowDeleteManifest() {
        return allowDeleteManifest;
    }
}
