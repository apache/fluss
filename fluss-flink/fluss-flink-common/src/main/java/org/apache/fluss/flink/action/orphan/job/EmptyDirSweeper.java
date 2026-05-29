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
import org.apache.fluss.flink.action.orphan.audit.AuditLogger;
import org.apache.fluss.flink.action.orphan.fs.SafeDeleter;
import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.shaded.guava32.com.google.common.util.concurrent.RateLimiter;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * End-of-run empty-directory reclaim. Walks every registered "touched" directory tree depth-first
 * and asks {@link SafeDeleter} to remove any empty directories encountered, bottom up; non-empty
 * directories are no-ops via {@code SafeDeleter}'s contract.
 *
 * <p>Run exactly once, after all per-table / per-db cleanup has finished. Any sub-flow that touched
 * a tablet dir or descended into an orphan dir is expected to register that root via {@link
 * #registerTouched(FsPath)} during its own pass, so this single end-of-run sweep can collect the
 * leftover empties without re-walking the live cleanup paths.
 *
 * <p>The sweeper deliberately does not own a {@link FileSystem} — it derives one per-root from the
 * given {@link FsPath} so different remote stores can coexist.
 */
@Internal
public final class EmptyDirSweeper {

    private final boolean dryRun;
    private final AuditLogger audit;
    private final RateLimiter rateLimiter;
    private final Set<FsPath> touchedRoots = new HashSet<FsPath>();

    public EmptyDirSweeper(boolean dryRun, AuditLogger audit) {
        this(dryRun, audit, RateLimiter.create(100.0));
    }

    public EmptyDirSweeper(boolean dryRun, AuditLogger audit, RateLimiter rateLimiter) {
        this.dryRun = dryRun;
        this.audit = audit;
        this.rateLimiter = rateLimiter;
    }

    /**
     * Register a directory root whose subtree should be considered by the final empty-dir sweep.
     * Call sites: every cleanup sub-flow that may have removed files under {@code root} (live log /
     * KV tablet dirs, orphan partition / orphan table dirs). Multiple registrations of the same
     * root are deduplicated; the actual sweep is deferred until {@link #sweep()} runs at end of
     * action.
     */
    public void registerTouched(FsPath root) {
        if (root != null) {
            touchedRoots.add(root);
        }
    }

    /**
     * Sweeps every registered subtree, removing empty leaf directories first and propagating up to
     * the registered root.
     *
     * @return the number of empty directories deleted, or that would be deleted in dry-run mode
     */
    public long sweep() throws IOException {
        long removed = 0L;
        for (FsPath root : touchedRoots) {
            removed += sweepOne(root);
        }
        return removed;
    }

    private long sweepOne(FsPath root) throws IOException {
        FileSystem fs = root.getFileSystem();
        SafeDeleter safeDeleter = new SafeDeleter(fs, dryRun, audit, rateLimiter);
        if (!fs.exists(root)) {
            return 0L;
        }
        // First, gather all directories (root and descendants) in pre-order; then process in
        // reverse order so deeper directories are visited before their parents.
        List<FsPath> dirs = new ArrayList<FsPath>();
        Deque<FsPath> stack = new ArrayDeque<FsPath>();
        stack.push(root);
        while (!stack.isEmpty()) {
            FsPath dir = stack.pop();
            dirs.add(dir);
            FileStatus[] children;
            try {
                children = fs.listStatus(dir);
            } catch (IOException ignored) {
                continue;
            }
            if (children == null) {
                continue;
            }
            for (FileStatus child : children) {
                if (child.isDir()) {
                    stack.push(child.getPath());
                }
            }
        }
        long deleted = 0L;
        if (dryRun) {
            Set<String> virtuallyDeletedDirs = new HashSet<String>();
            for (int i = dirs.size() - 1; i >= 0; i--) {
                FsPath dir = dirs.get(i);
                if (!fs.exists(dir)) {
                    continue;
                }
                if (!isEffectivelyEmpty(fs, dir, virtuallyDeletedDirs)) {
                    continue;
                }
                audit.logWouldDeleteDir(dir);
                virtuallyDeletedDirs.add(dir.toString());
                deleted++;
            }
        } else {
            for (int i = dirs.size() - 1; i >= 0; i--) {
                if (safeDeleter.deleteEmptyDir(dirs.get(i))) {
                    deleted++;
                }
            }
        }
        return deleted;
    }

    private boolean isEffectivelyEmpty(
            FileSystem fs, FsPath dir, Set<String> virtuallyDeletedDirs) {
        FileStatus[] remaining;
        try {
            remaining = fs.listStatus(dir);
        } catch (IOException ignored) {
            return false;
        }
        if (remaining == null) {
            return false;
        }
        for (FileStatus child : remaining) {
            if (!child.isDir() || !virtuallyDeletedDirs.contains(child.getPath().toString())) {
                return false;
            }
        }
        return true;
    }
}
