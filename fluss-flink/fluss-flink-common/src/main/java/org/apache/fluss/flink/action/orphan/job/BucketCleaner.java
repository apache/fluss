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
import org.apache.fluss.flink.action.orphan.fs.FileSystemProbe;
import org.apache.fluss.flink.action.orphan.fs.SafeDeleter;
import org.apache.fluss.flink.action.orphan.rule.BucketActiveRefs;
import org.apache.fluss.flink.action.orphan.rule.Decision;
import org.apache.fluss.flink.action.orphan.rule.FileMeta;
import org.apache.fluss.flink.action.orphan.rule.FileRule;
import org.apache.fluss.flink.action.orphan.rule.MtimePolicy;
import org.apache.fluss.flink.action.orphan.rule.RuleDispatcher;
import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.shaded.guava32.com.google.common.util.concurrent.RateLimiter;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;

/**
 * Per-bucket orphan cleanup for live buckets: walks the provided bucket directories and dispatches
 * each file to the appropriate {@link FileRule} using the caller-supplied active reference set.
 *
 * <p>All deletions go through {@link SafeDeleter} (no recursive deletes). Unknown file types are
 * skipped with an audit warning per the design's "unknown-types-not-deleted" principle.
 */
@Internal
public final class BucketCleaner {

    private final RuleDispatcher dispatcher;
    private final SafeDeleter safeDeleter;
    private final AuditLogger audit;
    private final long cutoffMillis;
    private final RateLimiter remoteFsOpRateLimiter;
    private final boolean dryRun;

    public BucketCleaner(
            RuleDispatcher dispatcher,
            SafeDeleter safeDeleter,
            AuditLogger audit,
            long cutoffMillis,
            RateLimiter remoteFsOpRateLimiter) {
        this(dispatcher, safeDeleter, audit, cutoffMillis, remoteFsOpRateLimiter, false);
    }

    public BucketCleaner(
            RuleDispatcher dispatcher,
            SafeDeleter safeDeleter,
            AuditLogger audit,
            long cutoffMillis,
            RateLimiter remoteFsOpRateLimiter,
            boolean dryRun) {
        this.dispatcher = dispatcher;
        this.safeDeleter = safeDeleter;
        this.audit = audit;
        this.cutoffMillis = cutoffMillis;
        this.remoteFsOpRateLimiter = remoteFsOpRateLimiter;
        this.dryRun = dryRun;
    }

    /** Cleans one bucket's log/kv subtrees using the caller-supplied active reference set. */
    public BucketCleanStats clean(BucketActiveRefs activeRefs, FsPath... bucketDirs)
            throws IOException {
        BucketCleanStats stats = BucketCleanStats.empty();
        for (FsPath bucketDir : bucketDirs) {
            if (bucketDir != null) {
                walkAndCleanDir(bucketDir, activeRefs, stats);
            }
        }
        return stats;
    }

    private void walkAndCleanDir(FsPath root, BucketActiveRefs activeRefs, BucketCleanStats stats)
            throws IOException {
        FileSystem fs = root.getFileSystem();
        Deque<DirVisit> stack = new ArrayDeque<DirVisit>();
        stack.push(new DirVisit(root, false, false, null, true));
        while (!stack.isEmpty()) {
            DirVisit visit = stack.pop();
            if (visit.postOrder) {
                if (visit.oldEnough && !visit.hasRemainingChild) {
                    stats.plannedDirs++;
                    if (dryRun) {
                        audit.logWouldDeleteDir(visit.dir);
                    } else if (safeDeleter.deleteEmptyDir(visit.dir)) {
                        stats.emptyDirsRemoved++;
                    } else {
                        stats.deleteFailures++;
                        visit.markParentRemaining();
                    }
                } else {
                    visit.markParentRemaining();
                }
                continue;
            }
            Optional<FileStatus[]> listing =
                    FileSystemProbe.listStatus(fs, visit.dir, remoteFsOpRateLimiter);
            if (!listing.isPresent()) {
                stats.rules.recordMissingDirectory();
                visit.markParentRemaining();
                continue;
            }
            FileStatus[] children = listing.get();
            if (!visit.root) {
                visit.postOrder = true;
                stack.push(visit);
            }
            for (FileStatus child : children) {
                FsPath childPath = child.getPath();
                if (child.isDir()) {
                    stats.rules.recordDirectoryMtime(child.getModificationTime());
                    stack.push(
                            new DirVisit(
                                    childPath,
                                    false,
                                    MtimePolicy.evaluateInactiveFile(
                                                    child.getModificationTime(), cutoffMillis)
                                            == Decision.DELETE,
                                    visit,
                                    false));
                    continue;
                }
                FileMeta meta =
                        new FileMeta(childPath, child.getLen(), child.getModificationTime());
                FileRule rule = dispatcher.dispatch(meta);
                Decision decision = rule.evaluate(meta, activeRefs, cutoffMillis);
                stats.scannedFiles++;
                stats.rules.record(rule.id(), decision, meta.size());
                switch (decision) {
                    case DELETE:
                        stats.plannedFiles++;
                        stats.plannedBytes += meta.size();
                        if (safeDeleter.deleteFile(meta.path(), decision, rule.id())) {
                            if (!dryRun) {
                                stats.deletedFiles++;
                                stats.bytesReclaimed += meta.size();
                            }
                        } else {
                            stats.deleteFailures++;
                            visit.hasRemainingChild = true;
                        }
                        break;
                    case SKIP_UNKNOWN:
                        audit.logSkipUnknown(meta.path(), rule.id());
                        visit.hasRemainingChild = true;
                        break;
                    case KEEP_ACTIVE:
                    case DEFER:
                        visit.hasRemainingChild = true;
                        // no-op
                        break;
                    default:
                        visit.hasRemainingChild = true;
                        // unknown decision — skip defensively
                        break;
                }
            }
        }
    }

    /** Per-bucket cleanup statistics. */
    public static final class BucketCleanStats {
        public final RuleSummary rules = new RuleSummary();
        public long scannedFiles;
        public long plannedFiles;
        public long plannedDirs;
        public long plannedBytes;
        public long deletedFiles;
        public long emptyDirsRemoved;
        public long deleteFailures;
        public long bytesReclaimed;

        public static BucketCleanStats empty() {
            return new BucketCleanStats();
        }
    }

    private static final class DirVisit {
        private final FsPath dir;
        private boolean postOrder;
        private final boolean oldEnough;
        private final DirVisit parent;
        private final boolean root;
        private boolean hasRemainingChild;

        private DirVisit(
                FsPath dir, boolean postOrder, boolean oldEnough, DirVisit parent, boolean root) {
            this.dir = dir;
            this.postOrder = postOrder;
            this.oldEnough = oldEnough;
            this.parent = parent;
            this.root = root;
        }

        private void markParentRemaining() {
            if (parent != null) {
                parent.hasRemainingChild = true;
            }
        }
    }
}
