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
import org.apache.fluss.flink.action.orphan.rule.BucketActiveRefs;
import org.apache.fluss.flink.action.orphan.rule.Decision;
import org.apache.fluss.flink.action.orphan.rule.FileMeta;
import org.apache.fluss.flink.action.orphan.rule.FileRule;
import org.apache.fluss.flink.action.orphan.rule.RuleDispatcher;
import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.utils.FlussPaths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Per-bucket orphan cleanup for live buckets: walks the provided bucket directories and dispatches
 * each file to the appropriate {@link FileRule} using the caller-supplied active reference set.
 *
 * <p>All deletions go through {@link SafeDeleter} (no recursive deletes). Unknown file types are
 * skipped with an audit warning per the design's "unknown-types-not-deleted" principle.
 */
@Internal
public final class BucketCleaner {

    private static final Logger LOG = LoggerFactory.getLogger(BucketCleaner.class);

    private final RuleDispatcher dispatcher;
    private final SafeDeleter safeDeleter;
    private final AuditLogger audit;
    private final long cutoffMillis;

    public BucketCleaner(
            RuleDispatcher dispatcher,
            SafeDeleter safeDeleter,
            AuditLogger audit,
            long cutoffMillis) {
        this.dispatcher = dispatcher;
        this.safeDeleter = safeDeleter;
        this.audit = audit;
        this.cutoffMillis = cutoffMillis;
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
        if (!fs.exists(root)) {
            return;
        }
        Deque<FsPath> stack = new ArrayDeque<FsPath>();
        stack.push(root);
        while (!stack.isEmpty()) {
            FsPath dir = stack.pop();
            FileStatus[] children;
            try {
                children = fs.listStatus(dir);
            } catch (IOException e) {
                LOG.warn("Failed to list directory: {}", dir, e);
                continue;
            }
            if (children == null) {
                continue;
            }
            for (FileStatus child : children) {
                FsPath childPath = child.getPath();
                if (child.isDir()) {
                    if (FlussPaths.REMOTE_KV_SNAPSHOT_SHARED_DIR.equals(childPath.getName())) {
                        continue;
                    }
                    stack.push(childPath);
                    continue;
                }
                if (childPath.getName().startsWith(".")) {
                    continue;
                }
                FileMeta meta =
                        new FileMeta(childPath, child.getLen(), child.getModificationTime());
                FileRule rule = dispatcher.dispatch(meta);
                Decision decision = rule.evaluate(meta, activeRefs, cutoffMillis);
                stats.scanned++;
                switch (decision) {
                    case DELETE:
                        if (safeDeleter.deleteFile(meta.path(), decision, rule.id())) {
                            stats.deleted++;
                            stats.bytesReclaimed += meta.size();
                        } else {
                            stats.deleteFailures++;
                        }
                        break;
                    case SKIP_UNKNOWN:
                        audit.logSkipUnknown(meta.path(), rule.id());
                        break;
                    case KEEP_ACTIVE:
                    case DEFER:
                        // no-op
                        break;
                    default:
                        // unknown decision — skip defensively
                        break;
                }
            }
        }
    }

    /** Per-bucket cleanup statistics. */
    public static final class BucketCleanStats {
        public long scanned;
        public long deleted;
        public long deleteFailures;
        public long bytesReclaimed;

        public static BucketCleanStats empty() {
            return new BucketCleanStats();
        }
    }
}
