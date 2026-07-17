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
import org.apache.fluss.config.Configuration;
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
import org.apache.fluss.shaded.guava32.com.google.common.util.concurrent.RateLimiter;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;

/**
 * Stage 2 of the orphan files cleanup job. Runs at user-configured parallelism (N) and performs
 * pure FS operations — no coordinator RPC interaction.
 *
 * <p>Each subtask processes assigned {@link CleanTask} items serially:
 *
 * <ul>
 *   <li>{@link BucketCleanTask}: second-reads manifests from object storage to build the active
 *       reference set, then walks log/kv directories and deletes orphan files and old empty child
 *       directories.
 *   <li>{@link OrphanDirCleanTask}: recursively walks the orphan directory and deletes all files
 *       older than the cutoff, then removes old empty directories bottom-up.
 * </ul>
 *
 * <p>Each task emits a single {@link CleanStats} containing scalar counters. Remote filesystem
 * operation rate is limited per-subtask: {@code configuredRate / runtimeParallelism}. The serial
 * processing within each subtask guarantees no concurrent throttler access.
 */
@Internal
public final class ScanAndCleanFunction extends ProcessFunction<CleanTask, CleanStats> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ScanAndCleanFunction.class);

    private final long remoteFsOpRateLimitPerSecond;
    private final Map<String, String> extraConfigs;

    private transient AuditLogger audit;
    private transient RateLimiter remoteFsOpRateLimiter;

    public ScanAndCleanFunction(
            long remoteFsOpRateLimitPerSecond, Map<String, String> extraConfigs) {
        this.remoteFsOpRateLimitPerSecond = remoteFsOpRateLimitPerSecond;
        this.extraConfigs = extraConfigs;
    }

    @Override
    public void open(org.apache.flink.api.common.functions.OpenContext openContext)
            throws Exception {
        super.open(openContext);
        if (!extraConfigs.isEmpty()) {
            FileSystem.initialize(Configuration.fromMap(extraConfigs), null);
        }
        audit = new AuditLogger();
        int parallelism = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();
        int subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        // Distribute the configured rate as base + 1 extra for the first `remainder` subtasks.
        // Flink does not provide a cross-JVM limiter here, so this is a best-effort job-level
        // target. Each subtask gets at least 1/s; if parallelism exceeds the configured rate, the
        // effective aggregate can exceed the target by that floor.
        remoteFsOpRateLimiter =
                RateLimiter.create(
                        perSubtaskRate(remoteFsOpRateLimitPerSecond, parallelism, subtaskIndex));
    }

    @Override
    public void processElement(CleanTask task, Context ctx, Collector<CleanStats> out)
            throws Exception {
        if (task instanceof BucketCleanTask) {
            out.collect(processBucketTask((BucketCleanTask) task));
        } else if (task instanceof OrphanDirCleanTask) {
            out.collect(processOrphanDirTask((OrphanDirCleanTask) task));
        }
    }

    // -------------------------------------------------------------------------
    // BucketCleanTask processing
    // -------------------------------------------------------------------------

    private CleanStats processBucketTask(BucketCleanTask task) throws IOException {
        FsPath logDir = task.logTabletDir() != null ? new FsPath(task.logTabletDir()) : null;
        FsPath kvDir = task.kvTabletDir() != null ? new FsPath(task.kvTabletDir()) : null;

        FsPath anyDir = logDir != null ? logDir : kvDir;
        if (anyDir == null) {
            return CleanStats.empty();
        }

        BucketActiveRefs activeRefs =
                new BucketActiveRefs(
                        task.logSegmentRelativePaths(),
                        task.kvActiveSnapDirs(),
                        task.logActiveManifestPaths());
        RuleDispatcher dispatcher = new RuleDispatcher(task.allowDeleteManifest());
        SafeDeleter safeDeleter = createSafeDeleter(anyDir.getFileSystem(), task.dryRun());
        BucketCleaner cleaner =
                new BucketCleaner(
                        dispatcher, safeDeleter, audit, task.cutoffMillis(), remoteFsOpRateLimiter);

        BucketCleaner.BucketCleanStats bucketStats = cleaner.clean(activeRefs, logDir, kvDir);

        return new CleanStats(
                bucketStats.scanned,
                bucketStats.deleted,
                bucketStats.emptyDirsRemoved,
                bucketStats.deleteFailures,
                bucketStats.bytesReclaimed);
    }

    // -------------------------------------------------------------------------
    // OrphanDirCleanTask processing
    // -------------------------------------------------------------------------

    private CleanStats processOrphanDirTask(OrphanDirCleanTask task) throws IOException {
        FsPath dirPath = new FsPath(task.dirPath());
        FileSystem fs = dirPath.getFileSystem();
        remoteFsOpRateLimiter.acquire();
        if (!fs.exists(dirPath)) {
            return CleanStats.empty();
        }

        SafeDeleter safeDeleter = createSafeDeleter(fs, task.dryRun());
        RuleDispatcher dispatcher = new RuleDispatcher(task.allowDeleteManifest());

        long scanned = 0L;
        long deleted = 0L;
        long emptyDirsRemoved = 0L;
        long deleteFailures = 0L;
        long bytesReclaimed = 0L;

        remoteFsOpRateLimiter.acquire();
        FileStatus rootStatus = fs.getFileStatus(dirPath);
        Deque<DirVisit> stack = new ArrayDeque<DirVisit>();
        stack.push(
                new DirVisit(
                        dirPath,
                        false,
                        rootStatus.isDir()
                                && rootStatus.getModificationTime() < task.cutoffMillis()));
        while (!stack.isEmpty()) {
            DirVisit visit = stack.pop();
            if (visit.postOrder) {
                if (visit.oldEnough && safeDeleter.deleteEmptyDir(visit.dir)) {
                    deleted++;
                    emptyDirsRemoved++;
                }
                continue;
            }
            FileStatus[] children;
            try {
                remoteFsOpRateLimiter.acquire();
                children = fs.listStatus(visit.dir);
            } catch (IOException e) {
                LOG.warn("Failed to list directory: {}", visit.dir, e);
                continue;
            }
            if (children == null) {
                continue;
            }
            stack.push(new DirVisit(visit.dir, true, visit.oldEnough));
            for (FileStatus child : children) {
                FsPath childPath = child.getPath();
                if (child.isDir()) {
                    stack.push(
                            new DirVisit(
                                    childPath,
                                    false,
                                    child.getModificationTime() < task.cutoffMillis()));
                    continue;
                }
                scanned++;
                if (child.getModificationTime() >= task.cutoffMillis()) {
                    continue;
                }
                FileMeta meta =
                        new FileMeta(childPath, child.getLen(), child.getModificationTime());
                FileRule rule = dispatcher.dispatch(meta);
                Decision decision =
                        rule.evaluate(meta, BucketActiveRefs.empty(), task.cutoffMillis());
                switch (decision) {
                    case DELETE:
                        if (safeDeleter.deleteFile(meta.path(), decision, rule.id())) {
                            deleted++;
                            bytesReclaimed += meta.size();
                        } else {
                            deleteFailures++;
                        }
                        break;
                    case SKIP_UNKNOWN:
                        audit.logSkipUnknown(meta.path(), rule.id());
                        break;
                    case KEEP_ACTIVE:
                    case DEFER:
                    default:
                        break;
                }
            }
        }

        return new CleanStats(scanned, deleted, emptyDirsRemoved, deleteFailures, bytesReclaimed);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private SafeDeleter createSafeDeleter(FileSystem fs, boolean dryRun) {
        return new SafeDeleter(fs, dryRun, audit, remoteFsOpRateLimiter);
    }

    private static double perSubtaskRate(long totalRate, int parallelism, int subtaskIndex) {
        long base = totalRate / parallelism;
        long remainder = totalRate % parallelism;
        long quota = base + (subtaskIndex < remainder ? 1L : 0L);
        return Math.max(1.0, (double) quota);
    }

    private static final class DirVisit {
        private final FsPath dir;
        private final boolean postOrder;
        private final boolean oldEnough;

        private DirVisit(FsPath dir, boolean postOrder, boolean oldEnough) {
            this.dir = dir;
            this.postOrder = postOrder;
            this.oldEnough = oldEnough;
        }
    }
}
