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
import org.apache.fluss.flink.action.orphan.audit.ResultAuditLogger;
import org.apache.fluss.flink.action.orphan.fs.FileSystemProbe;
import org.apache.fluss.flink.action.orphan.fs.SafeDeleter;
import org.apache.fluss.flink.action.orphan.rule.BucketActiveRefs;
import org.apache.fluss.flink.action.orphan.rule.Decision;
import org.apache.fluss.flink.action.orphan.rule.FileMeta;
import org.apache.fluss.flink.action.orphan.rule.FileRule;
import org.apache.fluss.flink.action.orphan.rule.MtimePolicy;
import org.apache.fluss.flink.action.orphan.rule.RuleDispatcher;
import org.apache.fluss.flink.adapter.RuntimeContextAdapter;
import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.shaded.guava32.com.google.common.util.concurrent.RateLimiter;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.Optional;

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
    private final long remoteFsOpRateLimitPerSecond;
    private final Map<String, String> extraConfigs;

    private final ResultAuditLogger resultAudit;
    private final boolean dryRun;
    private transient CleanupCounters subtaskCounters;
    private transient RuleSummary subtaskRules;
    private transient long tasksCompleted;
    private transient AuditLogger audit;
    private transient RateLimiter remoteFsOpRateLimiter;

    public ScanAndCleanFunction(
            long remoteFsOpRateLimitPerSecond, Map<String, String> extraConfigs) {
        this(
                remoteFsOpRateLimitPerSecond,
                extraConfigs,
                false,
                new ResultAuditLogger(extraConfigs));
    }

    public ScanAndCleanFunction(
            long remoteFsOpRateLimitPerSecond,
            Map<String, String> extraConfigs,
            boolean dryRun,
            ResultAuditLogger resultAudit) {
        this.dryRun = dryRun;
        this.resultAudit = resultAudit;
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
        subtaskCounters = CleanupCounters.empty();
        subtaskRules = new RuleSummary();
        tasksCompleted = 0;
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
        if (task instanceof ScopeSummaryTask) {
            out.collect(CleanStats.scopeSummary(((ScopeSummaryTask) task).coverage()));
        } else if (task instanceof BucketCleanTask) {
            emitTaskStats(processBucketTask((BucketCleanTask) task), out);
        } else if (task instanceof OrphanDirCleanTask) {
            emitTaskStats(processOrphanDirTask((OrphanDirCleanTask) task), out);
        }
    }

    private void emitTaskStats(CleanStats stats, Collector<CleanStats> out) {
        subtaskCounters = subtaskCounters.add(stats.counters());
        subtaskRules.add(stats.rules());
        tasksCompleted++;
        out.collect(stats);
    }

    @Override
    public void close() throws Exception {
        if (subtaskCounters != null) {
            resultAudit.subtask(
                    RuntimeContextAdapter.getIndexOfThisSubtask(
                            (StreamingRuntimeContext) getRuntimeContext()),
                    RuntimeContextAdapter.getAttemptNumber(getRuntimeContext()),
                    tasksCompleted,
                    subtaskCounters,
                    subtaskRules,
                    dryRun);
        }
        super.close();
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
                        task.logActiveManifestPaths(),
                        task.kvSharedSstFileNames(),
                        task.kvSharedSstRefsComplete());
        RuleDispatcher dispatcher = new RuleDispatcher(task.allowDeleteManifest());
        SafeDeleter safeDeleter = createSafeDeleter(anyDir.getFileSystem(), task.dryRun());
        BucketCleaner cleaner =
                new BucketCleaner(
                        dispatcher,
                        safeDeleter,
                        audit,
                        task.cutoffMillis(),
                        remoteFsOpRateLimiter,
                        task.dryRun());

        BucketCleaner.BucketCleanStats bucketStats = cleaner.clean(activeRefs, logDir, kvDir);

        CleanStats result =
                new CleanStats(
                        new CleanupCounters(
                                bucketStats.scannedFiles,
                                bucketStats.plannedFiles,
                                bucketStats.plannedDirs,
                                bucketStats.plannedBytes,
                                bucketStats.deletedFiles,
                                bucketStats.emptyDirsRemoved,
                                bucketStats.deleteFailures,
                                bucketStats.bytesReclaimed));
        result.rules().add(bucketStats.rules);
        return result;
    }

    // -------------------------------------------------------------------------
    // OrphanDirCleanTask processing
    // -------------------------------------------------------------------------

    private CleanStats processOrphanDirTask(OrphanDirCleanTask task) throws IOException {
        FsPath dirPath = new FsPath(task.dirPath());
        FileSystem fs = dirPath.getFileSystem();

        SafeDeleter safeDeleter = createSafeDeleter(fs, task.dryRun());
        RuleDispatcher dispatcher = new RuleDispatcher(task.allowDeleteManifest());

        RuleSummary rules = new RuleSummary();
        long scanned = 0L;
        long plannedFiles = 0L;
        long plannedDirs = 0L;
        long plannedBytes = 0L;
        long deleted = 0L;
        long emptyDirsRemoved = 0L;
        long deleteFailures = 0L;
        long bytesReclaimed = 0L;

        Optional<FileStatus> rootStatusResult =
                FileSystemProbe.getFileStatus(fs, dirPath, remoteFsOpRateLimiter);
        if (!rootStatusResult.isPresent()) {
            CleanStats missing = CleanStats.empty();
            missing.rules().recordMissingDirectory();
            return missing;
        }
        FileStatus rootStatus = rootStatusResult.get();
        rules.recordDirectoryMtime(rootStatus.getModificationTime());
        Deque<DirVisit> stack = new ArrayDeque<DirVisit>();
        stack.push(
                new DirVisit(
                        dirPath,
                        false,
                        rootStatus.isDir()
                                && MtimePolicy.evaluateInactiveFile(
                                                rootStatus.getModificationTime(),
                                                task.cutoffMillis())
                                        == Decision.DELETE,
                        null));
        while (!stack.isEmpty()) {
            DirVisit visit = stack.pop();
            if (visit.postOrder) {
                if (visit.oldEnough && !visit.hasRemainingChild) {
                    plannedDirs++;
                    if (task.dryRun()) {
                        audit.logWouldDeleteDir(visit.dir);
                    } else if (safeDeleter.deleteEmptyDir(visit.dir)) {
                        emptyDirsRemoved++;
                    } else {
                        deleteFailures++;
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
                rules.recordMissingDirectory();
                visit.markParentRemaining();
                continue;
            }
            FileStatus[] children = listing.get();
            visit.postOrder = true;
            stack.push(visit);
            for (FileStatus child : children) {
                FsPath childPath = child.getPath();
                if (child.isDir()) {
                    rules.recordDirectoryMtime(child.getModificationTime());
                    stack.push(
                            new DirVisit(
                                    childPath,
                                    false,
                                    MtimePolicy.evaluateInactiveFile(
                                                    child.getModificationTime(),
                                                    task.cutoffMillis())
                                            == Decision.DELETE,
                                    visit));
                    continue;
                }
                scanned++;
                FileMeta meta =
                        new FileMeta(childPath, child.getLen(), child.getModificationTime());
                FileRule rule = dispatcher.dispatch(meta);
                Decision decision =
                        rule.evaluate(meta, BucketActiveRefs.knownEmpty(), task.cutoffMillis());
                rules.record(rule.id(), decision, meta.size());
                switch (decision) {
                    case DELETE:
                        plannedFiles++;
                        plannedBytes += meta.size();
                        if (safeDeleter.deleteFile(meta.path(), decision, rule.id())) {
                            if (!task.dryRun()) {
                                deleted++;
                                bytesReclaimed += meta.size();
                            }
                        } else {
                            deleteFailures++;
                            visit.hasRemainingChild = true;
                        }
                        break;
                    case SKIP_UNKNOWN:
                        audit.logSkipUnknown(meta.path(), rule.id());
                        visit.hasRemainingChild = true;
                        break;
                    case KEEP_ACTIVE:
                    case DEFER:
                    default:
                        visit.hasRemainingChild = true;
                        break;
                }
            }
        }

        CleanStats result =
                new CleanStats(
                        new CleanupCounters(
                                scanned,
                                plannedFiles,
                                plannedDirs,
                                plannedBytes,
                                deleted,
                                emptyDirsRemoved,
                                deleteFailures,
                                bytesReclaimed));
        result.rules().add(rules);
        return result;
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
        private boolean postOrder;
        private final boolean oldEnough;
        private final DirVisit parent;
        private boolean hasRemainingChild;

        private DirVisit(FsPath dir, boolean postOrder, boolean oldEnough, DirVisit parent) {
            this.dir = dir;
            this.postOrder = postOrder;
            this.oldEnough = oldEnough;
            this.parent = parent;
        }

        private void markParentRemaining() {
            if (parent != null) {
                parent.hasRemainingChild = true;
            }
        }
    }
}
