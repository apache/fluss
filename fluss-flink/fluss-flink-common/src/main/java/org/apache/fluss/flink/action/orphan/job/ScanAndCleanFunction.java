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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Map;

/**
 * Stage 2 of the orphan files cleanup job. Runs at user-configured parallelism (N) and performs
 * pure FS operations — no coordinator RPC interaction.
 *
 * <p>Each subtask processes assigned {@link CleanTask} items serially:
 *
 * <ul>
 *   <li>{@link BucketCleanTask}: second-reads manifests from object storage to build the active
 *       reference set, then walks log/kv directories and deletes orphan files.
 *   <li>{@link OrphanDirCleanTask}: recursively walks the orphan directory and deletes all files
 *       older than the cutoff.
 * </ul>
 *
 * <p>Each task emits a single {@link CleanStats} containing scalar counters and the short list of
 * directories walked. Delete rate is limited per-subtask: {@code configuredRate /
 * runtimeParallelism}. The serial processing within each subtask guarantees no concurrent throttler
 * access.
 */
@Internal
public final class ScanAndCleanFunction extends ProcessFunction<CleanTask, CleanStats> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ScanAndCleanFunction.class);

    private final long deleteRateLimitPerSecond;
    private final Map<String, String> extraConfigs;

    private transient AuditLogger audit;
    private transient RateLimiter rateLimiter;

    public ScanAndCleanFunction(long deleteRateLimitPerSecond, Map<String, String> extraConfigs) {
        this.deleteRateLimitPerSecond = deleteRateLimitPerSecond;
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
        // Distribute the configured rate as base + 1 extra for the first `remainder` subtasks so
        // that the per-subtask rates sum back to the configured aggregate. Each subtask gets at
        // least 1/s (hard floor) — when parallelism exceeds the configured rate, the aggregate
        // may theoretically exceed it; in practice Batch scheduling limits actual concurrency.
        long base = deleteRateLimitPerSecond / parallelism;
        long remainder = deleteRateLimitPerSecond % parallelism;
        long quota = base + (subtaskIndex < remainder ? 1L : 0L);
        rateLimiter = RateLimiter.create(Math.max(1.0, (double) quota));
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
                new BucketCleaner(dispatcher, safeDeleter, audit, task.cutoffMillis());

        BucketCleaner.BucketCleanStats bucketStats = cleaner.clean(activeRefs, logDir, kvDir);

        List<String> touchedDirs = new ArrayList<String>(2);
        if (logDir != null) {
            touchedDirs.add(logDir.toString());
        }
        if (kvDir != null) {
            touchedDirs.add(kvDir.toString());
        }

        return new CleanStats(
                bucketStats.scanned,
                bucketStats.deleted,
                bucketStats.deleteFailures,
                bucketStats.bytesReclaimed,
                touchedDirs);
    }

    // -------------------------------------------------------------------------
    // OrphanDirCleanTask processing
    // -------------------------------------------------------------------------

    private CleanStats processOrphanDirTask(OrphanDirCleanTask task) throws IOException {
        FsPath dirPath = new FsPath(task.dirPath());
        FileSystem fs = dirPath.getFileSystem();
        if (!fs.exists(dirPath)) {
            return CleanStats.empty();
        }

        SafeDeleter safeDeleter = createSafeDeleter(fs, task.dryRun());
        RuleDispatcher dispatcher = new RuleDispatcher(task.allowDeleteManifest(), true);

        long scanned = 0L;
        long deleted = 0L;
        long deleteFailures = 0L;
        long bytesReclaimed = 0L;

        Deque<FsPath> stack = new ArrayDeque<FsPath>();
        stack.push(dirPath);
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
                    stack.push(childPath);
                    continue;
                }
                if (childPath.getName().startsWith(".")) {
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

        return new CleanStats(
                scanned,
                deleted,
                deleteFailures,
                bytesReclaimed,
                Arrays.asList(dirPath.toString()));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private SafeDeleter createSafeDeleter(FileSystem fs, boolean dryRun) {
        return new SafeDeleter(fs, dryRun, audit, rateLimiter);
    }
}
