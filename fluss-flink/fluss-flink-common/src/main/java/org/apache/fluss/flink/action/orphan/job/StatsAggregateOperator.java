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
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.shaded.guava32.com.google.common.util.concurrent.RateLimiter;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Stage 3 of the orphan files cleanup job. Runs at parallelism=1 to aggregate per-subtask {@link
 * CleanStats} records and perform the final empty-directory sweep.
 *
 * <p>Implemented as a custom operator (not ProcessFunction) because {@code ProcessOperator} does
 * not implement {@link BoundedOneInput} — the {@code endInput()} callback would never fire.
 *
 * <p>Scalar counters are accumulated into four longs; directory paths from each incoming {@link
 * CleanStats#touchedDirs()} are inserted into a {@link HashSet} for O(1) deduplication. The final
 * empty-dir sweep happens in {@link #endInput()}.
 */
@Internal
public final class StatsAggregateOperator extends AbstractStreamOperator<CleanStats>
        implements OneInputStreamOperator<CleanStats, CleanStats>, BoundedOneInput {

    private static final long serialVersionUID = 2L;
    private static final Logger LOG = LoggerFactory.getLogger(StatsAggregateOperator.class);

    private final boolean dryRun;
    private final Map<String, String> extraConfigs;
    private final long deleteRateLimitPerSecond;

    private transient long scanned;
    private transient long deleted;
    private transient long deleteFailures;
    private transient long bytesReclaimed;
    private transient Set<String> touchedDirs;
    private transient RateLimiter sweepRateLimiter;

    public StatsAggregateOperator(
            boolean dryRun, Map<String, String> extraConfigs, long deleteRateLimitPerSecond) {
        this.dryRun = dryRun;
        this.extraConfigs = extraConfigs;
        this.deleteRateLimitPerSecond = deleteRateLimitPerSecond;
    }

    @Override
    public void open() throws Exception {
        super.open();
        if (!extraConfigs.isEmpty()) {
            FileSystem.initialize(Configuration.fromMap(extraConfigs), null);
        }
        scanned = 0L;
        deleted = 0L;
        deleteFailures = 0L;
        bytesReclaimed = 0L;
        touchedDirs = new HashSet<String>();
        sweepRateLimiter = RateLimiter.create((double) deleteRateLimitPerSecond);
    }

    @Override
    public void processElement(StreamRecord<CleanStats> element) {
        CleanStats stats = element.getValue();
        scanned += stats.scanned();
        deleted += stats.deleted();
        deleteFailures += stats.deleteFailures();
        bytesReclaimed += stats.bytesReclaimed();
        touchedDirs.addAll(stats.touchedDirs());
    }

    @Override
    public void endInput() {
        long emptyDirsRemoved = sweepEmptyDirs(touchedDirs);
        long totalDeleted = deleted + emptyDirsRemoved;

        CleanStats finalStats =
                new CleanStats(
                        scanned,
                        totalDeleted,
                        deleteFailures,
                        bytesReclaimed,
                        new ArrayList<String>(0));

        LOG.info(
                "Orphan cleanup complete: scanned={}, deleted={} (files={}, emptyDirs={}), "
                        + "failures={}, bytesReclaimed={}",
                finalStats.scanned(),
                totalDeleted,
                deleted,
                emptyDirsRemoved,
                finalStats.deleteFailures(),
                finalStats.bytesReclaimed());

        output.collect(new StreamRecord<>(finalStats));
    }

    private long sweepEmptyDirs(Set<String> dirs) {
        if (dirs.isEmpty()) {
            return 0L;
        }
        AuditLogger audit = new AuditLogger();
        EmptyDirSweeper sweeper = new EmptyDirSweeper(dryRun, audit, sweepRateLimiter);
        for (String dir : dirs) {
            sweeper.registerTouched(new FsPath(dir));
        }
        try {
            return sweeper.sweep();
        } catch (IOException e) {
            LOG.warn("Empty directory sweep encountered errors", e);
            return 0L;
        }
    }
}
