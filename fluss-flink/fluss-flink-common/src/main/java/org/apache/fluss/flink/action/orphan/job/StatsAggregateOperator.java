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

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Stage 3 of the orphan files cleanup job. Runs at parallelism=1 to aggregate {@link CleanStats}
 * from all Stage 2 subtasks and perform the final empty-directory sweep.
 *
 * <p>Implemented as a custom operator (not ProcessFunction) because {@code ProcessOperator} does
 * not implement {@link BoundedOneInput} — the {@code endInput()} callback would never fire. This
 * operator accumulates all incoming stats and performs the empty-dir sweep in {@code endInput()}.
 */
@Internal
public final class StatsAggregateOperator extends AbstractStreamOperator<CleanStats>
        implements OneInputStreamOperator<CleanStats, CleanStats>, BoundedOneInput {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(StatsAggregateOperator.class);

    private final boolean dryRun;
    private final Map<String, String> extraConfigs;

    private transient CleanStats accumulated;

    public StatsAggregateOperator(boolean dryRun, Map<String, String> extraConfigs) {
        this.dryRun = dryRun;
        this.extraConfigs = extraConfigs;
    }

    @Override
    public void open() throws Exception {
        super.open();
        if (!extraConfigs.isEmpty()) {
            FileSystem.initialize(Configuration.fromMap(extraConfigs), null);
        }
    }

    @Override
    public void processElement(StreamRecord<CleanStats> element) {
        if (accumulated == null) {
            accumulated = CleanStats.empty();
        }
        accumulated = accumulated.merge(element.getValue());
    }

    @Override
    public void endInput() {
        if (accumulated == null) {
            accumulated = CleanStats.empty();
        }

        long emptyDirsRemoved = sweepEmptyDirs(accumulated.touchedDirs());
        long totalDeleted = accumulated.deleted() + emptyDirsRemoved;

        CleanStats finalStats =
                new CleanStats(
                        accumulated.scanned(),
                        totalDeleted,
                        accumulated.deleteFailures(),
                        accumulated.bytesReclaimed(),
                        new ArrayList<String>());

        LOG.info(
                "Orphan cleanup complete: scanned={}, deleted={} (files={}, emptyDirs={}), "
                        + "failures={}, bytesReclaimed={}",
                finalStats.scanned(),
                totalDeleted,
                accumulated.deleted(),
                emptyDirsRemoved,
                finalStats.deleteFailures(),
                finalStats.bytesReclaimed());

        output.collect(new StreamRecord<>(finalStats));
    }

    private long sweepEmptyDirs(List<String> touchedDirs) {
        if (touchedDirs.isEmpty()) {
            return 0L;
        }
        AuditLogger audit = new AuditLogger();
        EmptyDirSweeper sweeper = new EmptyDirSweeper(dryRun, audit);
        for (String dir : touchedDirs) {
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
