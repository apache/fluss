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

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Stage 3 of the orphan files cleanup job. Runs at parallelism=1 to aggregate per-subtask {@link
 * CleanStats} records.
 *
 * <p>Implemented as a custom operator (not ProcessFunction) because {@code ProcessOperator} does
 * not implement {@link BoundedOneInput} — the {@code endInput()} callback would never fire.
 *
 * <p>Scalar counters are accumulated into longs and the final summary is emitted in {@link
 * #endInput()}.
 */
@Internal
public final class StatsAggregateOperator extends AbstractStreamOperator<CleanStats>
        implements OneInputStreamOperator<CleanStats, CleanStats>, BoundedOneInput {

    private static final long serialVersionUID = 2L;

    private final boolean dryRun;

    private transient long scanned;
    private transient long deleted;
    private transient long emptyDirsRemoved;
    private transient long deleteFailures;
    private transient long bytesReclaimed;

    public StatsAggregateOperator(boolean dryRun) {
        this.dryRun = dryRun;
    }

    @Override
    public void open() throws Exception {
        super.open();
        scanned = 0L;
        deleted = 0L;
        emptyDirsRemoved = 0L;
        deleteFailures = 0L;
        bytesReclaimed = 0L;
    }

    @Override
    public void processElement(StreamRecord<CleanStats> element) {
        CleanStats stats = element.getValue();
        scanned += stats.scanned();
        deleted += stats.deleted();
        emptyDirsRemoved += stats.emptyDirsRemoved();
        deleteFailures += stats.deleteFailures();
        bytesReclaimed += stats.bytesReclaimed();
    }

    @Override
    public void endInput() {
        AuditLogger audit = new AuditLogger();
        CleanStats finalStats =
                new CleanStats(scanned, deleted, emptyDirsRemoved, deleteFailures, bytesReclaimed);

        audit.logSummary(
                scanned,
                deleted - emptyDirsRemoved,
                emptyDirsRemoved,
                deleteFailures,
                bytesReclaimed,
                dryRun);

        output.collect(new StreamRecord<>(finalStats));
    }
}
