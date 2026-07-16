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

package org.apache.fluss.lake.paimon.tiering;

import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.lake.committer.PartitionDoneCandidate;

import org.apache.paimon.partition.PartitionTimeExtractor;
import org.apache.paimon.partition.actions.PartitionMarkDoneAction;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link PaimonPartitionDoneHandler} mark-done judgement logic. */
class PaimonPartitionDoneHandlerTest {

    private static final ZoneId UTC = ZoneId.of("UTC");
    private static final long ONE_DAY_MS = 24L * 60 * 60 * 1000;
    private static final long ONE_HOUR_MS = 60L * 60 * 1000;

    // 2024-01-01T00:00:00 UTC
    private static final long DAY_20240101_START = 1704067200000L;
    // 2024-01-02T00:00:00 UTC
    private static final long DAY_20240101_END = DAY_20240101_START + ONE_DAY_MS;

    /** A fake action recording the partitions it was asked to mark done. */
    private static class RecordingAction implements PartitionMarkDoneAction {
        final List<String> marked = new ArrayList<>();
        boolean closed = false;

        @Override
        public void markDone(String partition) {
            marked.add(partition);
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private PaimonPartitionDoneHandler nonAutoHandler(RecordingAction action, long idleMs) {
        // non-auto-partition table: default PartitionTimeExtractor parses "yyyy-MM-dd".
        return new PaimonPartitionDoneHandler(
                Collections.singletonList(action),
                Collections.singletonList("dt"),
                idleMs,
                UTC,
                null,
                new PartitionTimeExtractor(null, null));
    }

    private PaimonPartitionDoneHandler autoDayHandler(RecordingAction action, long idleMs) {
        return new PaimonPartitionDoneHandler(
                Collections.singletonList(action),
                Collections.singletonList("dt"),
                idleMs,
                UTC,
                AutoPartitionTimeUnit.DAY,
                null);
    }

    @Test
    void testMarkDoneWhenIdleExceeded() throws Exception {
        RecordingAction action = new RecordingAction();
        PaimonPartitionDoneHandler handler = nonAutoHandler(action, ONE_HOUR_MS);

        // non-auto table: threshold = MAX(lastUpdate, partitionStart) = partition start.
        PartitionDoneCandidate state = new PartitionDoneCandidate("2024-01-01", DAY_20240101_START);
        long now = DAY_20240101_END + ONE_HOUR_MS + 1; // strictly greater than idle

        List<String> done = handler.markDoneIfReady(Collections.singletonList(state), now);

        assertThat(done).containsExactly("2024-01-01");
        assertThat(action.marked).containsExactly("dt=2024-01-01");
    }

    @Test
    void testNotMarkDoneAtIdleBoundary() throws Exception {
        RecordingAction action = new RecordingAction();
        PaimonPartitionDoneHandler handler = nonAutoHandler(action, ONE_HOUR_MS);

        PartitionDoneCandidate state = new PartitionDoneCandidate("2024-01-01", DAY_20240101_START);
        long now =
                DAY_20240101_START
                        + ONE_HOUR_MS; // exactly one idle after partition start -> not done

        List<String> done = handler.markDoneIfReady(Collections.singletonList(state), now);

        assertThat(done).isEmpty();
        assertThat(action.marked).isEmpty();
    }

    @Test
    void testNotMarkDoneWhenPartitionStartInFuture() throws Exception {
        RecordingAction action = new RecordingAction();
        PaimonPartitionDoneHandler handler = nonAutoHandler(action, ONE_HOUR_MS);

        // lastUpdateTime is very old, but the partition's own start time is in the future relative
        // to now, so threshold = MAX(lastUpdateTime, partitionStart) = partitionStart and it must
        // not be done.
        PartitionDoneCandidate state = new PartitionDoneCandidate("2024-01-02", 1L);
        long now = DAY_20240101_START + ONE_HOUR_MS; // before the 2024-01-02 partition start

        List<String> done = handler.markDoneIfReady(Collections.singletonList(state), now);

        assertThat(done).isEmpty();
    }

    @Test
    void testAutoPartitionDayEndTimeComputation() throws Exception {
        RecordingAction action = new RecordingAction();
        PaimonPartitionDoneHandler handler = autoDayHandler(action, ONE_HOUR_MS);

        // auto-partition value uses Fluss format yyyyMMdd.
        PartitionDoneCandidate state = new PartitionDoneCandidate("20240101", DAY_20240101_START);

        // at boundary -> not done
        assertThat(
                        handler.markDoneIfReady(
                                Collections.singletonList(state), DAY_20240101_END + ONE_HOUR_MS))
                .isEmpty();
        // just past boundary -> done
        assertThat(
                        handler.markDoneIfReady(
                                Collections.singletonList(state),
                                DAY_20240101_END + ONE_HOUR_MS + 1))
                .containsExactly("20240101");
        assertThat(action.marked).containsExactly("dt=20240101");
    }

    @Test
    void testPartialDoneAmongMultiplePartitions() throws Exception {
        RecordingAction action = new RecordingAction();
        PaimonPartitionDoneHandler handler = autoDayHandler(action, ONE_HOUR_MS);

        // 2024-01-01 ended long ago -> done; 2024-01-03 end in the future -> not done.
        PartitionDoneCandidate oldPartition =
                new PartitionDoneCandidate("20240101", DAY_20240101_START);
        long day03Start = DAY_20240101_START + 2 * ONE_DAY_MS;
        PartitionDoneCandidate currentPartition =
                new PartitionDoneCandidate("20240103", day03Start);
        long now = day03Start + ONE_HOUR_MS; // 2024-01-03 not ended yet

        List<String> done =
                handler.markDoneIfReady(Arrays.asList(oldPartition, currentPartition), now);

        assertThat(done).containsExactly("20240101");
    }

    private PaimonPartitionDoneHandler autoHandler(
            RecordingAction action, long idleMs, AutoPartitionTimeUnit unit) {
        return new PaimonPartitionDoneHandler(
                Collections.singletonList(action),
                Collections.singletonList("dt"),
                idleMs,
                UTC,
                unit,
                null);
    }

    @Test
    void testAutoPartitionEndTimeForAllTimeUnits() throws Exception {
        // now far in the future so every partition below is idle enough to be done, exercising
        // each auto-partition-value parsing branch (YEAR/QUARTER/MONTH/HOUR; DAY covered above).
        long farFuture = 1893456000000L; // 2030-01-01T00:00:00 UTC
        assertAutoDone(AutoPartitionTimeUnit.YEAR, "2024", farFuture);
        assertAutoDone(AutoPartitionTimeUnit.QUARTER, "20241", farFuture);
        assertAutoDone(AutoPartitionTimeUnit.MONTH, "202401", farFuture);
        assertAutoDone(AutoPartitionTimeUnit.HOUR, "2024010100", farFuture);
    }

    private void assertAutoDone(AutoPartitionTimeUnit unit, String value, long now)
            throws Exception {
        RecordingAction action = new RecordingAction();
        PaimonPartitionDoneHandler handler = autoHandler(action, ONE_HOUR_MS, unit);
        PartitionDoneCandidate state = new PartitionDoneCandidate(value, 0L);
        assertThat(handler.markDoneIfReady(Collections.singletonList(state), now))
                .containsExactly(value);
        assertThat(action.marked).containsExactly("dt=" + value);
    }

    @Test
    void testDegradeToIdleWhenPartitionTimeUnparseable() throws Exception {
        // a non-date partition value makes PartitionTimeExtractor throw, so partitionEndTime
        // degrades to 0 and the judgement falls back to lastUpdateTime alone.
        RecordingAction action = new RecordingAction();
        PaimonPartitionDoneHandler handler = nonAutoHandler(action, ONE_HOUR_MS);
        PartitionDoneCandidate state = new PartitionDoneCandidate("not-a-date", 1000L);
        long now = 1000L + ONE_HOUR_MS + 1;
        assertThat(handler.markDoneIfReady(Collections.singletonList(state), now))
                .containsExactly("not-a-date");
    }

    @Test
    void testSkipWhenNeitherTimeAvailable() throws Exception {
        // both lastUpdateTime and partitionEndTime unavailable -> skip.
        RecordingAction action = new RecordingAction();
        PaimonPartitionDoneHandler handler = nonAutoHandler(action, ONE_HOUR_MS);
        PartitionDoneCandidate state = new PartitionDoneCandidate("not-a-date", 0L);
        assertThat(handler.markDoneIfReady(Collections.singletonList(state), Long.MAX_VALUE))
                .isEmpty();
        assertThat(action.marked).isEmpty();
    }

    @Test
    void testCloseClosesActions() throws Exception {
        RecordingAction action = new RecordingAction();
        PaimonPartitionDoneHandler handler = autoDayHandler(action, ONE_HOUR_MS);
        handler.close();
        assertThat(action.closed).isTrue();
    }
}
