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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.server.coordinator.event.CoordinatorEvent;
import org.apache.fluss.server.coordinator.event.DropPartitionEvent;
import org.apache.fluss.server.coordinator.event.DropTableEvent;
import org.apache.fluss.server.coordinator.event.EventManager;
import org.apache.fluss.server.coordinator.event.ResumeDropEvent;
import org.apache.fluss.utils.clock.ManualClock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link TableLifecycleThrottler}. */
class TableLifecycleThrottlerTest {

    private RecordingEventManager eventManager;
    private ManualClock clock;
    private NoOpScheduledExecutor timeoutExecutor;

    @BeforeEach
    void setup() {
        eventManager = new RecordingEventManager();
        clock = new ManualClock(0L);
        timeoutExecutor = new NoOpScheduledExecutor();
    }

    @AfterEach
    void tearDown() {
        timeoutExecutor.shutdownNow();
    }

    @Test
    void testSubmitPartitionDropAdmitsImmediately() {
        TableLifecycleThrottler manager = newThrottler();

        manager.submitPartitionDrop(1L, 100L, "p20240101");

        assertThat(eventManager.summaries()).containsExactly("partition:1:100:p20240101");
        assertThat(manager.getInflightCount()).isOne();
        assertThat(manager.getPendingDropCount()).isZero();
    }

    @Test
    void testSubmitTableDropAdmitsImmediately() {
        TableLifecycleThrottler manager = newThrottler();

        manager.submitTableDrop(7L, false, false, false);

        assertThat(eventManager.summaries()).containsExactly("table:7:auto=false:lake=false");
        assertThat(manager.getInflightCount()).isOne();
    }

    @Test
    void testOneAtATimeThrottling() {
        TableLifecycleThrottler manager = newThrottler();

        // First drop is admitted immediately; subsequent drops pend.
        manager.submitPartitionDrop(1L, 100L, "p1");
        manager.submitPartitionDrop(1L, 101L, "p2");
        manager.submitPartitionDrop(1L, 102L, "p3");

        assertThat(eventManager.summaries()).containsExactly("partition:1:100:p1");
        assertThat(manager.getInflightCount()).isOne();
        assertThat(manager.getPendingDropCount()).isEqualTo(2);
    }

    @Test
    void testCompletionAdmitsNextPending() {
        TableLifecycleThrottler manager = newThrottler();

        manager.submitPartitionDrop(1L, 100L, "p1");
        manager.submitPartitionDrop(1L, 101L, "p2");
        manager.submitPartitionDrop(1L, 102L, "p3");

        assertThat(eventManager.summaries()).containsExactly("partition:1:100:p1");

        // Complete the first; second is admitted.
        manager.onPartitionDropCompleted(new TablePartition(1L, 100L));
        assertThat(eventManager.summaries())
                .containsExactly("partition:1:100:p1", "partition:1:101:p2");
        assertThat(manager.getInflightCount()).isOne();
        assertThat(manager.getPendingDropCount()).isOne();

        // Complete the second; third is admitted.
        manager.onPartitionDropCompleted(new TablePartition(1L, 101L));
        assertThat(eventManager.summaries())
                .containsExactly("partition:1:100:p1", "partition:1:101:p2", "partition:1:102:p3");
        assertThat(manager.getInflightCount()).isOne();
        assertThat(manager.getPendingDropCount()).isZero();
    }

    @Test
    void testTableDropCompletionAdmitsNext() {
        TableLifecycleThrottler manager = newThrottler();

        manager.submitTableDrop(1L, false, false, false);
        manager.submitPartitionDrop(2L, 200L, "p2");

        assertThat(eventManager.summaries()).containsExactly("table:1:auto=false:lake=false");
        assertThat(manager.getPendingDropCount()).isOne();

        manager.onTableDropCompleted(1L);

        assertThat(eventManager.summaries())
                .containsExactly("table:1:auto=false:lake=false", "partition:2:200:p2");
        assertThat(manager.getPendingDropCount()).isZero();
    }

    @Test
    void testFifoOrder() {
        TableLifecycleThrottler manager = newThrottler();

        manager.submitPartitionDrop(1L, 10L, "p10");
        manager.submitPartitionDrop(1L, 11L, "p11");
        manager.submitPartitionDrop(1L, 12L, "p12");

        assertThat(eventManager.summaries()).containsExactly("partition:1:10:p10");

        manager.onPartitionDropCompleted(new TablePartition(1L, 10L));
        manager.onPartitionDropCompleted(new TablePartition(1L, 11L));

        assertThat(eventManager.summaries())
                .containsExactly("partition:1:10:p10", "partition:1:11:p11", "partition:1:12:p12");
    }

    @Test
    void testCompletionForUnknownDropIsNoOp() {
        TableLifecycleThrottler manager = newThrottler();

        manager.onPartitionDropCompleted(new TablePartition(99L, 999L));
        manager.onTableDropCompleted(123L);

        assertThat(eventManager.summaries()).isEmpty();
        assertThat(manager.getInflightCount()).isZero();
    }

    @Test
    void testCompletionForMismatchedDropIsNoOp() {
        TableLifecycleThrottler manager = newThrottler();

        manager.submitPartitionDrop(1L, 100L, "p1");

        // Complete with wrong partition id — should be no-op.
        manager.onPartitionDropCompleted(new TablePartition(1L, 999L));
        assertThat(manager.getInflightCount()).isOne();

        // Complete with table callback — should be no-op (inflight is a partition drop).
        manager.onTableDropCompleted(1L);
        assertThat(manager.getInflightCount()).isOne();
    }

    @Test
    void testTimeoutAbandonsInflightDrop() {
        TableLifecycleThrottler manager = newThrottler();

        manager.submitTableDrop(1L, false, false, false);
        manager.submitPartitionDrop(2L, 200L, "px");

        assertThat(manager.getInflightCount()).isOne();
        assertThat(manager.getPendingDropCount()).isOne();

        // Not yet expired.
        clock.advanceTime(Duration.ofMillis(100_000));
        manager.checkTimeouts();
        assertThat(manager.getInflightCount()).isOne();

        // Crosses the 3-minute timeout boundary.
        clock.advanceTime(Duration.ofMillis(90_000));
        manager.checkTimeouts();

        assertThat(eventManager.summaries())
                .containsExactly("table:1:auto=false:lake=false", "partition:2:200:px");
        assertThat(manager.getPendingDropCount()).isZero();
        assertThat(manager.getInflightCount()).isOne();
    }

    @Test
    void testDuplicateCompletionIsNoOp() {
        TableLifecycleThrottler manager = newThrottler();
        TablePartition tp = new TablePartition(1L, 100L);

        manager.submitPartitionDrop(tp.getTableId(), tp.getPartitionId(), "px");
        assertThat(manager.getInflightCount()).isOne();

        manager.onPartitionDropCompleted(tp);
        assertThat(manager.getInflightCount()).isZero();

        // Second completion is a no-op.
        manager.onPartitionDropCompleted(tp);
        assertThat(manager.getInflightCount()).isZero();
    }

    @Test
    void testStartIsIdempotentAndCloseDisablesStart() {
        TableLifecycleThrottler manager = newThrottler();

        manager.start();
        manager.start(); // second start is a no-op

        manager.close();

        assertThatThrownBy(manager::start).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testCloseIsIdempotent() {
        TableLifecycleThrottler manager = newThrottler();

        manager.close();
        manager.close(); // must not throw
    }

    @Test
    void testSubmitTableDropForResumeEnqueuesResumeEvent() {
        TableLifecycleThrottler manager = newThrottler();

        manager.submitTableDropForResume(7L);

        // Resume-mode submission enqueues an identity-only ResumeDropEvent for the event thread.
        assertThat(eventManager.events).hasSize(1);
        assertThat(eventManager.events.get(0)).isInstanceOf(ResumeDropEvent.class);
        ResumeDropEvent resumeEvent = (ResumeDropEvent) eventManager.events.get(0);
        assertThat(resumeEvent.getPartitionId()).isNull();
        assertThat(resumeEvent.getTableId()).isEqualTo(7L);
        assertThat(manager.getInflightCount()).isOne();
    }

    @Test
    void testSubmitPartitionDropForResumeEnqueuesResumeEvent() {
        TableLifecycleThrottler manager = newThrottler();

        manager.submitPartitionDropForResume(1L, 100L, "p20240101");

        assertThat(eventManager.events).hasSize(1);
        assertThat(eventManager.events.get(0)).isInstanceOf(ResumeDropEvent.class);
        ResumeDropEvent resumeEvent = (ResumeDropEvent) eventManager.events.get(0);
        assertThat(resumeEvent.getTableId()).isEqualTo(1L);
        assertThat(resumeEvent.getPartitionId()).isEqualTo(100L);
        assertThat(manager.getInflightCount()).isOne();
    }

    @Test
    void testForResumeDropRespectsOneAtATime() {
        TableLifecycleThrottler manager = newThrottler();

        manager.submitTableDropForResume(1L);
        manager.submitPartitionDropForResume(2L, 200L, "px");

        // First admitted (resume event enqueued); second pends.
        assertThat(eventManager.events).hasSize(1);
        ResumeDropEvent firstEvent = (ResumeDropEvent) eventManager.events.get(0);
        assertThat(firstEvent.getPartitionId()).isNull();
        assertThat(firstEvent.getTableId()).isEqualTo(1L);
        assertThat(manager.getInflightCount()).isOne();
        assertThat(manager.getPendingDropCount()).isOne();

        // Completion of the first admits the second.
        manager.onTableDropCompleted(1L);
        assertThat(eventManager.events).hasSize(2);
        ResumeDropEvent secondEvent = (ResumeDropEvent) eventManager.events.get(1);
        assertThat(secondEvent.getTableId()).isEqualTo(2L);
        assertThat(secondEvent.getPartitionId()).isEqualTo(200L);
    }

    @Test
    void testPartitionedTableDropIsFireAndForget() {
        TableLifecycleThrottler manager = newThrottler();

        // Submit a partition drop first (will be admitted as inflight).
        manager.submitPartitionDrop(1L, 100L, "p1");
        // Then submit a partitioned table drop (pending).
        manager.submitTableDrop(1L, true, true, false);
        // Then submit another partition drop (pending).
        manager.submitPartitionDrop(2L, 200L, "p2");

        assertThat(eventManager.summaries()).containsExactly("partition:1:100:p1");
        assertThat(manager.getInflightCount()).isOne();
        assertThat(manager.getPendingDropCount()).isEqualTo(2);

        // Complete partition1. The auto-partition table drop is admitted next.
        // Since it's fire-and-forget, it immediately releases and admits the next drop.
        manager.onPartitionDropCompleted(new TablePartition(1L, 100L));

        // Both the table drop AND the following partition drop should have been admitted.
        assertThat(eventManager.summaries())
                .containsExactly(
                        "partition:1:100:p1", "table:1:auto=true:lake=false", "partition:2:200:p2");
        // The last partition drop is now inflight (waiting for completion).
        assertThat(manager.getInflightCount()).isOne();
        assertThat(manager.getPendingDropCount()).isZero();
    }

    @Test
    void testPartitionedTableDropAdmittedFirstIsAlsoFireAndForget() {
        TableLifecycleThrottler manager = newThrottler();

        // Submit a partitioned table drop as the first drop.
        manager.submitTableDrop(5L, true, true, false);

        // It should be executed immediately AND released (not tracked as inflight).
        assertThat(eventManager.summaries()).containsExactly("table:5:auto=true:lake=false");
        assertThat(manager.getInflightCount()).isZero();
        assertThat(manager.getPendingDropCount()).isZero();
    }

    @Test
    void testPartitionedTableWithoutAutoPartitionIsAlsoFireAndForget() {
        TableLifecycleThrottler manager = newThrottler();

        // A partitioned table without auto-partition still has no table-level replicas.
        manager.submitTableDrop(5L, true, false, false);

        // It should also be fire-and-forget.
        assertThat(eventManager.summaries()).containsExactly("table:5:auto=false:lake=false");
        assertThat(manager.getInflightCount()).isZero();
        assertThat(manager.getPendingDropCount()).isZero();
    }

    @Test
    void testNonPartitionedTableDropIsNotFireAndForget() {
        TableLifecycleThrottler manager = newThrottler();

        // Non-partitioned table drop should be tracked normally.
        manager.submitTableDrop(5L, false, false, false);

        assertThat(eventManager.summaries()).containsExactly("table:5:auto=false:lake=false");
        assertThat(manager.getInflightCount()).isOne();
        assertThat(manager.getPendingDropCount()).isZero();
    }

    // ------------------------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------------------------

    private TableLifecycleThrottler newThrottler() {
        TableLifecycleThrottler throttler =
                new TableLifecycleThrottler(
                        eventManager, clock, timeoutExecutor, 3 * 60 * 1000L, 60 * 1000L);
        throttler.start(); // Activate throttling for unit tests.
        return throttler;
    }

    /** Records the order of drop events admitted into the coordinator event queue. */
    private static final class RecordingEventManager implements EventManager {
        final List<CoordinatorEvent> events = new ArrayList<>();

        @Override
        public void put(CoordinatorEvent event) {
            events.add(event);
        }

        List<String> summaries() {
            List<String> out = new ArrayList<>(events.size());
            for (CoordinatorEvent event : events) {
                if (event instanceof DropPartitionEvent) {
                    DropPartitionEvent e = (DropPartitionEvent) event;
                    out.add(
                            "partition:"
                                    + e.getTableId()
                                    + ":"
                                    + e.getPartitionId()
                                    + ":"
                                    + e.getPartitionName());
                } else if (event instanceof DropTableEvent) {
                    DropTableEvent e = (DropTableEvent) event;
                    out.add(
                            "table:"
                                    + e.getTableId()
                                    + ":auto="
                                    + e.isAutoPartitionTable()
                                    + ":lake="
                                    + e.isDataLakeEnabled());
                } else {
                    out.add(event.toString());
                }
            }
            return out;
        }
    }

    /**
     * A scheduled executor that never actually runs scheduled tasks, so tests retain full control
     * over when {@link TableLifecycleThrottler#checkTimeouts()} is invoked.
     */
    private static final class NoOpScheduledExecutor extends ScheduledThreadPoolExecutor
            implements ScheduledExecutorService {

        NoOpScheduledExecutor() {
            super(0);
        }

        @Override
        public java.util.concurrent.ScheduledFuture<?> scheduleWithFixedDelay(
                Runnable command,
                long initialDelay,
                long delay,
                java.util.concurrent.TimeUnit unit) {
            // Intentionally drop the schedule request: tests drive checkTimeouts() directly.
            return null;
        }
    }
}
