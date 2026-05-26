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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.server.coordinator.event.CoordinatorEvent;
import org.apache.fluss.server.coordinator.event.DropPartitionEvent;
import org.apache.fluss.server.coordinator.event.DropTableEvent;
import org.apache.fluss.server.coordinator.event.EventManager;
import org.apache.fluss.utils.clock.ManualClock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link ReplicaCleanupManager}. */
class ReplicaCleanupManagerTest {

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
    void testSubmitPartitionDropAdmitsImmediatelyWithinBudget() {
        ReplicaCleanupManager manager = newManager(configWithBucketBudget(1000));

        manager.submitPartitionDrop(1L, 100L, "p20240101", 4);

        assertThat(eventManager.summaries()).containsExactly("partition:1:100:p20240101");
        assertThat(manager.getInflightCount()).isOne();
        assertThat(manager.getInflightBuckets()).isEqualTo(4);
        assertThat(manager.getPendingDropCount()).isZero();
    }

    @Test
    void testSubmitTableDropAdmitsImmediatelyWithinBudget() {
        ReplicaCleanupManager manager = newManager(configWithBucketBudget(1000));

        manager.submitTableDrop(7L, true, false, 8);

        assertThat(eventManager.summaries()).containsExactly("table:7:auto=true:lake=false");
        assertThat(manager.getInflightCount()).isOne();
        assertThat(manager.getInflightBuckets()).isEqualTo(8);
    }

    @Test
    void testBucketBudgetHoldsBackExtraDrops() {
        // budget = 1000; admit greedily while running sum stays within budget.
        ReplicaCleanupManager manager = newManager(configWithBucketBudget(1000));

        // 3 partitions @ 300 buckets each = 900 (still <= 1000), 4th @ 300 would push to 1200.
        manager.submitPartitionDrop(1L, 100L, "p1", 300);
        manager.submitPartitionDrop(1L, 101L, "p2", 300);
        manager.submitPartitionDrop(1L, 102L, "p3", 300);
        manager.submitPartitionDrop(1L, 103L, "p4", 300);

        assertThat(eventManager.summaries())
                .containsExactly("partition:1:100:p1", "partition:1:101:p2", "partition:1:102:p3");
        assertThat(manager.getInflightBuckets()).isEqualTo(900);
        assertThat(manager.getInflightCount()).isEqualTo(3);
        assertThat(manager.getPendingDropCount()).isOne();
    }

    @Test
    void testOversizedHeadOfQueueDropAdmittedAlone() {
        // Single table whose bucket count already exceeds the budget on its own.
        ReplicaCleanupManager manager = newManager(configWithBucketBudget(1000));

        manager.submitTableDrop(1L, false, false, 5000);
        manager.submitPartitionDrop(2L, 200L, "p2", 100);
        manager.submitPartitionDrop(2L, 201L, "p3", 100);

        // The oversized head is admitted on its own (starvation guard); subsequent drops wait
        // until it completes, even though their cumulative bucket count would otherwise fit.
        assertThat(eventManager.summaries()).containsExactly("table:1:auto=false:lake=false");
        assertThat(manager.getInflightBuckets()).isEqualTo(5000);
        assertThat(manager.getInflightCount()).isOne();
        assertThat(manager.getPendingDropCount()).isEqualTo(2);

        manager.onTableDropCompleted(1L);

        // After completion the next two partitions fit within budget and are admitted together.
        assertThat(eventManager.summaries())
                .containsExactly(
                        "table:1:auto=false:lake=false",
                        "partition:2:200:p2",
                        "partition:2:201:p3");
        assertThat(manager.getInflightBuckets()).isEqualTo(200);
    }

    @Test
    void testCallbackReleasesBudgetAndPullsNextPending() {
        ReplicaCleanupManager manager = newManager(configWithBucketBudget(1000));

        manager.submitPartitionDrop(1L, 100L, "p1", 600);
        manager.submitPartitionDrop(1L, 101L, "p2", 600); // pends: 600+600 > 1000
        manager.submitPartitionDrop(1L, 102L, "p3", 300);

        assertThat(eventManager.summaries()).containsExactly("partition:1:100:p1");
        assertThat(manager.getPendingDropCount()).isEqualTo(2);
        assertThat(manager.getInflightBuckets()).isEqualTo(600);

        manager.onPartitionDropCompleted(new TablePartition(1L, 100L));

        assertThat(eventManager.summaries())
                .containsExactly("partition:1:100:p1", "partition:1:101:p2", "partition:1:102:p3");
        assertThat(manager.getInflightBuckets()).isEqualTo(900);
        assertThat(manager.getPendingDropCount()).isZero();
    }

    @Test
    void testNonPositiveBatchCapDisablesLimit() {
        ReplicaCleanupManager manager = newManager(configWithBucketBudget(0));

        manager.submitTableDrop(1L, false, false, 100_000);
        manager.submitTableDrop(2L, true, true, 50_000);
        manager.submitPartitionDrop(3L, 30L, "px", 10_000);

        assertThat(eventManager.summaries())
                .containsExactly(
                        "table:1:auto=false:lake=false",
                        "table:2:auto=true:lake=true",
                        "partition:3:30:px");
        assertThat(manager.getPendingDropCount()).isZero();
    }

    @Test
    void testFifoOrderForPendingDrops() {
        // Each drop on its own equals the budget, so they admit one at a time in order.
        ReplicaCleanupManager manager = newManager(configWithBucketBudget(100));

        manager.submitPartitionDrop(1L, 10L, "p10", 100);
        manager.submitPartitionDrop(1L, 11L, "p11", 100);
        manager.submitPartitionDrop(1L, 12L, "p12", 100);

        assertThat(eventManager.summaries()).containsExactly("partition:1:10:p10");

        manager.onPartitionDropCompleted(new TablePartition(1L, 10L));
        manager.onPartitionDropCompleted(new TablePartition(1L, 11L));

        assertThat(eventManager.summaries())
                .containsExactly("partition:1:10:p10", "partition:1:11:p11", "partition:1:12:p12");
    }

    @Test
    void testZeroBucketCountNormalizedToMinimumCost() {
        // bucketCount=0 means caller couldn't determine the size (e.g. cascade drop where the
        // table znode was already gone). The manager normalizes it to a minimum cost of 1 so
        // that every drop still contributes to the budget and cannot bypass throttling.
        ReplicaCleanupManager manager = newManager(configWithBucketBudget(2));

        manager.submitPartitionDrop(1L, 100L, "p1", 0);
        manager.submitPartitionDrop(1L, 101L, "p2", 0);
        manager.submitPartitionDrop(1L, 102L, "p3", 0);

        // Each drop costs 1 bucket; budget=2 admits only the first two.
        assertThat(eventManager.summaries())
                .containsExactly("partition:1:100:p1", "partition:1:101:p2");
        assertThat(manager.getInflightBuckets()).isEqualTo(2);
        assertThat(manager.getPendingDropCount()).isOne();

        // Completing one frees room for the third.
        manager.onPartitionDropCompleted(new TablePartition(1L, 100L));
        assertThat(eventManager.summaries())
                .containsExactly("partition:1:100:p1", "partition:1:101:p2", "partition:1:102:p3");
    }

    @Test
    void testCompletionForUnknownDropIsNoOp() {
        ReplicaCleanupManager manager = newManager(configWithBucketBudget(1000));

        manager.onPartitionDropCompleted(new TablePartition(99L, 999L));
        manager.onTableDropCompleted(123L);

        assertThat(eventManager.summaries()).isEmpty();
        assertThat(manager.getInflightCount()).isZero();
        assertThat(manager.getInflightBuckets()).isZero();
    }

    @Test
    void testTimeoutAbandonsExpiredInflightDrops() {
        Configuration conf = configWithBucketBudget(100);
        conf.set(
                ConfigOptions.COORDINATOR_REPLICA_CLEANUP_INFLIGHT_TIMEOUT, Duration.ofMillis(100));
        ReplicaCleanupManager manager = newManager(conf);

        manager.submitTableDrop(1L, false, false, 100);
        manager.submitPartitionDrop(2L, 200L, "px", 100);

        // budget = 100 buckets; second drop pends behind the first.
        assertThat(manager.getInflightCount()).isOne();
        assertThat(manager.getInflightBuckets()).isEqualTo(100);
        assertThat(manager.getPendingDropCount()).isOne();

        // not yet expired
        clock.advanceTime(Duration.ofMillis(50));
        manager.checkTimeouts();
        assertThat(manager.getInflightCount()).isOne();

        // crosses the timeout boundary, in-flight gets abandoned and pending pulled in
        clock.advanceTime(Duration.ofMillis(60));
        manager.checkTimeouts();

        assertThat(eventManager.summaries())
                .containsExactly("table:1:auto=false:lake=false", "partition:2:200:px");
        assertThat(manager.getPendingDropCount()).isZero();
        assertThat(manager.getInflightCount()).isOne();
        assertThat(manager.getInflightBuckets()).isEqualTo(100);
    }

    @Test
    void testDuplicateCompletionCallbackIsNoOp() {
        ReplicaCleanupManager manager = newManager(configWithBucketBudget(1000));
        TablePartition tp = new TablePartition(1L, 100L);

        manager.submitPartitionDrop(tp.getTableId(), tp.getPartitionId(), "px", 4);
        assertThat(manager.getInflightCount()).isOne();
        assertThat(manager.getInflightBuckets()).isEqualTo(4);

        manager.onPartitionDropCompleted(tp);
        assertThat(manager.getInflightCount()).isZero();
        assertThat(manager.getInflightBuckets()).isZero();

        // second completion is a no-op (inflight entry already removed)
        manager.onPartitionDropCompleted(tp);
        assertThat(manager.getInflightCount()).isZero();
        assertThat(manager.getInflightBuckets()).isZero();
    }

    @Test
    void testStartIsIdempotentAndCloseDisablesStart() {
        ReplicaCleanupManager manager = newManager(configWithBucketBudget(1000));

        manager.start();
        manager.start(); // second start is a no-op

        manager.close();

        assertThatThrownBy(manager::start).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testCloseIsIdempotent() {
        ReplicaCleanupManager manager = newManager(configWithBucketBudget(1000));

        manager.close();
        manager.close(); // must not throw
    }

    @Test
    void testSubmitTableDropForResumeRunsRunnableInsteadOfEvent() {
        // Resume path used during coordinator startup: stale TableInfo is gone, so the manager
        // must drive the deletion via the supplied Runnable rather than enqueue DropTableEvent.
        ReplicaCleanupManager manager = newManager(configWithBucketBudget(1000));
        AtomicInteger invoked = new AtomicInteger();

        manager.submitTableDropForResume(7L, 8, invoked::incrementAndGet);

        assertThat(invoked).hasValue(1);
        assertThat(eventManager.summaries()).isEmpty();
        assertThat(manager.getInflightCount()).isOne();
        assertThat(manager.getInflightBuckets()).isEqualTo(8);
    }

    @Test
    void testSubmitPartitionDropForResumeRunsRunnableInsteadOfEvent() {
        ReplicaCleanupManager manager = newManager(configWithBucketBudget(1000));
        AtomicInteger invoked = new AtomicInteger();

        manager.submitPartitionDropForResume(1L, 100L, "p20240101", 4, invoked::incrementAndGet);

        assertThat(invoked).hasValue(1);
        assertThat(eventManager.summaries()).isEmpty();
        assertThat(manager.getInflightCount()).isOne();
        assertThat(manager.getInflightBuckets()).isEqualTo(4);
    }

    @Test
    void testForResumeDropRespectsBucketBudget() {
        // The resume path shares the same bucket-budget queue: oversized drops still wait.
        ReplicaCleanupManager manager = newManager(configWithBucketBudget(100));
        AtomicInteger first = new AtomicInteger();
        AtomicInteger second = new AtomicInteger();

        manager.submitTableDropForResume(1L, 100, first::incrementAndGet);
        manager.submitPartitionDropForResume(2L, 200L, "px", 100, second::incrementAndGet);

        // First admitted, second pends.
        assertThat(first).hasValue(1);
        assertThat(second).hasValue(0);
        assertThat(manager.getInflightCount()).isOne();
        assertThat(manager.getPendingDropCount()).isOne();

        // Completion of the first releases the budget and pulls the second resume runnable.
        manager.onTableDropCompleted(1L);
        assertThat(second).hasValue(1);
        assertThat(eventManager.summaries()).isEmpty();
    }

    // ------------------------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------------------------

    private ReplicaCleanupManager newManager(Configuration conf) {
        return new ReplicaCleanupManager(eventManager, conf, clock, timeoutExecutor);
    }

    private static Configuration configWithBucketBudget(int budget) {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.COORDINATOR_REPLICA_CLEANUP_MAX_BUCKETS_PER_BATCH, budget);
        return conf;
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
     * over when {@link ReplicaCleanupManager#checkTimeouts()} is invoked.
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
