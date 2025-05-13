/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.coordinator;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.exception.FencedTieringEpochException;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.exception.TableNotExistException;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.server.entity.LakeTieringTableInfo;
import com.alibaba.fluss.server.utils.timer.DefaultTimer;
import com.alibaba.fluss.server.utils.timer.Timer;
import com.alibaba.fluss.server.utils.timer.TimerTask;
import com.alibaba.fluss.utils.clock.Clock;
import com.alibaba.fluss.utils.clock.SystemClock;
import com.alibaba.fluss.utils.concurrent.ExecutorThreadFactory;
import com.alibaba.fluss.utils.concurrent.ShutdownableThread;
import com.alibaba.fluss.utils.types.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.alibaba.fluss.utils.concurrent.LockUtils.inLock;

/**
 * A manager to manage the tables to be tiered.
 *
 * <p>For a lake table to be tiered, when created, it wil be put into this manager and scheduled to
 * be tiered by tiering services.
 *
 * <p>There are five states for the table to be tiered:
 *
 * <ul>
 *   <li>New: when a new lake table is created
 *   <li>Initialized: when the coordinator server is restarted, the state of all existing lake table
 *       will be Initialized state
 *   <li>Scheduled: when the lake table is waiting for a period of time to be tiered
 *   <li>Pending: when the lake table is waiting for tiering service to request the table
 *   <li>Tiering: when the lake table is being tiered by tiering service
 *   <li>Tiered: when the lake table finish one round of tiering
 *   <li>Failed: when the lake table tiering failed
 * </ul>
 *
 * <p>The state machine of table to be tiered is as follows:
 *
 * <pre>{@code
 * ┌─────┐ ┌──────┐
 * │ New │ │ Init │
 * └──┬──┘ └──┬───┘
 *    ▼       ▼
 *  ┌──────────┐ (lake freshness > tiering interval)
 *  │Scheduled ├──────┐
 *  └──────────┘      ▼
 *       ▲        ┌───────┐ (assign to tier service)  ┌───────┐
 *       |        |Pending├──────────────────────────►|Tiering├─┐
 *       |        └───────┘                           └───┬───┘ │
 *       |            ▲                 ┌─────────────────┘     │
 *       |            |                 | (timeout or failure)  | (finished)
 *       |            |                 ▼                       ▼
 *       |            |  (retry)   ┌─────────┐             ┌────────┐
 *       |            └────────────│ Failed  │             │ Tiered │
 *       |                         └─────────┘             └────┬───┘
 *       |                                                      |
 *       └──────────────────────────────────────────────────────┘
 *                   (ready for next round of tiering)
 * }</pre>
 */
public class LakeTableTieringManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(LakeTableTieringManager.class);

    protected static final long TIERING_SERVICE_TIMEOUT_MS = 2 * 60 * 1000; // 2 minutes

    private final Timer lakeTieringScheduleTimer;
    private final ScheduledExecutorService lakeTieringServiceTimeoutChecker;
    private final Clock clock;
    private final Queue<Long> pendingTieringTables;
    private final LakeTieringExpiredOperationReaper expirationReaper;

    // the tiering state of the table to be tiered,
    // from table_id -> tiering state
    private final Map<Long, TieringState> tieringStates;

    // table_id -> table path
    private final Map<Long, TablePath> tablePaths;

    // table_id -> freshness (tiering interval)
    private final Map<Long, Long> tableLakeFreshness;

    // cache table_id -> table tiering epoch
    private final Map<Long, Long> tableTierEpoch;

    // table_id -> the last timestamp of tiered lake snapshot
    private final Map<Long, Long> tableLastTieredTime;

    // the live tables that are tiering,
    // from table_id -> last heartbeat time by the tiering service
    private final Map<Long, Long> liveTieringTableIds;

    private final Lock lock = new ReentrantLock();

    public LakeTableTieringManager() {
        this(
                new DefaultTimer("delay lake tiering", 1_000, 20),
                Executors.newSingleThreadScheduledExecutor(
                        new ExecutorThreadFactory("fluss-lake-tiering-timeout-checker")),
                SystemClock.getInstance());
    }

    @VisibleForTesting
    protected LakeTableTieringManager(
            Timer lakeTieringScheduleTimer,
            ScheduledExecutorService lakeTieringServiceTimeoutChecker,
            Clock clock) {
        this.lakeTieringScheduleTimer = lakeTieringScheduleTimer;
        this.lakeTieringServiceTimeoutChecker = lakeTieringServiceTimeoutChecker;
        this.clock = clock;
        this.pendingTieringTables = new ArrayDeque<>();
        this.tieringStates = new HashMap<>();
        this.liveTieringTableIds = new HashMap<>();
        this.tablePaths = new HashMap<>();
        this.tableLakeFreshness = new HashMap<>();
        this.expirationReaper = new LakeTieringExpiredOperationReaper();
        expirationReaper.start();
        this.lakeTieringServiceTimeoutChecker.scheduleWithFixedDelay(
                this::checkTieringServiceTimeout, 0, 15, TimeUnit.SECONDS);
        this.tableTierEpoch = new HashMap<>();
        this.tableLastTieredTime = new HashMap<>();
    }

    public void initWithLakeTables(List<Tuple2<TableInfo, Long>> tableInfoWithTieredTime) {
        inLock(
                lock,
                () -> {
                    for (Tuple2<TableInfo, Long> tableInfoAndLastLakeTime :
                            tableInfoWithTieredTime) {
                        TableInfo tableInfo = tableInfoAndLastLakeTime.f0;
                        long lastTieredTime = tableInfoAndLastLakeTime.f1;
                        registerLakeTable(tableInfo, lastTieredTime);
                        doHandleStateChange(tableInfo.getTableId(), TieringState.Initialized);
                        // schedule it to be tiered after the tiering interval
                        doHandleStateChange(tableInfo.getTableId(), TieringState.Scheduled);
                    }
                });
    }

    public void addNewLakeTable(TableInfo tableInfo) {
        inLock(
                lock,
                () -> {
                    registerLakeTable(tableInfo, clock.milliseconds());
                    doHandleStateChange(tableInfo.getTableId(), TieringState.New);
                    // schedule it to be tiered after the tiering interval
                    doHandleStateChange(tableInfo.getTableId(), TieringState.Scheduled);
                });
    }

    @GuardedBy("lock")
    private void registerLakeTable(TableInfo tableInfo, long lastTieredTime) {
        long tableId = tableInfo.getTableId();
        tablePaths.put(tableId, tableInfo.getTablePath());
        tableLakeFreshness.put(
                tableId, tableInfo.getTableConfig().getDataLakeFreshness().toMillis());
        tableLastTieredTime.put(tableId, lastTieredTime);
        tableTierEpoch.put(tableId, 0L);
    }

    private void scheduleTableTiering(long tableId) {
        Long freshnessInterval = tableLakeFreshness.get(tableId);
        Long lastTieredTime = tableLastTieredTime.get(tableId);
        if (freshnessInterval == null || lastTieredTime == null) {
            // the table has been dropped, return directly
            return;
        }
        long delayMs = freshnessInterval - (clock.milliseconds() - lastTieredTime);
        // if the delayMs is < 0, the DelayedTiering will be triggered at once without
        // adding into timing wheel.
        lakeTieringScheduleTimer.add(new DelayedTiering(tableId, delayMs));
    }

    public void removeLakeTable(long tableId) {
        inLock(
                lock,
                () -> {
                    tablePaths.remove(tableId);
                    tableLakeFreshness.remove(tableId);
                    tableLastTieredTime.remove(tableId);
                    tieringStates.remove(tableId);
                    liveTieringTableIds.remove(tableId);
                    tableTierEpoch.remove(tableId);
                });
    }

    @VisibleForTesting
    protected void checkTieringServiceTimeout() {
        inLock(
                lock,
                () -> {
                    long currentTime = clock.milliseconds();
                    Map<Long, TablePath> timeoutTables = new HashMap<>();
                    liveTieringTableIds.forEach(
                            (tableId, lastHeartbeat) -> {
                                if (currentTime - lastHeartbeat >= TIERING_SERVICE_TIMEOUT_MS) {
                                    timeoutTables.put(tableId, tablePaths.get(tableId));
                                }
                            });
                    timeoutTables.forEach(
                            (tableId, tablePath) -> {
                                LOG.warn(
                                        "The lake tiering service for table {}({}) is timeout, change it to PENDING.",
                                        tablePaths.get(tableId),
                                        tableId);
                                doHandleStateChange(tableId, TieringState.Failed);
                                // then to pending state to enable other tiering service can
                                // pick it
                                doHandleStateChange(tableId, TieringState.Pending);
                            });
                });
    }

    @Nullable
    public LakeTieringTableInfo requestTable() {
        return inLock(
                lock,
                () -> {
                    Long tableId = pendingTieringTables.poll();
                    // no any pending table, return directly
                    if (tableId == null) {
                        return null;
                    }
                    TablePath tablePath = tablePaths.get(tableId);
                    // the table has been dropped, request again
                    if (tablePath == null) {
                        return requestTable();
                    }
                    doHandleStateChange(tableId, TieringState.Tiering);
                    long tieringEpoch = tableTierEpoch.get(tableId);
                    return new LakeTieringTableInfo(tableId, tablePath, tieringEpoch);
                });
    }

    public void finishTableTiering(long tableId, long tieredEpoch) {
        inLock(
                lock,
                () -> {
                    validateTieringServiceRequest(tableId, tieredEpoch);
                    // to tiered state firstly
                    doHandleStateChange(tableId, TieringState.Tiered);
                    // then to scheduled state to enable other tiering service can pick it
                    doHandleStateChange(tableId, TieringState.Scheduled);
                });
    }

    public void reportTieringFail(long tableId, long tieringEpoch) {
        inLock(
                lock,
                () -> {
                    validateTieringServiceRequest(tableId, tieringEpoch);
                    // to fail state firstly
                    doHandleStateChange(tableId, TieringState.Failed);
                    // then to pending state to enable other tiering service can pick it
                    doHandleStateChange(tableId, TieringState.Pending);
                });
    }

    public void renewTieringHeartbeat(long tableId, long tieringEpoch) {
        inLock(
                lock,
                () -> {
                    validateTieringServiceRequest(tableId, tieringEpoch);
                    TieringState tieringState = tieringStates.get(tableId);
                    if (tieringState != TieringState.Tiering) {
                        throw new IllegalStateException(
                                String.format(
                                        "The table %d to renew tiering heartbeat must in Tiering state, but in %s state.",
                                        tableId, tieringState));
                    }
                    liveTieringTableIds.put(tableId, clock.milliseconds());
                });
    }

    private void validateTieringServiceRequest(long tableId, long tieringEpoch) {
        Long currentEpoch = tableTierEpoch.get(tableId);
        // the table has been dropped, return false
        if (currentEpoch == null) {
            throw new TableNotExistException("The table " + tableId + " doesn't exist.");
        }
        if (tieringEpoch != currentEpoch) {
            throw new FencedTieringEpochException(
                    String.format(
                            "The tiering epoch %d is not match current epoch %d in coordinator for table %d.",
                            tieringEpoch, currentEpoch, tableId));
        }
    }

    /**
     * Handle the state change of the lake table to be tiered. The core state transitions for the
     * state machine are as follows:
     *
     * <p>New -> Scheduled:
     *
     * <p>-- When the lake table is newly created, do: schedule a timer to wait for a freshness
     * interval configured in table which will transmit the table to Pending.
     *
     * <p>Initialized -> Scheduled：
     *
     * <p>-- When the coordinator server is restarted, for all existing lake table, if the interval
     * from last lake snapshot is not less than tiering interval, do: transmit to Pending, otherwise
     * schedule a timer to wait for a freshness interval which will transmit the table to Pending.
     *
     * <p>Scheduled -> Pending
     *
     * <p>-- The freshness interval to wait has passed, do: transmit to Pending state
     *
     * <p>Failed -> Pending
     *
     * <p>-- The previous tiering service failed to tier the table, retry to tier again, do:
     * transmit to Pending state
     *
     * <p>Pending -> Tiering
     *
     * <p>-- When the table is assigned to a tiering service after tiering service request the
     * table, do: transmit to Tiering state
     *
     * <p>Tiering -> Tiered
     *
     * <p>-- When the tiering service finished the table, do: transmit to Tiered state
     *
     * <p>Tiering -> Failed
     *
     * <p>-- When the tiering service timeout to report heartbeat or report failure for the table,
     * do: transmit to Tiered state
     */
    private void doHandleStateChange(long tableId, TieringState targetState) {
        TieringState currentState = tieringStates.get(tableId);
        if (!isValidStateTransition(currentState, targetState)) {
            LOG.error(
                    "Fail to change state for table {} from {} to {} as it's not a valid state change.",
                    tableId,
                    currentState,
                    targetState);
            return;
        }
        switch (targetState) {
            case New:
            case Initialized:
                // do nothing
                break;
            case Scheduled:
                scheduleTableTiering(tableId);
                break;
            case Pending:
                // increase tiering epoch and initialize the heartbeat of the tiering table
                tableTierEpoch.computeIfPresent(tableId, (t, v) -> v + 1);
                pendingTieringTables.add(tableId);
                break;
            case Tiering:
                liveTieringTableIds.put(tableId, clock.milliseconds());
                break;
            case Tiered:
            case Failed:
                liveTieringTableIds.remove(tableId);
                // do nothing
                break;
        }
        doStateChange(tableId, currentState, targetState);
    }

    private boolean isValidStateTransition(
            @Nullable TieringState curState, TieringState targetState) {
        if (targetState == TieringState.New || targetState == TieringState.Initialized) {
            // when target state is new or Initialized, it's valid when current state is null
            return curState == null;
        }
        if (curState == null) {
            // the table is dropped, shouldn't continue to do state transition
            return false;
        }
        return targetState.validPreviousStates().contains(curState);
    }

    private void doStateChange(long tableId, TieringState fromState, TieringState toState) {
        tieringStates.put(tableId, toState);
        LOG.debug(
                "Successfully changed tiering state for table {} from {} to {}.",
                tableId,
                fromState,
                fromState);
    }

    @Override
    public void close() throws Exception {
        lakeTieringServiceTimeoutChecker.shutdown();
        expirationReaper.initiateShutdown();
        // improve shutdown time by waking up any ShutdownableThread(s) blocked on poll by
        // sending a no-op.
        lakeTieringScheduleTimer.add(
                new TimerTask(0) {
                    @Override
                    public void run() {}
                });
        try {
            expirationReaper.awaitShutdown();
        } catch (InterruptedException e) {
            throw new FlussRuntimeException(
                    "Error while shutdown lake tiering expired operation manager", e);
        }

        lakeTieringScheduleTimer.shutdown();
    }

    private class DelayedTiering extends TimerTask {

        private final long tableId;

        public DelayedTiering(long tableId, long delayMs) {
            super(delayMs);
            this.tableId = tableId;
        }

        @Override
        public void run() {
            inLock(
                    lock,
                    () ->
                            // to pending state
                            doHandleStateChange(tableId, TieringState.Pending));
        }
    }

    private class LakeTieringExpiredOperationReaper extends ShutdownableThread {

        public LakeTieringExpiredOperationReaper() {
            super("LakeTieringExpiredOperationReaper", false);
        }

        @Override
        public void doWork() throws Exception {
            advanceClock();
        }

        private void advanceClock() throws InterruptedException {
            lakeTieringScheduleTimer.advanceClock(200L);
        }
    }

    private enum TieringState {
        // When a new lake table is created, the state will be New
        New {
            @Override
            public Set<TieringState> validPreviousStates() {
                return Collections.emptySet();
            }
        },
        // When the coordinator server is restarted, the state of existing lake table
        // will be Initialized
        Initialized {
            @Override
            public Set<TieringState> validPreviousStates() {
                return Collections.emptySet();
            }
        },
        // When the lake table is waiting to be tiered, such as waiting for the period of tiering
        // interval, the state will be Scheduled
        Scheduled {
            @Override
            public Set<TieringState> validPreviousStates() {
                return EnumSet.of(New, Initialized, Tiered);
            }
        },
        // When the period of tiering interval has passed, but no any tiering service requesting
        // table, the state will be Pending
        Pending {
            @Override
            public Set<TieringState> validPreviousStates() {
                return EnumSet.of(Scheduled, Failed);
            }
        },
        // When one tiering service is tiering the table, the state will be Tiering
        Tiering {
            @Override
            public Set<TieringState> validPreviousStates() {
                return EnumSet.of(Pending);
            }
        },

        // When one tiering service has successfully tiered the table, the state will be Tiered
        Tiered {
            @Override
            public Set<TieringState> validPreviousStates() {
                return EnumSet.of(Tiering);
            }
        },
        // When one tiering service fail or timeout to tier the table, the state will be Failed
        Failed {
            @Override
            public Set<TieringState> validPreviousStates() {
                return EnumSet.of(Tiering);
            }
        };

        abstract Set<TieringState> validPreviousStates();
    }
}
