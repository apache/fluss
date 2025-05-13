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

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.exception.FencedTieringEpochException;
import com.alibaba.fluss.exception.TableNotExistException;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.server.entity.LakeTieringTableInfo;
import com.alibaba.fluss.server.utils.timer.DefaultTimer;
import com.alibaba.fluss.testutils.common.ManuallyTriggeredScheduledExecutorService;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.utils.clock.ManualClock;
import com.alibaba.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.alibaba.fluss.server.coordinator.LakeTableTieringManager.TIERING_SERVICE_TIMEOUT_MS;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link LakeTableTieringManager}. */
class LakeTableTieringManagerTest {

    private LakeTableTieringManager tableTieringManager;
    private ManualClock manualClock;
    private ManuallyTriggeredScheduledExecutorService lakeTieringServiceTimeoutChecker;

    @BeforeEach
    void beforeEach() {
        manualClock = new ManualClock();
        lakeTieringServiceTimeoutChecker = new ManuallyTriggeredScheduledExecutorService();
        tableTieringManager = createLakeTableTieringManager();
    }

    private LakeTableTieringManager createLakeTableTieringManager() {
        return new LakeTableTieringManager(
                new DefaultTimer("delay lake tiering", 1_000, 20, manualClock),
                lakeTieringServiceTimeoutChecker,
                manualClock);
    }

    @Test
    void testInitLakeTableTieringManagerWithTables() {
        long tableId1 = 1L;
        TablePath tablePath1 = TablePath.of("db", "table1");
        TableInfo tableInfo1 = createTableInfo(tableId1, tablePath1, Duration.ofMinutes(3));

        long tableId2 = 2L;
        TablePath tablePath2 = TablePath.of("db", "table2");
        TableInfo tableInfo2 = createTableInfo(tableId2, tablePath2, Duration.ofMinutes(3));

        List<Tuple2<TableInfo, Long>> lakeTables =
                Arrays.asList(
                        Tuple2.of(tableInfo1, manualClock.milliseconds()),
                        // the last lake snapshot of table2 is older than 3 minutes, should be
                        // tiered right now
                        Tuple2.of(
                                tableInfo2,
                                manualClock.milliseconds() - Duration.ofMinutes(3).toMillis()));
        tableTieringManager.initWithLakeTables(lakeTables);
        // table2 should be PENDING at once without async scheduling
        assertRequestTable(tableId2, tablePath2, 1);

        // advance 3 min to trigger table1 to be tiered
        manualClock.advanceTime(Duration.ofMinutes(3));
        assertRequestTable(tableId1, tablePath1, 1);
    }

    @Test
    void testAddNewLakeTable() {
        long tableId1 = 1L;
        TablePath tablePath1 = TablePath.of("db", "table");
        TableInfo tableInfo1 = createTableInfo(tableId1, tablePath1, Duration.ofSeconds(10));
        tableTieringManager.addNewLakeTable(tableInfo1);

        // advance time to trigger the table tiering
        manualClock.advanceTime(Duration.ofSeconds(10));
        assertRequestTable(tableId1, tablePath1, 1);
    }

    @Test
    void testRemoveLakeTable() {
        long tableId1 = 1L;
        TablePath tablePath1 = TablePath.of("db", "table");
        TableInfo tableInfo1 = createTableInfo(tableId1, tablePath1, Duration.ofSeconds(10));
        tableTieringManager.addNewLakeTable(tableInfo1);

        long tableId2 = 2L;
        TablePath tablePath2 = TablePath.of("db", "table2");
        TableInfo tableInfo2 = createTableInfo(tableId2, tablePath2, Duration.ofSeconds(10));
        tableTieringManager.addNewLakeTable(tableInfo2);

        // remove the tableId1
        tableTieringManager.removeLakeTable(tableId1);

        // advance time to trigger the table tiering
        manualClock.advanceTime(Duration.ofSeconds(10));
        // shouldn't get tableId1, should only get tableId2
        assertRequestTable(tableId2, tablePath2, 1);

        // verify the request for table1 should throw table not exist exception
        assertThatThrownBy(() -> tableTieringManager.renewTieringHeartbeat(tableId1, 1))
                .isInstanceOf(TableNotExistException.class)
                .hasMessage("The table %d doesn't exist.", tableId1);
        assertThatThrownBy(() -> tableTieringManager.reportTieringFail(tableId1, 1))
                .isInstanceOf(TableNotExistException.class)
                .hasMessage("The table %d doesn't exist.", tableId1);
        assertThatThrownBy(() -> tableTieringManager.finishTableTiering(tableId1, 1))
                .isInstanceOf(TableNotExistException.class)
                .hasMessage("The table %d doesn't exist.", tableId1);
    }

    @Test
    void testFinishTableTieringReTriggerSchedule() {
        long tieredEpoch = 1;
        long tableId1 = 1L;
        TablePath tablePath1 = TablePath.of("db", "table");
        TableInfo tableInfo1 = createTableInfo(tableId1, tablePath1, Duration.ofSeconds(10));
        tableTieringManager.addNewLakeTable(tableInfo1);

        manualClock.advanceTime(Duration.ofSeconds(10));
        // check requested table
        assertRequestTable(tableId1, tablePath1, 1);

        // request table should return null
        assertThat(tableTieringManager.requestTable()).isNull();

        // mock lake tiering finish one-round tiering
        tableTieringManager.finishTableTiering(tableId1, tieredEpoch);
        // not advance time, request table should return null
        assertThat(tableTieringManager.requestTable()).isNull();

        // now, advance time to trigger the table tiering
        manualClock.advanceTime(Duration.ofSeconds(10));
        // the tiered epoch should be 2 now
        assertRequestTable(tableId1, tablePath1, 2);
    }

    @Test
    void testTieringServiceTimeOutReTriggerPending() {
        long tableId1 = 1L;
        TablePath tablePath1 = TablePath.of("db", "table1");
        TableInfo tableInfo1 = createTableInfo(tableId1, tablePath1, Duration.ofSeconds(10));
        tableTieringManager.addNewLakeTable(tableInfo1);
        long tableId2 = 2L;
        TablePath tablePath2 = TablePath.of("db", "table2");
        TableInfo tableInfo2 = createTableInfo(tableId2, tablePath2, Duration.ofSeconds(10));
        tableTieringManager.addNewLakeTable(tableInfo2);

        manualClock.advanceTime(Duration.ofSeconds(10));
        // check requested table
        assertRequestTable(tableId1, tablePath1, 1);
        assertRequestTable(tableId2, tablePath2, 1);

        // advance time and mock tiering service heartbeat
        manualClock.advanceTime(Duration.ofMillis(TIERING_SERVICE_TIMEOUT_MS - 1));
        // tableid1 renew the tiering heartbeat, so that it won't be
        // re-pending after heartbeat timeout
        tableTieringManager.renewTieringHeartbeat(tableId1, 1);
        // should only get table2
        manualClock.advanceTime(Duration.ofSeconds(10));
        lakeTieringServiceTimeoutChecker.triggerPeriodicScheduledTasks();
        assertRequestTable(tableId2, tablePath2, 2);

        // advance a large time to mock tiering service heartbeat timeout
        // and check the request table, the table1 should be re-scheduled
        manualClock.advanceTime(Duration.ofMinutes(5));
        tableTieringManager.checkTieringServiceTimeout();
        assertRequestTable(tableId1, tablePath1, 2);

        // now, assume the previous tiering service come alive, try to send request for the table1
        // should throw FencedTieringEpochException
        assertThatThrownBy(() -> tableTieringManager.renewTieringHeartbeat(tableId1, 1))
                .isInstanceOf(FencedTieringEpochException.class)
                .hasMessage(
                        "The tiering epoch %d is not match current epoch %d in coordinator for table %d.",
                        1, 2, tableId1);
        assertThatThrownBy(() -> tableTieringManager.reportTieringFail(tableId1, 1))
                .isInstanceOf(FencedTieringEpochException.class)
                .hasMessage(
                        "The tiering epoch %d is not match current epoch %d in coordinator for table %d.",
                        1, 2, tableId1);
        assertThatThrownBy(() -> tableTieringManager.finishTableTiering(tableId1, 1))
                .isInstanceOf(FencedTieringEpochException.class)
                .hasMessage(
                        "The tiering epoch %d is not match current epoch %d in coordinator for table %d.",
                        1, 2, tableId1);
        assertThatThrownBy(() -> tableTieringManager.finishTableTiering(tableId1, 3))
                .isInstanceOf(FencedTieringEpochException.class)
                .hasMessage(
                        "The tiering epoch %d is not match current epoch %d in coordinator for table %d.",
                        3, 2, tableId1);
    }

    @Test
    void testTieringFail() {
        long tableId1 = 1L;
        TablePath tablePath1 = TablePath.of("db", "table1");
        TableInfo tableInfo1 = createTableInfo(tableId1, tablePath1, Duration.ofSeconds(10));
        tableTieringManager.addNewLakeTable(tableInfo1);
        manualClock.advanceTime(Duration.ofSeconds(10));
        assertRequestTable(tableId1, tablePath1, 1);

        // should be re-pending after tiering fail
        tableTieringManager.reportTieringFail(tableId1, 1);
        // we should get the table again
        assertRequestTable(tableId1, tablePath1, 2);
    }

    private TableInfo createTableInfo(long tableId, TablePath tablePath, Duration freshness) {
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("c1", DataTypes.INT()).build())
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, freshness)
                        .distributedBy(1)
                        .build();

        return TableInfo.of(
                tablePath,
                tableId,
                1,
                tableDescriptor,
                System.currentTimeMillis(),
                System.currentTimeMillis());
    }

    private void assertRequestTable(long tableId, TablePath tablePath, long tieredEpoch) {
        LakeTieringTableInfo table =
                waitValue(
                        () -> Optional.ofNullable(tableTieringManager.requestTable()),
                        Duration.ofSeconds(10),
                        "Request tiering table timout");
        assertThat(table).isEqualTo(new LakeTieringTableInfo(tableId, tablePath, tieredEpoch));
    }
}
