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
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.DataLakeFormat;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.rpc.gateway.AdminGateway;
import com.alibaba.fluss.rpc.gateway.CoordinatorGateway;
import com.alibaba.fluss.rpc.messages.ErrorResponse;
import com.alibaba.fluss.rpc.messages.LakeTieringHeartbeatRequest;
import com.alibaba.fluss.rpc.messages.LakeTieringHeartbeatResponse;
import com.alibaba.fluss.rpc.messages.PbHeartbeatRespForTable;
import com.alibaba.fluss.rpc.messages.PbLakeTieringTableInfo;
import com.alibaba.fluss.rpc.messages.PbTablePath;
import com.alibaba.fluss.rpc.protocol.Errors;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import com.alibaba.fluss.types.DataTypes;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.alibaba.fluss.server.coordinator.CoordinatorContext.INITIAL_COORDINATOR_EPOCH;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newCreateTableRequest;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for {@link CoordinatorService#lakeTieringHeartbeat}. */
class LakeTieringHeartbeatITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(Configuration.fromMap(getDataLakeFormat()))
                    .build();

    private static CoordinatorGateway coordinatorGateway;

    private static Map<String, String> getDataLakeFormat() {
        Map<String, String> datalakeFormat = new HashMap<>();
        datalakeFormat.put(ConfigOptions.DATALAKE_FORMAT.key(), DataLakeFormat.PAIMON.toString());
        return datalakeFormat;
    }

    @BeforeAll
    static void beforeAll() {
        coordinatorGateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
    }

    @Test
    void testLakeTieringHeartBeat() throws Exception {
        int tableCounts = 4;
        // firstly, create 4 tables;
        createLakeTables(tableCounts);

        // verify coordinator epoch in response
        LakeTieringHeartbeatResponse heartbeatResponse =
                coordinatorGateway.lakeTieringHeartbeat(new LakeTieringHeartbeatRequest()).get();
        assertThat(heartbeatResponse.getCoordinatorEpoch()).isEqualTo(INITIAL_COORDINATOR_EPOCH);

        for (int i = 0; i < tableCounts; i++) {
            verifyGetTableToTier(i);
        }
        // now, no table to tier
        verifyNoAnyTableToTier();

        // send heartbeat to mark some table has been finished, and some has been failed
        LakeTieringHeartbeatRequest request =
                makeTieringHeartbeatRequest(new long[] {0}, new long[] {1, 3}, new long[] {2});
        coordinatorGateway.lakeTieringHeartbeat(request).get();

        // then, request table via heartbeat, should get the tables has been finished and failed
        Set<Long> expectedTieredTableIds = new HashSet<>(Arrays.asList(1L, 3L, 2L));
        for (int i = 0; i < 3; i++) {
            PbLakeTieringTableInfo pbLakeTieringTableInfo = waitGetLakeTieringTableInfo();
            long tieredTableId = pbLakeTieringTableInfo.getTableId();
            assertThat(expectedTieredTableIds.remove(tieredTableId)).isTrue();
            // tiering epoch should be 2 then
            verifyLakeTieringTableInfo(pbLakeTieringTableInfo, tieredTableId, 2);
        }
        // no table to tier
        verifyNoAnyTableToTier();

        // verify error response for the tables carried in heartbeat request,
        // the tiering epoch for table 0,2,3 in request is 1, but actual is 2, should throw
        // exception
        request = makeTieringHeartbeatRequest(new long[] {0}, new long[] {1, 2}, new long[] {3});
        LakeTieringHeartbeatResponse lakeTieringHeartbeatResponse =
                coordinatorGateway.lakeTieringHeartbeat(request).get();
        // error will only be in finish & failed response
        List<PbHeartbeatRespForTable> tieringTableResps =
                lakeTieringHeartbeatResponse.getTieringTableRespsList();
        assertThat(tieringTableResps).hasSize(1);
        assertThat(tieringTableResps.get(0).getTableId()).isEqualTo(0);
        assertThat(tieringTableResps.get(0).hasError()).isFalse();

        List<PbHeartbeatRespForTable> finishedTableResps =
                lakeTieringHeartbeatResponse.getFinishedTableRespsList();
        assertThat(finishedTableResps).hasSize(2);
        for (PbHeartbeatRespForTable heartbeatRespForTable : finishedTableResps) {
            verifyFencedTieringEpochException(
                    heartbeatRespForTable, 1, 2, heartbeatRespForTable.getTableId());
        }

        List<PbHeartbeatRespForTable> failedTableResps =
                lakeTieringHeartbeatResponse.getFailedTableRespsList();
        verifyFencedTieringEpochException(failedTableResps.get(0), 1, 2, 3);

        // now, let's finish some table and get the table again
        request =
                makeTieringHeartbeatRequest(
                        new long[0],
                        new long[] {1, 2, 3},
                        new long[0],
                        INITIAL_COORDINATOR_EPOCH,
                        2);
        coordinatorGateway.lakeTieringHeartbeat(request).get();

        expectedTieredTableIds = new HashSet<>(Arrays.asList(1L, 2L, 3L));
        // request table
        for (int i = 0; i < 3; i++) {
            PbLakeTieringTableInfo pbLakeTieringTableInfo = waitGetLakeTieringTableInfo();
            long tieredTableId = pbLakeTieringTableInfo.getTableId();
            assertThat(expectedTieredTableIds.remove(tieredTableId)).isTrue();
            // tiering epoch should be 3 then
            verifyLakeTieringTableInfo(pbLakeTieringTableInfo, tieredTableId, 3);
        }

        // verify error response for the tiering tables carried in heartbeat request
        // when tiering epoch is not match current tiering epoch
        request =
                makeTieringHeartbeatRequest(
                        new long[] {1, 2, 3},
                        new long[0],
                        new long[0],
                        INITIAL_COORDINATOR_EPOCH,
                        2);
        lakeTieringHeartbeatResponse = coordinatorGateway.lakeTieringHeartbeat(request).get();
        tieringTableResps = lakeTieringHeartbeatResponse.getTieringTableRespsList();
        assertThat(tieringTableResps).hasSize(3);
        for (PbHeartbeatRespForTable pbHeartbeatRespForTable : tieringTableResps) {
            verifyFencedTieringEpochException(
                    pbHeartbeatRespForTable, 2, 3, pbHeartbeatRespForTable.getTableId());
        }

        // verify error response for the tables carried in heartbeat request
        // when coordinator epoch is not match current coordinator epoch
        int invalidEpoch = INITIAL_COORDINATOR_EPOCH + 1;
        request =
                makeTieringHeartbeatRequest(
                        new long[] {1}, new long[] {2}, new long[] {3}, invalidEpoch, 2);
        lakeTieringHeartbeatResponse = coordinatorGateway.lakeTieringHeartbeat(request).get();
        // collect all resp
        List<PbHeartbeatRespForTable> pbHeartbeatRespForTables =
                new ArrayList<>(lakeTieringHeartbeatResponse.getTieringTableRespsList());
        pbHeartbeatRespForTables.addAll(lakeTieringHeartbeatResponse.getFinishedTableRespsList());
        pbHeartbeatRespForTables.addAll(lakeTieringHeartbeatResponse.getFailedTableRespsList());
        for (PbHeartbeatRespForTable pbHeartbeatRespForTable : pbHeartbeatRespForTables) {
            verifyError(
                    pbHeartbeatRespForTable.getError(),
                    Errors.INVALID_COORDINATOR_EXCEPTION.code(),
                    String.format(
                            "The coordinator epoch %s in request is not match current coordinator epoch %d for table %d.",
                            invalidEpoch,
                            INITIAL_COORDINATOR_EPOCH,
                            pbHeartbeatRespForTable.getTableId()));
        }
    }

    private void verifyNoAnyTableToTier() throws Exception {
        assertThat(
                        coordinatorGateway
                                .lakeTieringHeartbeat(makeRequestTableTieringHeartbeat())
                                .get()
                                .hasTieringTable())
                .isFalse();
    }

    private LakeTieringHeartbeatRequest makeRequestTableTieringHeartbeat() {
        LakeTieringHeartbeatRequest heartbeatRequest = new LakeTieringHeartbeatRequest();
        heartbeatRequest.setRequestTable(true);
        return heartbeatRequest;
    }

    private void verifyGetTableToTier(long expectTableId) {
        PbLakeTieringTableInfo pbLakeTieringTableInfo = waitGetLakeTieringTableInfo();
        verifyLakeTieringTableInfo(pbLakeTieringTableInfo, expectTableId);
    }

    private void verifyFencedTieringEpochException(
            PbHeartbeatRespForTable pbHeartbeatRespForTable,
            int tieringEpochInRequest,
            int tieringEpochInCoordinator,
            long tableId) {
        verifyError(
                pbHeartbeatRespForTable.getError(),
                Errors.FENCED_TIERING_EPOCH_EXCEPTION.code(),
                String.format(
                        "The tiering epoch %d is not match current epoch %d in coordinator for table %d.",
                        tieringEpochInRequest, tieringEpochInCoordinator, tableId));
    }

    private PbLakeTieringTableInfo waitGetLakeTieringTableInfo() {
        return waitValue(
                () -> {
                    LakeTieringHeartbeatResponse response =
                            coordinatorGateway
                                    .lakeTieringHeartbeat(makeRequestTableTieringHeartbeat())
                                    .get();
                    if (response.hasTieringTable()) {
                        return Optional.of(response.getTieringTable());
                    } else {
                        return Optional.empty();
                    }
                },
                Duration.ofMinutes(1),
                "Fail to wait to get table info to be tiered.");
    }

    private void verifyError(
            ErrorResponse errorResponse, int errorCode, String expectedErrorMessage) {
        assertThat(errorResponse.getErrorCode()).isEqualTo(errorCode);
        assertThat(errorResponse.getErrorMessage()).isEqualTo(expectedErrorMessage);
    }

    private LakeTieringHeartbeatRequest makeTieringHeartbeatRequest(
            long[] tieringTables, long[] finishTieringTables, long[] failedTieringTables) {
        return makeTieringHeartbeatRequest(
                tieringTables,
                finishTieringTables,
                failedTieringTables,
                INITIAL_COORDINATOR_EPOCH,
                1);
    }

    private LakeTieringHeartbeatRequest makeTieringHeartbeatRequest(
            long[] tieringTables,
            long[] finishTieringTables,
            long[] failedTieringTables,
            int coordinatorEpoch,
            int tieringEpoch) {
        LakeTieringHeartbeatRequest heartbeatRequest = new LakeTieringHeartbeatRequest();
        for (long tableId : tieringTables) {
            heartbeatRequest
                    .addTieringTable()
                    .setCoordinatorEpoch(coordinatorEpoch)
                    .setTieringEpoch(tieringEpoch)
                    .setTableId(tableId);
        }

        for (long tableId : finishTieringTables) {
            heartbeatRequest
                    .addFinishedTable()
                    .setTieringEpoch(tieringEpoch)
                    .setCoordinatorEpoch(coordinatorEpoch)
                    .setTableId(tableId);
        }

        for (long tableId : failedTieringTables) {
            heartbeatRequest
                    .addFailedTable()
                    .setCoordinatorEpoch(coordinatorEpoch)
                    .setTieringEpoch(tieringEpoch)
                    .setTableId(tableId);
        }
        return heartbeatRequest;
    }

    private void verifyLakeTieringTableInfo(
            PbLakeTieringTableInfo pbLakeTieringTableInfo, long expectTableId) {
        verifyLakeTieringTableInfo(pbLakeTieringTableInfo, expectTableId, 1);
    }

    private void verifyLakeTieringTableInfo(
            PbLakeTieringTableInfo pbLakeTieringTableInfo,
            long expectTableId,
            long expectTieringEpoch) {
        assertThat(pbLakeTieringTableInfo.getTableId()).isEqualTo(expectTableId);
        assertThat(pbLakeTieringTableInfo.getTieringEpoch()).isEqualTo(expectTieringEpoch);
        PbTablePath pbTablePath = pbLakeTieringTableInfo.getTablePath();
        assertThat(pbTablePath.getDatabaseName()).isEqualTo("fluss");
        assertThat(pbTablePath.getTableName()).isEqualTo("test_lake_table_" + expectTableId);
    }

    private void createLakeTables(int tableCount) throws Exception {
        AdminGateway adminGateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        for (int i = 0; i < tableCount; i++) {
            TableDescriptor tableDescriptor =
                    TableDescriptor.builder()
                            .schema(Schema.newBuilder().column("f1", DataTypes.INT()).build())
                            .property("table.datalake.enabled", "true")
                            .property(
                                    ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(100))
                            .build();
            TablePath tablePath = TablePath.of("fluss", "test_lake_table_" + i);
            // create the table
            adminGateway
                    .createTable(newCreateTableRequest(tablePath, tableDescriptor, false))
                    .get();
        }
    }
}
