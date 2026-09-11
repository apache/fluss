/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.fluss.client.admin;

import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.TableNotPartitionedException;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.ListPartitionInfosRequest;
import org.apache.fluss.rpc.messages.ListPartitionInfosResponse;
import org.apache.fluss.rpc.messages.PbTablePath;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.apache.fluss.metadata.DataLakeFormat.PAIMON;
import static org.apache.fluss.record.TestData.DATA1_PARTITIONED_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR_PK;
import static org.apache.fluss.utils.PartitionUtils.HISTORICAL_PARTITION_VALUE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Additional integration tests for {@link FlussAdmin}.
 *
 * <p>This class contains additional tests because {@link FlussAdminITCase} is close to Checkstyle's
 * 3000-line limit per file. Add new FlussAdmin integration tests here.
 */
class FlussAdmin2ITCase extends ClientToServerITCaseBase {

    @Test
    void testListPartitionInfos() throws Exception {
        String dbName = "test_db";
        TablePath nonPartitionedTablePath = TablePath.of(dbName, "test_non_partitioned_table");
        createTable(nonPartitionedTablePath, DATA1_TABLE_DESCRIPTOR_PK, false);
        assertThatThrownBy(() -> admin.listPartitionInfos(nonPartitionedTablePath).get())
                .cause()
                .isInstanceOf(TableNotPartitionedException.class)
                .hasMessage("Table '%s' is not a partitioned table.", nonPartitionedTablePath);

        TableDescriptor partitionedTable =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.STRING())
                                        .column("name", DataTypes.STRING())
                                        .column("pt", DataTypes.STRING())
                                        .primaryKey("id", "pt")
                                        .build())
                        .distributedBy(3, "id")
                        .partitionedBy("pt")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_KEY, "pt")
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                AutoPartitionTimeUnit.YEAR)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .property(ConfigOptions.TABLE_DATALAKE_FORMAT, PAIMON)
                        .property(ConfigOptions.TABLE_DATALAKE_HISTORICAL_PARTITION_ENABLED, true)
                        .build();
        TablePath partitionedTablePath = TablePath.of(dbName, "test_partitioned_table");
        admin.createTable(partitionedTablePath, partitionedTable, false).get();
        Map<String, Long> partitionIdByNames =
                FLUSS_CLUSTER_EXTENSION.waitUntilPartitionAllReady(
                        partitionedTablePath,
                        ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE.defaultValue() + 1);
        assertThat(partitionIdByNames).containsKey(HISTORICAL_PARTITION_VALUE);

        List<PartitionInfo> partitionInfos = admin.listPartitionInfos(partitionedTablePath).get();
        assertThat(partitionInfos)
                .hasSize(partitionIdByNames.size() - 1)
                .extracting(PartitionInfo::getPartitionName)
                .doesNotContain(HISTORICAL_PARTITION_VALUE);

        List<PartitionInfo> allPartitionInfos =
                admin.listPartitionInfos(partitionedTablePath, true).get();
        assertThat(allPartitionInfos)
                .hasSize(partitionIdByNames.size())
                .extracting(PartitionInfo::getPartitionName)
                .contains(HISTORICAL_PARTITION_VALUE);
        PartitionInfo historicalPartitionInfo =
                allPartitionInfos.stream()
                        .filter(
                                partitionInfo ->
                                        HISTORICAL_PARTITION_VALUE.equals(
                                                partitionInfo.getPartitionName()))
                        .findFirst()
                        .get();
        assertThat(historicalPartitionInfo.getPartitionId())
                .isEqualTo(partitionIdByNames.get(HISTORICAL_PARTITION_VALUE));
        assertThat(historicalPartitionInfo.getBucketCount()).isEqualTo(3);

        FlussAdmin flussAdmin = (FlussAdmin) admin;
        ListPartitionInfosRequest legacyRequest = requestFor(partitionedTablePath, false);
        ListPartitionInfosResponse legacyResponse =
                flussAdmin.getAdminReadOnlyGateway().listPartitionInfos(legacyRequest).get();
        assertThat(legacyResponse.hasSystemPartitionsIncluded()).isFalse();
        List<PartitionInfo> legacyCompatibleInfos =
                flussAdmin
                        .handleListPartitionInfosResponse(
                                partitionedTablePath, true, legacyResponse)
                        .get();
        assertThat(legacyCompatibleInfos)
                .hasSize(partitionIdByNames.size())
                .extracting(PartitionInfo::getPartitionName)
                .contains(HISTORICAL_PARTITION_VALUE);
        legacyResponse.setSystemPartitionsIncluded(false);
        assertThat(
                        flussAdmin
                                .handleListPartitionInfosResponse(
                                        partitionedTablePath, true, legacyResponse)
                                .get())
                .extracting(PartitionInfo::getPartitionName)
                .contains(HISTORICAL_PARTITION_VALUE);

        TablePath noSystemPartitionTablePath =
                TablePath.of(dbName, "test_partitioned_table_without_system_partition");
        admin.createTable(noSystemPartitionTablePath, DATA1_PARTITIONED_TABLE_DESCRIPTOR, false)
                .get();
        ListPartitionInfosResponse responseWithoutSystemPartitions =
                flussAdmin
                        .getAdminReadOnlyGateway()
                        .listPartitionInfos(requestFor(noSystemPartitionTablePath, true))
                        .get();
        assertThat(responseWithoutSystemPartitions.hasSystemPartitionsIncluded()).isTrue();
        assertThat(responseWithoutSystemPartitions.isSystemPartitionsIncluded()).isTrue();

        ListPartitionInfosResponse legacyResponseWithoutSystemPartitions =
                flussAdmin
                        .getAdminReadOnlyGateway()
                        .listPartitionInfos(requestFor(noSystemPartitionTablePath, false))
                        .get();
        assertThat(
                        flussAdmin
                                .handleListPartitionInfosResponse(
                                        noSystemPartitionTablePath,
                                        true,
                                        legacyResponseWithoutSystemPartitions)
                                .get())
                .extracting(PartitionInfo::getPartitionName)
                .doesNotContain(HISTORICAL_PARTITION_VALUE);
    }

    private static ListPartitionInfosRequest requestFor(
            TablePath tablePath, boolean includeSystemPartitions) {
        ListPartitionInfosRequest request =
                new ListPartitionInfosRequest()
                        .setTablePath(
                                new PbTablePath()
                                        .setDatabaseName(tablePath.getDatabaseName())
                                        .setTableName(tablePath.getTableName()));
        if (includeSystemPartitions) {
            request.setIncludeSystemPartitions(true);
        }
        return request;
    }
}
