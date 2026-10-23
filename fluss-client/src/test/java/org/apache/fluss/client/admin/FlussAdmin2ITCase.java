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

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.exception.TableNotPartitionedException;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.ListPartitionInfosRequest;
import org.apache.fluss.rpc.messages.ListPartitionInfosResponse;
import org.apache.fluss.rpc.messages.PbTablePath;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static org.apache.fluss.metadata.DataLakeFormat.PAIMON;
import static org.apache.fluss.record.TestData.DATA1_PARTITIONED_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR_PK;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
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

    @Test
    void testListOffsetsByPhysicalTableId() throws Exception {
        TablePath tablePath = TablePath.of("test_db", "test_offsets_by_table_id");
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                        .distributedBy(1)
                        .build();

        long tableId = createTable(tablePath, tableDescriptor, false);
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(tableId);

        List<Integer> buckets = Collections.singletonList(0);

        for (OffsetSpec offsetSpec :
                new OffsetSpec[] {new OffsetSpec.EarliestSpec(), new OffsetSpec.LatestSpec()}) {
            assertThat(admin.listOffsets(tablePath, tableId, null, buckets, offsetSpec).all().get())
                    .isEqualTo(admin.listOffsets(tablePath, buckets, offsetSpec).all().get());
        }

        assertThatThrownBy(
                        () ->
                                admin.listOffsets(
                                        tablePath,
                                        tableId,
                                        123L,
                                        buckets,
                                        new OffsetSpec.LatestSpec()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testListOffsetsByPhysicalPartitionId() throws Exception {
        TablePath tablePath = TablePath.of("test_db", "test_offsets_by_partition_id");
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .column("pt", DataTypes.STRING())
                                        .build())
                        .distributedBy(1)
                        .partitionedBy("pt")
                        .build();

        long tableId = createTable(tablePath, tableDescriptor, false);
        String partitionName = "2026";
        PartitionSpec partitionSpec = newPartitionSpec("pt", partitionName);

        admin.createPartition(tablePath, partitionSpec, false).get();

        List<PartitionInfo> partitionInfos = admin.listPartitionInfos(tablePath).get();
        assertThat(partitionInfos).hasSize(1);

        long originalPartitionId = partitionInfos.get(0).getPartitionId();
        FLUSS_CLUSTER_EXTENSION.waitUntilTablePartitionReady(tableId, originalPartitionId);

        List<Integer> buckets = Collections.singletonList(0);
        OffsetSpec offsetSpec = new OffsetSpec.LatestSpec();

        long originalStartOffset =
                admin.listOffsets(tablePath, tableId, originalPartitionId, buckets, offsetSpec)
                        .all()
                        .get()
                        .get(0);

        appendPartitionRecords(conn, tablePath, partitionName, 2);

        long originalLatestOffset =
                admin.listOffsets(tablePath, tableId, originalPartitionId, buckets, offsetSpec)
                        .all()
                        .get()
                        .get(0);

        assertThat(originalLatestOffset - originalStartOffset).isEqualTo(2);
        assertThat(admin.listOffsets(tablePath, partitionName, buckets, offsetSpec).all().get())
                .containsEntry(0, originalLatestOffset);

        assertThatThrownBy(() -> admin.listOffsets(tablePath, tableId, null, buckets, offsetSpec))
                .isInstanceOf(IllegalArgumentException.class);

        admin.dropPartition(tablePath, partitionSpec, false).get();
        admin.createPartition(tablePath, partitionSpec, false).get();

        List<PartitionInfo> recreatedPartitions = admin.listPartitionInfos(tablePath).get();
        assertThat(recreatedPartitions).hasSize(1);

        long recreatedPartitionId = recreatedPartitions.get(0).getPartitionId();
        assertThat(recreatedPartitionId).isNotEqualTo(originalPartitionId);

        FLUSS_CLUSTER_EXTENSION.waitUntilTablePartitionReady(tableId, recreatedPartitionId);

        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(
                                        FLUSS_CLUSTER_EXTENSION
                                                .getZooKeeperClient()
                                                .getPartitionAssignment(originalPartitionId))
                                .isEmpty());

        try (Connection connection = ConnectionFactory.createConnection(clientConf)) {
            Admin refreshedAdmin = connection.getAdmin();

            long recreatedStartOffset =
                    refreshedAdmin
                            .listOffsets(
                                    tablePath, tableId, recreatedPartitionId, buckets, offsetSpec)
                            .all()
                            .get()
                            .get(0);

            appendPartitionRecords(connection, tablePath, partitionName, 5);

            long recreatedLatestOffset =
                    refreshedAdmin
                            .listOffsets(
                                    tablePath, tableId, recreatedPartitionId, buckets, offsetSpec)
                            .all()
                            .get()
                            .get(0);

            assertThat(recreatedLatestOffset - recreatedStartOffset).isEqualTo(5);

            assertThat(
                            refreshedAdmin
                                    .listOffsets(tablePath, partitionName, buckets, offsetSpec)
                                    .all()
                                    .get())
                    .containsEntry(0, recreatedLatestOffset);

            assertThatThrownBy(
                            () ->
                                    refreshedAdmin
                                            .listOffsets(
                                                    tablePath,
                                                    tableId,
                                                    originalPartitionId,
                                                    buckets,
                                                    offsetSpec)
                                            .all()
                                            .get())
                    .satisfies(
                            throwable -> {
                                Throwable cause = throwable;
                                while ((cause instanceof ExecutionException
                                                || cause instanceof CompletionException)
                                        && cause.getCause() != null) {
                                    cause = cause.getCause();
                                }

                                if (!(cause instanceof PartitionNotExistException)) {
                                    assertThat(cause)
                                            .isInstanceOf(FlussRuntimeException.class)
                                            .hasMessageContaining("Leader not found after retry");
                                }
                            });
        }
    }

    @Test
    void testListOffsetsAfterTableRecreation() throws Exception {
        TablePath tablePath = TablePath.of("test_db", "test_offsets_recreated_table");
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                        .distributedBy(1)
                        .build();

        long originalTableId = createTable(tablePath, tableDescriptor, false);
        admin.dropTable(tablePath, false).get();
        admin.createTable(tablePath, tableDescriptor, false).get();

        TableInfo recreatedTableInfo = admin.getTableInfo(tablePath).get();
        long recreatedTableId = recreatedTableInfo.getTableId();

        assertThat(recreatedTableId).isNotEqualTo(originalTableId);

        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(recreatedTableId);

        List<Integer> buckets = Collections.singletonList(0);
        OffsetSpec offsetSpec = new OffsetSpec.LatestSpec();

        try (Connection connection = ConnectionFactory.createConnection(clientConf)) {
            Admin refreshedAdmin = connection.getAdmin();

            assertThatThrownBy(
                            () ->
                                    refreshedAdmin.listOffsets(
                                            tablePath, originalTableId, null, buckets, offsetSpec))
                    .isInstanceOf(TableNotExistException.class);

            assertThat(
                            refreshedAdmin
                                    .listOffsets(
                                            tablePath, recreatedTableId, null, buckets, offsetSpec)
                                    .all()
                                    .get())
                    .containsKey(0);
        }
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

    private static void appendPartitionRecords(
            Connection connection, TablePath tablePath, String partitionValue, int recordCount)
            throws Exception {
        try (Table table = connection.getTable(tablePath)) {
            AppendWriter writer = table.newAppend().createWriter();
            for (int i = 0; i < recordCount; i++) {
                writer.append(row(i, partitionValue));
            }
            writer.flush();
        }
    }
}
