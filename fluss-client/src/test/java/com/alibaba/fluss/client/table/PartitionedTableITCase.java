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

package com.alibaba.fluss.client.table;

import com.alibaba.fluss.client.admin.ClientToServerITCaseBase;
import com.alibaba.fluss.client.lookup.Lookuper;
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.exception.PartitionNotExistException;
import com.alibaba.fluss.exception.TooManyPartitionsException;
import com.alibaba.fluss.metadata.PartitionInfo;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static com.alibaba.fluss.testutils.InternalRowAssert.assertThatRow;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.retry;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * IT case for Fluss partitioned table.
 *
 * <p>If you want to add ITCase for auto partitioned table in client, please adding to class {@link
 * AutoPartitionedTableITCase}.
 */
class PartitionedTableITCase extends ClientToServerITCaseBase {

    @Test
    void testPartitionedPrimaryKeyTable() throws Exception {
        TablePath tablePath = TablePath.of("test_db_1", "test_static_partitioned_pk_table_1");
        Schema schema = createPartitionedTable(tablePath, true);

        List<PartitionInfo> partitionInfos = admin.listPartitionInfos(tablePath).get();
        assertThat(partitionInfos.isEmpty()).isTrue();

        // add three partitions.
        for (int i = 0; i < 3; i++) {
            admin.createPartition(tablePath, newPartitionSpec("c", "c" + i), false).get();
        }
        partitionInfos = admin.listPartitionInfos(tablePath).get();
        assertThat(partitionInfos.size()).isEqualTo(3);

        Table table = conn.getTable(tablePath);
        UpsertWriter upsertWriter = table.newUpsert().createWriter();
        int recordsPerPartition = 5;
        // now, put some data to the partitions
        Map<Long, List<InternalRow>> expectPutRows = new HashMap<>();
        for (PartitionInfo partitionInfo : partitionInfos) {
            String partitionName = partitionInfo.getPartitionName();
            long partitionId = partitionInfo.getPartitionId();
            for (int j = 0; j < recordsPerPartition; j++) {
                InternalRow row = row(j, "a" + j, partitionName);
                upsertWriter.upsert(row);
                expectPutRows.computeIfAbsent(partitionId, k -> new ArrayList<>()).add(row);
            }
        }

        upsertWriter.flush();

        Lookuper lookuper = table.newLookup().createLookuper();
        // now, let's lookup the written data by look up.
        for (PartitionInfo partitionInfo : partitionInfos) {
            String partitionName = partitionInfo.getPartitionName();
            for (int j = 0; j < recordsPerPartition; j++) {
                InternalRow actualRow = row(j, "a" + j, partitionName);
                InternalRow lookupRow =
                        lookuper.lookup(row(j, partitionName)).get().getSingletonRow();
                assertThatRow(lookupRow).withSchema(schema.getRowType()).isEqualTo(actualRow);
            }
        }

        // then, let's scan and check the cdc log
        verifyPartitionLogs(table, schema.getRowType(), expectPutRows);
    }

    @Test
    void testPartitionedLogTable() throws Exception {
        TablePath tablePath = TablePath.of("test_db_1", "test_static_partitioned_log_table_1");
        Schema schema = createPartitionedTable(tablePath, false);

        List<PartitionInfo> partitionInfos = admin.listPartitionInfos(tablePath).get();
        assertThat(partitionInfos.isEmpty()).isTrue();

        // add three partitions.
        for (int i = 0; i < 3; i++) {
            admin.createPartition(tablePath, newPartitionSpec("c", "c" + i), false).get();
        }
        partitionInfos = admin.listPartitionInfos(tablePath).get();
        assertThat(partitionInfos.size()).isEqualTo(3);

        Table table = conn.getTable(tablePath);
        AppendWriter appendWriter = table.newAppend().createWriter();
        int recordsPerPartition = 5;
        Map<Long, List<InternalRow>> expectPartitionAppendRows = new HashMap<>();
        for (PartitionInfo partitionInfo : partitionInfos) {
            String partitionName = partitionInfo.getPartitionName();
            long partitionId = partitionInfo.getPartitionId();
            for (int j = 0; j < recordsPerPartition; j++) {
                InternalRow row = row(j, "a" + j, partitionName);
                appendWriter.append(row);
                expectPartitionAppendRows
                        .computeIfAbsent(partitionId, k -> new ArrayList<>())
                        .add(row);
            }
        }
        appendWriter.flush();

        // then, let's verify the logs
        verifyPartitionLogs(table, schema.getRowType(), expectPartitionAppendRows);
    }

    @Test
    void testWriteToNonExistsPartitionWhenDisabledDynamicPartition() throws Exception {
        clientConf.set(ConfigOptions.CLIENT_WRITER_DYNAMIC_CREATE_PARTITION_ENABLED, false);
        createPartitionedTable(DATA1_TABLE_PATH_PK, true);
        Table table = conn.getTable(DATA1_TABLE_PATH_PK);

        // test write to not exist partition when enable dynamic create partition
        UpsertWriter upsertWriter = table.newUpsert().createWriter();
        GenericRow row = row(1, "a", "notExistPartition");
        assertThatThrownBy(() -> upsertWriter.upsert(row).get())
                .cause()
                .isInstanceOf(PartitionNotExistException.class)
                .hasMessageContaining(
                        "Table partition '%s' does not exist.",
                        PhysicalTablePath.of(DATA1_TABLE_PATH_PK, "notExistPartition"));
    }

    @Test
    void testWriteToNonExistsPartitionWhenEnabledDynamicPartition() throws Exception {
        Schema schema = createPartitionedTable(DATA1_TABLE_PATH_PK, false);
        Table table = conn.getTable(DATA1_TABLE_PATH_PK);
        AppendWriter appendWriter = table.newAppend().createWriter();
        int partitionSize = 5;

        // first try to add records add wait partition created.
        Map<String, List<InternalRow>> expectPartitionNameAndAppendRows = new HashMap<>();
        for (int i = 0; i < partitionSize; i++) {
            String partitionName = String.valueOf(i);
            InternalRow row = row(i, "a" + i, partitionName);
            appendWriter.append(row);
            expectPartitionNameAndAppendRows
                    .computeIfAbsent(partitionName, k -> new ArrayList<>())
                    .add(row);
        }
        appendWriter.flush();

        List<PartitionInfo> partitionInfoList =
                waitValue(
                        () -> {
                            List<PartitionInfo> partitionInfos =
                                    admin.listPartitionInfos(DATA1_TABLE_PATH_PK).get();
                            if (partitionInfos.size() == partitionSize) {
                                return Optional.of(partitionInfos);
                            } else {
                                return Optional.empty();
                            }
                        },
                        Duration.ofMinutes(1),
                        "Fail to wait for the partition created.");
        Map<Long, List<InternalRow>> expectPartitionIdAndAppendRows = new HashMap<>();
        for (PartitionInfo partitionInfo : partitionInfoList) {
            expectPartitionIdAndAppendRows.put(
                    partitionInfo.getPartitionId(),
                    expectPartitionNameAndAppendRows.get(partitionInfo.getPartitionName()));
        }

        // then, let's verify the logs
        verifyPartitionLogs(table, schema.getRowType(), expectPartitionIdAndAppendRows);
    }

    @Test
    void testCreatePartitionExceedMaxPartitionNumber() throws Exception {
        // test make partition number exceed max partition number 10 (set in
        // ClientToServerITCaseBase).
        createPartitionedTable(DATA1_TABLE_PATH_PK, true);
        Table table = conn.getTable(DATA1_TABLE_PATH_PK);

        // test write to not exist partition when enable dynamic create partition
        UpsertWriter upsertWriter = table.newUpsert().createWriter();
        for (int i = 0; i < 10; i++) {
            String partitionName = String.valueOf(i);
            InternalRow row = row(i, "a" + i, partitionName);
            upsertWriter.upsert(row).get();
        }

        // add one row will not throw TooManyPartitionsException immediately.
        upsertWriter.upsert(row(10, "a" + 10, "10"));

        // add another rows will throw TooManyPartitionsException final.
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThatThrownBy(() -> upsertWriter.upsert(row(10, "a" + 10, "10")).get())
                                .rootCause()
                                .isInstanceOf(TooManyPartitionsException.class)
                                .hasMessageContaining(
                                        "Exceed the maximum number of partitions for table "
                                                + "test_db_1.test_pk_table_1, only allow 10 partitions."));
    }

    private Schema createPartitionedTable(TablePath tablePath, boolean isPrimaryTable)
            throws Exception {
        Schema.Builder schemaBuilder =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .withComment("a is first column")
                        .column("b", DataTypes.STRING())
                        .withComment("b is second column")
                        .column("c", DataTypes.STRING())
                        .withComment("c is third column");

        if (isPrimaryTable) {
            schemaBuilder.primaryKey("a", "c");
        }

        Schema schema = schemaBuilder.build();

        TableDescriptor partitionTableDescriptor =
                TableDescriptor.builder().schema(schema).partitionedBy("c").build();
        createTable(tablePath, partitionTableDescriptor, false);
        return schema;
    }
}
