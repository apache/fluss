/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.client.table;

import com.alibaba.fluss.client.admin.ClientToServerITCaseBase;
import com.alibaba.fluss.client.lookup.Lookuper;
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.metadata.PartitionInfo;
import com.alibaba.fluss.metadata.PartitionSpec;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.utils.TypeUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.fluss.testutils.DataTestUtils.compactedRow;
import static com.alibaba.fluss.testutils.DataTestUtils.keyRow;
import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** IT case for Fluss static partitioned table. */
class StaticPartitionedTableITCase extends ClientToServerITCaseBase {

    @Test
    void testPartitionedPrimaryKeyTable() throws Exception {
        TablePath tablePath = TablePath.of("test_db_1", "test_static_partitioned_pk_table_1");
        Schema schema = createPartitionedTable(tablePath, true);

        List<PartitionInfo> partitionInfos = admin.listPartitionInfos(tablePath).get();
        assertThat(partitionInfos.isEmpty()).isTrue();

        // add three partitions.
        for (int i = 0; i < 3; i++) {
            admin.addPartition(
                            tablePath,
                            new PartitionSpec(Collections.singletonMap("c", "c" + i)),
                            false)
                    .get();
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
                InternalRow row =
                        compactedRow(schema.getRowType(), new Object[] {j, "a" + j, partitionName});
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
                InternalRow actualRow =
                        compactedRow(schema.getRowType(), new Object[] {j, "a" + j, partitionName});
                InternalRow lookupRow =
                        lookuper.lookup(keyRow(schema, new Object[] {j, null, partitionName}))
                                .get()
                                .getSingletonRow();
                assertThat(lookupRow).isEqualTo(actualRow);
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
            admin.addPartition(
                            tablePath,
                            new PartitionSpec(Collections.singletonMap("c", "c" + i)),
                            false)
                    .get();
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
                InternalRow row =
                        row(schema.getRowType(), new Object[] {j, "a" + j, partitionName});
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

    @ParameterizedTest
    @MethodSource("keyTypeAndSpec")
    void testPartitionKeyType(DataType dataType, Object keySpec) throws Exception {
        TablePath tablePath =
                TablePath.of(
                        "test_db_1",
                        "test_static_partitioned_log_table_" + dataType.getTypeRoot().name());
        Schema.Builder schemaBuilder =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column("c", dataType);
        Schema schema = schemaBuilder.build();
        TableDescriptor partitionTableDescriptor =
                TableDescriptor.builder().schema(schema).partitionedBy("c").build();
        createTable(tablePath, partitionTableDescriptor, false);

        List<PartitionInfo> partitionInfos = admin.listPartitionInfos(tablePath).get();
        assertThat(partitionInfos.isEmpty()).isTrue();

        // add one partitions.
        admin.addPartition(
                        tablePath,
                        new PartitionSpec(Collections.singletonMap("c", keySpec.toString())),
                        false)
                .get();
        partitionInfos = admin.listPartitionInfos(tablePath).get();
        assertThat(partitionInfos.size()).isEqualTo(1);

        Table table = conn.getTable(tablePath);
        AppendWriter appendWriter = table.newAppend().createWriter();
        int recordSize = 5;
        List<InternalRow> expectResult = new ArrayList<>();
        for (int i = 0; i < recordSize; i++) {
            InternalRow row = row(schema.getRowType(), new Object[] {i, "a" + i, keySpec});
            appendWriter.append(row);
            expectResult.add(row);
        }
        appendWriter.flush();

        // then, let's verify the logs
        verifyPartitionLogs(
                table,
                schema.getRowType(),
                Collections.singletonMap(partitionInfos.get(0).getPartitionId(), expectResult));
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

    private static Collection<Arguments> keyTypeAndSpec() {
        return Arrays.asList(
                Arguments.arguments(DataTypes.INT(), 1),
                Arguments.arguments(DataTypes.STRING(), "c0"),
                Arguments.arguments(DataTypes.BOOLEAN(), true),
                Arguments.arguments(
                        DataTypes.DATE(), TypeUtils.castFromString("2023-10-25", DataTypes.DATE())),
                Arguments.arguments(
                        DataTypes.TIME(),
                        TypeUtils.castFromString("09:00:00.0", DataTypes.TIME())));
    }
}
