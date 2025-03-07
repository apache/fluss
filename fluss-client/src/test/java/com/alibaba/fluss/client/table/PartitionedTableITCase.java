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
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static com.alibaba.fluss.testutils.InternalRowAssert.assertThatRow;
import static org.assertj.core.api.Assertions.assertThat;

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
