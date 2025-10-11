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

package org.apache.fluss.client.table;

import org.apache.fluss.client.admin.ClientToServerITCaseBase;
import org.apache.fluss.client.lookup.LookupResult;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for multi-partition prefix lookup functionality. */
class MultiPartitionPrefixLookupITCase extends ClientToServerITCaseBase {

    @Test
    void testMultiPartitionPrefixLookupWithoutPartitionKeys() throws Exception {
        // Create a partitioned table with partition key not in bucket keys
        TablePath tablePath = TablePath.of("test_db_1", "multi_partition_lookup_table");
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column("c", DataTypes.BIGINT())
                        .column("d", DataTypes.STRING())
                        .column("partition_col", DataTypes.STRING())
                        .primaryKey("a", "b", "c", "partition_col")
                        .build();

        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(3, "a", "b")
                        .partitionedBy("partition_col")
                        .build();

        createTable(tablePath, descriptor, false);

        // Create partitions manually
        admin.createPartition(tablePath, newPartitionSpec("partition_col", "p1"), false).get();
        admin.createPartition(tablePath, newPartitionSpec("partition_col", "p2"), false).get();

        Table table = conn.getTable(tablePath);

        // Insert data into different partitions
        UpsertWriter upsertWriter = table.newUpsert().createWriter();

        // Insert data to partition p1
        upsertWriter.upsert(row(1, "a", 1L, "value1_p1", "p1"));
        upsertWriter.upsert(row(1, "a", 2L, "value2_p1", "p1"));

        // Insert data to partition p2
        upsertWriter.upsert(row(1, "a", 1L, "value1_p2", "p2"));
        upsertWriter.upsert(row(1, "a", 3L, "value3_p2", "p2"));

        upsertWriter.flush();

        // Test prefix lookup without partition column
        // This should search across all partitions.
        Lookuper prefixLookuper = table.newLookup().lookupBy("a", "b").createLookuper();

        CompletableFuture<LookupResult> result = prefixLookuper.lookup(row(1, "a"));
        LookupResult lookupResult = result.get();
        assertThat(lookupResult).isNotNull();
        List<InternalRow> rowList = lookupResult.getRowList();

        // Should find 4 rows across all partitions (2 from p1, 2 from p2)
        assertThat(rowList.size()).isEqualTo(4);

        // Verify we got data from multiple partitions
        boolean foundP1 = false, foundP2 = false;
        for (InternalRow row : rowList) {
            String partitionValue = row.getString(4).toString();
            if ("p1".equals(partitionValue)) {
                foundP1 = true;
            }
            if ("p2".equals(partitionValue)) {
                foundP2 = true;
            }
        }
        assertThat(foundP1).isTrue();
        assertThat(foundP2).isTrue();
    }

    @Test
    void testMultiPartitionPrefixLookupEmptyResult() throws Exception {
        // Test lookup when no matching data exists
        TablePath tablePath = TablePath.of("test_db_1", "empty_result_lookup_table");
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column("c", DataTypes.BIGINT())
                        .column("d", DataTypes.STRING())
                        .column("partition_col", DataTypes.STRING())
                        .primaryKey("a", "b", "c", "partition_col")
                        .build();

        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(3, "a", "b")
                        .partitionedBy("partition_col")
                        .build();

        createTable(tablePath, descriptor, false);
        admin.createPartition(tablePath, newPartitionSpec("partition_col", "p1"), false).get();

        Table table = conn.getTable(tablePath);

        // Test prefix lookup without inserting any data
        Lookuper prefixLookuper = table.newLookup().lookupBy("a", "b").createLookuper();

        CompletableFuture<LookupResult> result = prefixLookuper.lookup(row(1, "a"));
        LookupResult lookupResult = result.get();
        assertThat(lookupResult).isNotNull();
        List<InternalRow> rowList = lookupResult.getRowList();

        // Should return empty result
        assertThat(rowList).isEmpty();
    }
}
