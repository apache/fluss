/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.iceberg.source;

import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.metadata.TablePath;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

/** Test class for {@link IcebergSplitPlanner}. */
class IcebergSplitPlannerTest extends IcebergSourceTestBase {
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testLogTablePlan(boolean isPartitioned) throws Exception {
        // prepare iceberg log table
        TablePath tablePath =
                TablePath.of(
                        DEFAULT_DB, "log_" + (isPartitioned ? "partitioned_" : "") + DEFAULT_TABLE);
        Schema schema =
                new Schema(
                        optional(1, "c1", Types.IntegerType.get()),
                        optional(2, "c2", Types.StringType.get()),
                        optional(3, "c3", Types.StringType.get()));
        PartitionSpec partitionSpec =
                isPartitioned
                        ? PartitionSpec.builderFor(schema).identity("c2").bucket("c1", 2).build()
                        : PartitionSpec.builderFor(schema).bucket("c1", 2).build();
        createTable(tablePath, schema, partitionSpec);

        // write data
        Table table = getTable(tablePath);
        GenericRecord record1 = createIcebergRecord(schema, 12, "a", "A");
        GenericRecord record2 = createIcebergRecord(schema, 13, "b", "B");

        writeRecord(table, Collections.singletonList(record1), isPartitioned ? "a" : null, 0);
        writeRecord(table, Collections.singletonList(record2), isPartitioned ? "b" : null, 1);

        // refresh table
        table.refresh();
        Snapshot snapshot = table.currentSnapshot();

        LakeSource<IcebergSplit> lakeSource = lakeStorage.createLakeSource(tablePath);
        List<IcebergSplit> icebergSplits = lakeSource.createPlanner(snapshot::snapshotId).plan();
        assertThat(icebergSplits.size()).isEqualTo(2);
        // Log table with bucket-aware
        assertThat(icebergSplits.stream().map(IcebergSplit::bucket))
                .containsExactlyInAnyOrder(0, 1);
        if (isPartitioned) {
            assertThat(icebergSplits.stream().map(IcebergSplit::partition))
                    .containsExactlyInAnyOrder(
                            Collections.singletonList("a"), Collections.singletonList("b"));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testBucketUnawareLogTablePlan(boolean isPartitioned) throws Exception {
        // prepare iceberg table which corresponding to a Fluss bucket unaware table log table
        TablePath tablePath =
                TablePath.of(
                        DEFAULT_DB,
                        "log_"
                                + (isPartitioned ? "partitioned_" : "")
                                + DEFAULT_TABLE
                                + "_fluss_bucket");
        Schema schema =
                new Schema(
                        optional(1, "c1", Types.IntegerType.get()),
                        optional(2, "c2", Types.StringType.get()),
                        optional(3, "c3", Types.StringType.get()),
                        // System columns
                        required(14, BUCKET_COLUMN_NAME, Types.IntegerType.get()),
                        required(15, OFFSET_COLUMN_NAME, Types.LongType.get()),
                        required(16, TIMESTAMP_COLUMN_NAME, Types.TimestampType.withZone()));
        PartitionSpec partitionSpec =
                isPartitioned
                        ? PartitionSpec.builderFor(schema)
                                .identity("c2")
                                .identity(BUCKET_COLUMN_NAME)
                                .build()
                        : PartitionSpec.builderFor(schema).identity(BUCKET_COLUMN_NAME).build();
        createTable(tablePath, schema, partitionSpec);

        // write data
        Table table = getTable(tablePath);
        GenericRecord record1 =
                createIcebergRecord(
                        schema, 12, "a", "A", 0, 100L, OffsetDateTime.now(ZoneOffset.UTC));
        GenericRecord record2 =
                createIcebergRecord(
                        schema, 13, "b", "B", 1, 200L, OffsetDateTime.now(ZoneOffset.UTC));

        writeRecord(table, Collections.singletonList(record1), isPartitioned ? "a" : null, 0);
        writeRecord(table, Collections.singletonList(record2), isPartitioned ? "b" : null, 1);

        // refresh table
        table.refresh();
        Snapshot snapshot = table.currentSnapshot();

        LakeSource<IcebergSplit> lakeSource = lakeStorage.createLakeSource(tablePath);
        List<IcebergSplit> icebergSplits = lakeSource.createPlanner(snapshot::snapshotId).plan();
        assertThat(icebergSplits.size()).isEqualTo(2);
        // Log table is not bucket-aware
        assertThat(icebergSplits.get(0).bucket()).isEqualTo(-1);
        assertThat(icebergSplits.get(1).bucket()).isEqualTo(-1);
        if (isPartitioned) {
            assertThat(icebergSplits.stream().map(IcebergSplit::partition))
                    .containsExactlyInAnyOrder(
                            Collections.singletonList("a"), Collections.singletonList("b"));
        }
    }
}
