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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.PredicateBuilder;
import org.apache.fluss.types.IntType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.StringType;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toFilterExpression;
import static org.apache.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

/** Test class for {@link IcebergSplitPlanner}. */
class IcebergSplitPlannerTest extends IcebergSourceTestBase {

    @Test
    void testImplicitTimePartitionPlan() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "implicit_time_partition_table");
        Schema schema =
                new Schema(
                        optional(1, "c1", Types.IntegerType.get()),
                        optional(2, "event_time", Types.TimestampType.withoutZone()),
                        required(3, BUCKET_COLUMN_NAME, Types.IntegerType.get()),
                        required(4, OFFSET_COLUMN_NAME, Types.LongType.get()),
                        required(5, TIMESTAMP_COLUMN_NAME, Types.TimestampType.withZone()));
        PartitionSpec partitionSpec =
                PartitionSpec.builderFor(schema)
                        .day("event_time", "event_day")
                        .identity(BUCKET_COLUMN_NAME)
                        .build();
        createTable(tablePath, schema, partitionSpec);

        Table table = getTable(tablePath);
        GenericRecord record =
                createIcebergRecord(
                        schema,
                        12,
                        LocalDateTime.of(2026, 6, 4, 15, 30),
                        0,
                        100L,
                        OffsetDateTime.now(ZoneOffset.UTC));
        writeRecord(table, Collections.singletonList(record), "20260604", 0);

        table.refresh();
        LakeSource<IcebergSplit> lakeSource = lakeStorage.createLakeSource(tablePath);
        List<IcebergSplit> icebergSplits =
                lakeSource.createPlanner(table.currentSnapshot()::snapshotId).plan();

        assertThat(icebergSplits).hasSize(1);
        assertThat(icebergSplits.get(0).bucket()).isEqualTo(-1);
        assertThat(icebergSplits.get(0).partition()).containsExactly("20260604");
        try (CloseableIterable<?> tasks =
                table.newScan().filter(toFilterExpression(table, "20260604", 0)).planFiles()) {
            assertThat(tasks).hasSize(1);
        }
    }

    @Test
    void testMixedPhysicalAndImplicitPartitionPlan() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "mixed_implicit_partition_table");
        Schema schema =
                new Schema(
                        optional(1, "c1", Types.IntegerType.get()),
                        required(2, "region", Types.StringType.get()),
                        required(3, "event_time", Types.TimestampType.withoutZone()),
                        required(4, BUCKET_COLUMN_NAME, Types.IntegerType.get()),
                        required(5, OFFSET_COLUMN_NAME, Types.LongType.get()),
                        required(6, TIMESTAMP_COLUMN_NAME, Types.TimestampType.withZone()));
        PartitionSpec partitionSpec =
                PartitionSpec.builderFor(schema)
                        .identity("region")
                        .day("event_time", "__fluss_implicit_partition_1")
                        .identity(BUCKET_COLUMN_NAME)
                        .build();
        createTable(tablePath, schema, partitionSpec);

        Table table = getTable(tablePath);
        GenericRecord record =
                createIcebergRecord(
                        schema,
                        12,
                        "us",
                        LocalDateTime.of(2026, 6, 4, 15, 30),
                        0,
                        100L,
                        OffsetDateTime.now(ZoneOffset.UTC));
        writeRecord(table, Collections.singletonList(record), "us$20260604", 0);

        table.refresh();
        LakeSource<IcebergSplit> lakeSource = lakeStorage.createLakeSource(tablePath);
        List<IcebergSplit> splits =
                lakeSource.createPlanner(table.currentSnapshot()::snapshotId).plan();

        assertThat(splits).hasSize(1);
        assertThat(splits.get(0).partition()).containsExactly("us", "20260604");
        try (CloseableIterable<?> tasks =
                table.newScan().filter(toFilterExpression(table, "us$20260604", 0)).planFiles()) {
            assertThat(tasks).hasSize(1);
        }
    }

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
        try (CloseableIterable<?> tasks =
                table.newScan()
                        .filter(toFilterExpression(table, isPartitioned ? "a" : null, 0))
                        .planFiles()) {
            assertThat(tasks).hasSize(1);
        }

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

    @Test
    void testOnlyIncludesFilterColumnStats() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "test_filter_column_stats");
        Schema schema =
                new Schema(
                        optional(1, "c1", Types.IntegerType.get()),
                        optional(2, "c2", Types.StringType.get()),
                        optional(3, "c3", Types.StringType.get()));
        PartitionSpec partitionSpec = PartitionSpec.builderFor(schema).bucket("c1", 2).build();
        createTable(tablePath, schema, partitionSpec);

        Table table = getTable(tablePath);
        GenericRecord record = createIcebergRecord(schema, 12, "a", "A");
        writeRecord(table, Collections.singletonList(record), null, 0);
        table.refresh();
        Snapshot snapshot = table.currentSnapshot();

        PredicateBuilder predicateBuilder =
                new PredicateBuilder(RowType.of(new IntType(), new StringType(), new StringType()));
        LakeSource<IcebergSplit> lakeSource = lakeStorage.createLakeSource(tablePath);
        lakeSource.withFilters(Collections.singletonList(predicateBuilder.greaterOrEqual(0, 10)));
        List<IcebergSplit> icebergSplits = lakeSource.createPlanner(snapshot::snapshotId).plan();

        assertThat(icebergSplits).hasSize(1);
        assertThat(icebergSplits.get(0).fileScanTask().file().lowerBounds().keySet())
                .containsOnly(1);
    }

    @Test
    void testReferencedColumns() {
        // Unbound predicates expose the referenced column through their named reference.
        assertThat(referencedColumns(Expressions.greaterThanOrEqual("c1", 10))).containsOnly("c1");

        // Compound expressions merge referenced columns and deduplicate repeated columns.
        Expression compoundExpression =
                Expressions.and(
                        Expressions.or(
                                Expressions.greaterThanOrEqual("c1", 10),
                                Expressions.equal("c2", "a")),
                        Expressions.not(Expressions.lessThan("c1", 20)));
        assertThat(referencedColumns(compoundExpression)).containsOnly("c1", "c2");

        // Bound predicates expose the resolved field through their bound reference.
        Schema schema =
                new Schema(
                        optional(1, "c1", Types.IntegerType.get()),
                        optional(2, "c2", Types.StringType.get()));
        Expression boundExpression =
                Expressions.greaterThanOrEqual("c2", "a").bind(schema.asStruct(), true);
        assertThat(referencedColumns(boundExpression)).containsOnly("c2");
    }

    private Set<String> referencedColumns(Expression expression) {
        return new IcebergSplitPlanner(
                        new Configuration(),
                        TablePath.of(DEFAULT_DB, "test_referenced_columns"),
                        0,
                        null)
                .referencedColumns(expression);
    }
}
