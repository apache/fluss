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
import org.apache.fluss.client.table.scanner.Scan;
import org.apache.fluss.client.table.scanner.batch.BatchScanUtils;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.writer.UpsertResult;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.Range;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** End-to-end coverage for routed index range scans and their fixed views. */
class TableRangeScanITCase extends ClientToServerITCaseBase {

    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .column("g", DataTypes.INT())
                    .column("t", DataTypes.BIGINT())
                    .column("id", DataTypes.INT())
                    .column("v", DataTypes.STRING())
                    .primaryKey("g", "t", "id")
                    .build();

    @ParameterizedTest
    @EnumSource(ReadDirection.class)
    void testDirectionAcrossBoundsAndFullBucketLimit(ReadDirection direction) throws Exception {
        try (Table table = create("direction", SCHEMA, false)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            CompletableFuture<UpsertResult> routing = writer.upsert(row(1, 0L, 0, "v"));
            for (long t : new long[] {-1, 0, 127, 128, Long.MAX_VALUE}) {
                for (int id : new int[] {0, 1}) {
                    writer.upsert(row(1, t, id, "v"));
                }
            }
            writer.flush();
            Scan base =
                    table.newScan()
                            .direction(direction)
                            .equalTo(Collections.singletonList("g"), row(1));
            List<String> expected = Arrays.asList("127:0", "127:1", "128:0", "128:1");
            List<String> ordered = new ArrayList<>(expected);
            if (direction == ReadDirection.REVERSE) {
                Collections.reverse(ordered);
            }
            try (BatchScanner scanner =
                    base.range("t", Range.closed(key(127), key(128)))
                            .project(Arrays.asList("t", "id"))
                            .limit(3)
                            .createBatchScanner()) {
                assertThat(
                                BatchScanUtils.collectRows(scanner).stream()
                                        .map(r -> r.getLong(0) + ":" + r.getInt(1))
                                        .collect(Collectors.toList()))
                        .containsExactlyElementsOf(ordered.subList(0, 3));
            }
            try (BatchScanner scanner =
                    base.range("t", Range.closedOpen(key(127), key(128))).createBatchScanner()) {
                assertThat(
                                BatchScanUtils.collectRows(scanner).stream()
                                        .map(r -> r.getInt(2))
                                        .collect(Collectors.toList()))
                        .containsExactlyElementsOf(
                                direction == ReadDirection.FORWARD
                                        ? Arrays.asList(0, 1)
                                        : Arrays.asList(1, 0));
            }
            try (BatchScanner scanner =
                    base.range("t", Range.greaterThan(key(Long.MAX_VALUE))).createBatchScanner()) {
                assertThat(BatchScanUtils.collectRows(scanner)).isEmpty();
            }
            try (BatchScanner scanner =
                    base.range("t", Range.atLeast(key(Long.MAX_VALUE))).createBatchScanner()) {
                assertThat(BatchScanUtils.collectRows(scanner)).hasSize(2);
            }
            int bucketId = routing.get(30, TimeUnit.SECONDS).getBucket().getBucket();
            TableBucket bucket = new TableBucket(table.getTableInfo().getTableId(), bucketId);
            // Full scans with limit must use the same directed, paginated KV path.
            try (BatchScanner scanner =
                    table.newScan().direction(direction).limit(1).createBatchScanner(bucket)) {
                List<InternalRow> rows = BatchScanUtils.collectRows(scanner);
                assertThat(rows).hasSize(1);
                assertThat(rows.get(0).getLong(1))
                        .isEqualTo(direction == ReadDirection.FORWARD ? -1L : Long.MAX_VALUE);
                assertThat(rows.get(0).getInt(2))
                        .isEqualTo(direction == ReadDirection.FORWARD ? 0 : 1);
            }
            try (BatchScanner scanner =
                    table.newScan().direction(direction).limit(0).createBatchScanner(bucket)) {
                assertThat(BatchScanUtils.collectRows(scanner)).isEmpty();
            }
            assertThatThrownBy(
                            () ->
                                    table.newScan()
                                            .direction(direction)
                                            .limit(-1)
                                            .createBatchScanner(bucket))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> table.newScan().direction(direction).createBatchScanner())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("single index bucket");
            assertThatThrownBy(() -> table.newScan().direction(direction).createLogScanner())
                    .isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(
                            () ->
                                    table.newScan()
                                            .direction(direction)
                                            .createBatchScanner(bucket, 1L))
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    @Test
    void testWholeTableDirectionWithSingleBucket() throws Exception {
        TablePath path = TablePath.of("range_scan", "single_bucket_direction");
        long tableId =
                createTable(
                        path,
                        TableDescriptor.builder()
                                .schema(SCHEMA)
                                .distributedBy(1, "g")
                                .property(ConfigOptions.TABLE_KV_ORDERED_KEY_ENABLED, true)
                                .build(),
                        true);
        waitAllReplicasReady(tableId, 1);
        try (Table table = conn.getTable(path)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(row(1, 100L, 0, "first"));
            writer.upsert(row(2, 0L, 0, "last"));
            writer.flush();
            Scan base =
                    table.newScan().direction(ReadDirection.REVERSE).project(Arrays.asList("v"));
            try (BatchScanner scanner = base.limit(1).createBatchScanner()) {
                assertThat(
                                BatchScanUtils.collectRows(scanner).stream()
                                        .map(r -> r.getString(0).toString())
                                        .collect(Collectors.toList()))
                        .containsExactly("last");
            }
            try (BatchScanner scanner =
                    base.direction(ReadDirection.FORWARD).createBatchScanner()) {
                assertThat(
                                BatchScanUtils.collectRows(scanner).stream()
                                        .map(r -> r.getString(0).toString())
                                        .collect(Collectors.toList()))
                        .containsExactly("first", "last");
            }
        }
    }

    @Test
    void testRangeBoundsProjectionLimitAndInputReuse() throws Exception {
        try (Table table = create("bounds", SCHEMA, false)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            for (int g : new int[] {1, 2}) {
                for (long time : new long[] {-1, 0, 127, 128, 255, 256, 300, 301, Long.MAX_VALUE}) {
                    for (int id : new int[] {0, 1}) {
                        writer.upsert(row(g, time, id, "v"));
                    }
                }
            }
            writer.flush();
            assertTimes(table, Range.closedOpen(key(255), key(301)), 255, 255, 256, 256, 300, 300);
            assertTimes(
                    table,
                    Range.closed(key(255), key(301)),
                    255,
                    255,
                    256,
                    256,
                    300,
                    300,
                    301,
                    301);
            assertTimes(table, Range.open(key(255), key(301)), 256, 256, 300, 300);
            assertTimes(table, Range.openClosed(key(255), key(301)), 256, 256, 300, 300, 301, 301);
            assertTimes(table, Range.lessThan(key(0)), -1, -1);
            assertTimes(table, Range.atMost(key(0)), -1, -1, 0, 0);
            assertTimes(table, Range.greaterThan(key(301)), Long.MAX_VALUE, Long.MAX_VALUE);
            assertTimes(table, Range.atLeast(key(Long.MAX_VALUE)), Long.MAX_VALUE, Long.MAX_VALUE);
            assertTimes(table, Range.greaterThan(key(Long.MAX_VALUE)));
            assertTimes(table, Range.closedOpen(key(255), key(255)));
            assertTimes(table, Range.closed(key(255), key(255)), 255, 255);
            assertThatThrownBy(
                            () ->
                                    table.newScan()
                                            .equalTo(
                                                    Collections.singletonList("g"),
                                                    GenericRow.of(1))
                                            .range("t", Range.closedOpen(key(301), key(255)))
                                            .createBatchScanner())
                    .isInstanceOf(IllegalArgumentException.class);

            try (BatchScanner scanner =
                    table.newScan()
                            .project(Arrays.asList("v", "t"))
                            .limit(3)
                            .equalTo(Collections.singletonList("g"), GenericRow.of(1))
                            .range("t", Range.atLeast(key(255)))
                            .createBatchScanner()) {
                List<InternalRow> result = BatchScanUtils.collectRows(scanner);
                assertThat(result).hasSize(3);
                assertThat(result.get(0).getFieldCount()).isEqualTo(2);
                assertThat(result.get(0).getString(0).toString()).isEqualTo("v");
            }
            GenericRow group = GenericRow.of(1);
            GenericRow lower = key(255);
            try (BatchScanner scanner =
                    table.newScan()
                            .equalTo(Collections.singletonList("g"), group)
                            .range("t", Range.closedOpen(lower, key(301)))
                            .createBatchScanner()) {
                group.setField(0, 2);
                lower.setField(0, Long.MAX_VALUE);
                assertThat(BatchScanUtils.collectRows(scanner)).hasSize(6);
            }
            try (BatchScanner scanner =
                    table.newScan()
                            .limit(0)
                            .equalTo(Collections.singletonList("g"), GenericRow.of(1))
                            .range("t", Range.atLeast(key(0)))
                            .createBatchScanner()) {
                assertThat(BatchScanUtils.collectRows(scanner)).isEmpty();
            }
        }
    }

    @Test
    void testMultipleEqualityColumnsAndImmutableRefinements() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("g", DataTypes.BYTES())
                        .column("h", DataTypes.STRING())
                        .column("t", DataTypes.BIGINT())
                        .column("id", DataTypes.INT())
                        .column("v", DataTypes.STRING())
                        .primaryKey("g", "h", "t", "id")
                        .build();
        try (Table table = create("refinements", schema, false)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(row(new byte[] {1}, "a", 255L, 0, "first"));
            writer.upsert(row(new byte[] {1}, "b", 256L, 0, "second"));
            writer.upsert(row(new byte[] {2}, "a", 255L, 0, "other"));
            writer.flush();
            Scan base = table.newScan().project(Collections.singletonList("v")).limit(10);
            List<String> columns = new ArrayList<>(Arrays.asList("g", "h"));
            byte[] group = new byte[] {1};
            byte[] text = new byte[] {'a'};
            GenericRow values = GenericRow.of(group, BinaryString.fromBytes(text));
            GenericRow lower = key(255);
            GenericRow upper = key(256);
            Scan first = base.equalTo(columns, values).range("t", Range.closedOpen(lower, upper));
            columns.set(0, "id");
            group[0] = 2;
            text[0] = 'b';
            values.setField(1, BinaryString.fromString("reused"));
            lower.setField(0, 999L);
            upper.setField(0, 999L);
            Scan second =
                    first.equalTo(Arrays.asList("h", "g"), row("b", new byte[] {1}))
                            .range("id", Range.atLeast(GenericRow.of(0)))
                            .range("t", Range.closed(key(256), key(256)));
            try (BatchScanner scanner = first.createBatchScanner()) {
                assertThat(BatchScanUtils.collectRows(scanner))
                        .extracting(r -> r.getString(0).toString())
                        .containsExactly("first");
            }
            try (BatchScanner scanner = second.createBatchScanner()) {
                assertThat(BatchScanUtils.collectRows(scanner))
                        .extracting(r -> r.getString(0).toString())
                        .containsExactly("second");
            }
            try (BatchScanner scanner =
                    base.range("t", Range.atMost(key(255)))
                            .equalTo(Arrays.asList("g", "h"), row(new byte[] {1}, "a"))
                            .createBatchScanner()) {
                assertThat(BatchScanUtils.collectRows(scanner))
                        .extracting(r -> r.getString(0).toString())
                        .containsExactly("first");
            }
            try (BatchScanner scanner = base.createBatchScanner()) {
                assertThat(BatchScanUtils.collectRows(scanner)).hasSize(3);
            }
        }
    }

    @Test
    void testExplicitBucketRestrictsRangeResults() throws Exception {
        try (Table table = create("bucket_scope", SCHEMA, false)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(row(1, 255L, 0, "match"));
            writer.flush();
            Scan scan =
                    table.newScan()
                            .equalTo(Collections.singletonList("g"), GenericRow.of(1))
                            .range("t", Range.closed(key(255), key(255)));
            BucketCalculator calculator = BucketCalculator.forBucketKeyRow(table.getTableInfo());
            int bucket = calculator.getBucketId(calculator.encodeKey(GenericRow.of(1)), 3);
            long tableId = table.getTableInfo().getTableId();
            try (BatchScanner scanner = scan.createBatchScanner(new TableBucket(tableId, bucket))) {
                assertThat(BatchScanUtils.collectRows(scanner)).hasSize(1);
            }
            try (BatchScanner scanner =
                    scan.createBatchScanner(new TableBucket(tableId, (bucket + 1) % 3))) {
                assertThat(BatchScanUtils.collectRows(scanner)).isEmpty();
            }
            assertThatThrownBy(() -> scan.createBatchScanner(new TableBucket(tableId + 1, bucket)))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> scan.createBatchScanner(new TableBucket(tableId, 3)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void testPartitionRoutingAndReorderedEqualityColumns() throws Exception {
        Schema partitioned =
                Schema.newBuilder()
                        .column("g", DataTypes.INT())
                        .column("t", DataTypes.BIGINT())
                        .column("id", DataTypes.INT())
                        .column("v", DataTypes.STRING())
                        .column("p", DataTypes.STRING())
                        .primaryKey("g", "t", "id", "p")
                        .build();
        try (Table table = create("partitioned", partitioned, true)) {
            TablePath path = table.getTableInfo().getTablePath();
            admin.createPartition(
                            path, new PartitionSpec(Collections.singletonMap("p", "p1")), false)
                    .get();
            admin.createPartition(
                            path, new PartitionSpec(Collections.singletonMap("p", "p2")), false)
                    .get();
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(row(1, 255L, 0, "v", "p1"));
            writer.flush();
            Scan scan = table.newScan().range("t", Range.atLeast(key(255)));
            BucketCalculator calculator = BucketCalculator.forBucketKeyRow(table.getTableInfo());
            int bucket = calculator.getBucketId(calculator.encodeKey(GenericRow.of(1)), 3);
            for (PartitionInfo partition : admin.listPartitionInfos(path).get()) {
                try (BatchScanner scanner =
                        scan.equalTo(Arrays.asList("p", "g"), row("p1", 1))
                                .createBatchScanner(
                                        new TableBucket(
                                                table.getTableInfo().getTableId(),
                                                partition.getPartitionId(),
                                                bucket))) {
                    assertThat(BatchScanUtils.collectRows(scanner))
                            .hasSize(partition.getPartitionName().equals("p1") ? 1 : 0);
                }
            }
            try (BatchScanner scanner =
                    scan.equalTo(Arrays.asList("p", "g"), row("p1", 1)).createBatchScanner()) {
                assertThat(BatchScanUtils.collectRows(scanner)).hasSize(1);
            }
            try (BatchScanner scanner =
                    scan.equalTo(Arrays.asList("p", "g"), row("missing", 1)).createBatchScanner()) {
                assertThat(BatchScanUtils.collectRows(scanner)).isEmpty();
            }
        }
    }

    @ParameterizedTest
    @EnumSource(ReadDirection.class)
    void testMultipleBatchesKeepTheirOpenedView(ReadDirection direction) throws Exception {
        try (Table table = create("snapshot", SCHEMA, false)) {
            org.apache.fluss.config.Configuration config =
                    new org.apache.fluss.config.Configuration(clientConf);
            config.set(
                    ConfigOptions.CLIENT_SCANNER_KV_FETCH_MAX_BYTES,
                    new org.apache.fluss.config.MemorySize(80));
            try (org.apache.fluss.client.Connection small =
                            org.apache.fluss.client.ConnectionFactory.createConnection(config);
                    Table smallTable = small.getTable(table.getTableInfo().getTablePath())) {
                UpsertWriter writer = smallTable.newUpsert().createWriter();
                for (int i = 0; i < 30; i++) {
                    writer.upsert(row(1, (long) i, 0, "payload"));
                }
                writer.flush();
                List<InternalRow> result = new ArrayList<>();
                try (BatchScanner scanner =
                        smallTable
                                .newScan()
                                .direction(direction)
                                .equalTo(Collections.singletonList("g"), GenericRow.of(1))
                                .range("t", Range.closedOpen(key(0), key(100)))
                                .createBatchScanner()) {
                    while (result.isEmpty()) {
                        try (CloseableIterator<InternalRow> first =
                                scanner.pollBatch(Duration.ofSeconds(10))) {
                            assertThat(first).isNotNull();
                            first.forEachRemaining(result::add);
                        }
                    }
                    assertThat(result.size()).isLessThan(30);
                    writer.upsert(row(1, 99L, 0, "late"));
                    writer.flush();
                    result.addAll(BatchScanUtils.collectRows(scanner));
                }
                List<Long> expected =
                        java.util.stream.LongStream.range(0, 30)
                                .boxed()
                                .collect(Collectors.toList());
                if (direction == ReadDirection.REVERSE) {
                    Collections.reverse(expected);
                }
                assertThat(result.stream().map(r -> r.getLong(1)).collect(Collectors.toList()))
                        .containsExactlyElementsOf(expected);
                assertThat(result.stream().map(r -> r.getLong(1))).doesNotContain(99L);
                assertTimes(table, Range.closed(key(99), key(99)), 99);
            }
        }
    }

    @Test
    void testRejectIncompatibleConfigurations() throws Exception {
        try (Table table = create("invalid", SCHEMA, false)) {
            Scan configured =
                    table.newScan()
                            .equalTo(Collections.singletonList("g"), GenericRow.of(1))
                            .range("t", Range.atLeast(key(0)));
            assertThatThrownBy(configured::createLogScanner)
                    .isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(
                            () ->
                                    configured.createBatchScanner(
                                            new TableBucket(table.getTableInfo().getTableId(), 0),
                                            1L))
                    .isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(
                            () ->
                                    table.newScan()
                                            .equalTo(
                                                    Collections.singletonList("g"),
                                                    GenericRow.of(1))
                                            .createBatchScanner())
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(
                            () ->
                                    table.newScan()
                                            .range("t", Range.atLeast(key(0)))
                                            .createBatchScanner())
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(
                            () ->
                                    table.newScan()
                                            .equalTo(
                                                    Collections.singletonList("g"),
                                                    GenericRow.of(1))
                                            .range("id", Range.atLeast(GenericRow.of(1)))
                                            .createBatchScanner())
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(
                            () ->
                                    configured.range(
                                            "t", Range.atLeast(GenericRow.of((Object) null))))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> configured.equalTo(Arrays.asList("g", "g"), row(1, 1)))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> configured.equalTo(Collections.singletonList("g"), row(1, 2)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        TablePath legacy = TablePath.of("range_scan", "legacy");
        createTable(
                legacy,
                TableDescriptor.builder().schema(SCHEMA).distributedBy(3, "g").build(),
                true);
        try (Table table = conn.getTable(legacy)) {
            for (ReadDirection direction : ReadDirection.values()) {
                assertThatThrownBy(() -> table.newScan().direction(direction).createBatchScanner())
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("ordered-key.enabled=true");
            }
            assertThatThrownBy(
                            () ->
                                    table.newScan()
                                            .equalTo(
                                                    Collections.singletonList("g"),
                                                    GenericRow.of(1))
                                            .range("t", Range.atLeast(key(0)))
                                            .createBatchScanner())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("ordered-key.enabled=true");
        }
    }

    private Table create(String name, Schema schema, boolean partitioned) throws Exception {
        TablePath path = TablePath.of("range_scan", name);
        TableDescriptor.Builder descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(3, "g")
                        .property(ConfigOptions.TABLE_KV_ORDERED_KEY_ENABLED, true);
        if (partitioned) {
            descriptor.partitionedBy("p");
        }
        long id = createTable(path, descriptor.build(), true);
        if (!partitioned) {
            waitAllReplicasReady(id, 3);
        }
        return conn.getTable(path);
    }

    private static GenericRow key(long time) {
        return GenericRow.of(time);
    }

    private static void assertTimes(
            Table table, Range<? extends InternalRow> range, long... expected) throws Exception {
        try (BatchScanner scanner =
                table.newScan()
                        .equalTo(Collections.singletonList("g"), GenericRow.of(1))
                        .range("t", range)
                        .createBatchScanner()) {
            List<InternalRow> rows = BatchScanUtils.collectRows(scanner);
            assertThat(rows.stream().map(r -> r.getLong(1)).collect(Collectors.toList()))
                    .containsExactlyElementsOf(
                            Arrays.stream(expected).boxed().collect(Collectors.toList()));
            assertThat(rows).allSatisfy(r -> assertThat(r.getInt(0)).isEqualTo(1));
        }
    }
}
