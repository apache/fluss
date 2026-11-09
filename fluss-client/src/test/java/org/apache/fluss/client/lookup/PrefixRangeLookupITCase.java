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

package org.apache.fluss.client.lookup;

import org.apache.fluss.client.admin.ClientToServerITCaseBase;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.Range;

import org.junit.jupiter.api.Test;

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

/** Complete request-local prefix range reads through the public lookup interfaces. */
class PrefixRangeLookupITCase extends ClientToServerITCaseBase {
    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .column("g", DataTypes.INT())
                    .column("t", DataTypes.BIGINT())
                    .column("id", DataTypes.INT())
                    .primaryKey("g", "t", "id")
                    .build();

    @Test
    void testBoundariesAndCompleteResults() throws Exception {
        try (Table table = create("bounds", SCHEMA, true, false)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            for (int g : new int[] {1, 2}) {
                for (long t : new long[] {-1, 0, 127, 128, 255, 256, Long.MAX_VALUE}) {
                    writer.upsert(row(g, t, 0));
                    writer.upsert(row(g, t, 1));
                }
            }
            writer.flush();
            Lookuper lookuper = table.newLookup().lookupBy("g").rangeBy("t").createLookuper();
            assertTimes(
                    lookuper, Range.closedOpen(key(127), key(256)), 127, 127, 128, 128, 255, 255);
            assertTimes(
                    lookuper, Range.openClosed(key(127), key(256)), 128, 128, 255, 255, 256, 256);
            assertTimes(lookuper, Range.open(key(127), key(256)), 128, 128, 255, 255);
            assertTimes(
                    lookuper,
                    Range.closed(key(127), key(256)),
                    127,
                    127,
                    128,
                    128,
                    255,
                    255,
                    256,
                    256);
            assertTimes(lookuper, Range.lessThan(key(0)), -1, -1);
            assertTimes(lookuper, Range.atMost(key(0)), -1, -1, 0, 0);
            assertTimes(
                    lookuper, Range.atLeast(key(Long.MAX_VALUE)), Long.MAX_VALUE, Long.MAX_VALUE);
            assertTimes(lookuper, Range.greaterThan(key(Long.MAX_VALUE)));
            assertTimes(lookuper, Range.closedOpen(key(128), key(128)));
            assertTimes(lookuper, Range.open(key(128), key(128)));
            assertTimes(lookuper, Range.closed(key(128), key(128)), 128, 128);
            assertThatThrownBy(() -> lookuper.lookup(row(1), Range.closed(key(2), key(1))))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(
                            lookuper.lookup(row(3), Range.atLeast(key(0)))
                                    .get(30, TimeUnit.SECONDS)
                                    .getRowList())
                    .isEmpty();
        }
    }

    @Test
    void testIndependentBoundsAndInputReuse() throws Exception {
        try (Table table = create("reuse", SCHEMA, true, false)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            for (int g : new int[] {1, 2}) {
                for (long t = 0; t < 20; t++) {
                    writer.upsert(row(g, t, 0));
                }
            }
            writer.flush();
            Lookup base = table.newLookup().lookupBy("g");
            Lookuper bounded = base.rangeBy("id").rangeBy("t").createLookuper();
            Lookuper ordinary = base.createLookuper();
            GenericRow prefix = row(1);
            GenericRow lower = key(0);
            GenericRow upper = key(0);
            List<CompletableFuture<LookupResult>> results = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                lower.setField(0, (long) i);
                upper.setField(0, (long) i);
                results.add(bounded.lookup(prefix, Range.closed(lower, upper)));
            }
            prefix.setField(0, 2);
            lower.setField(0, 100L);
            upper.setField(0, 101L);
            CompletableFuture<LookupResult> unbounded = ordinary.lookup(row(1));
            for (int i = 0; i < results.size(); i++) {
                List<InternalRow> rows = results.get(i).get(30, TimeUnit.SECONDS).getRowList();
                assertThat(rows).hasSize(1);
                assertThat(rows.get(0).getInt(0)).isEqualTo(1);
                assertThat(rows.get(0).getLong(1)).isEqualTo((long) i);
            }
            assertThat(unbounded.get(30, TimeUnit.SECONDS).getRowList()).hasSize(20);
        }
    }

    @Test
    void testMultipleEqualityColumnsAndTypedKeys() throws Exception {
        try (Table table = create("typed", SCHEMA, true, false)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(row(1, 100L, 1));
            writer.upsert(row(1, 100L, 2));
            writer.upsert(row(1, 101L, 2));
            writer.flush();
            TypedLookuper<PrefixKey> lookuper =
                    table.newLookup()
                            .rangeBy("id")
                            .lookupBy("g", "t")
                            .createTypedLookuper(PrefixKey.class);
            PrefixKey prefix = new PrefixKey();
            prefix.g = 1;
            prefix.t = 100L;
            CompletableFuture<LookupResult> result =
                    lookuper.lookup(prefix, Range.closed(row(2), row(2)));
            prefix.t = 101L;
            List<InternalRow> rows = result.get(30, TimeUnit.SECONDS).getRowList();
            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getLong(1)).isEqualTo(100L);
            assertThat(rows.get(0).getInt(2)).isEqualTo(2);
            assertThatThrownBy(() -> lookuper.lookup(prefix))
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    @Test
    void testPartitionRoutingAndMissingPartition() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("p", DataTypes.STRING())
                        .column("g", DataTypes.INT())
                        .column("t", DataTypes.BIGINT())
                        .primaryKey("p", "g", "t")
                        .build();
        TablePath path = TablePath.of("prefix_range", "partitioned");
        try (Table table = create("partitioned", schema, true, true)) {
            admin.createPartition(
                            path, new PartitionSpec(Collections.singletonMap("p", "p1")), false)
                    .get();
            admin.createPartition(
                            path, new PartitionSpec(Collections.singletonMap("p", "p2")), false)
                    .get();
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(row("p1", 1, 100L));
            writer.upsert(row("p2", 1, 100L));
            writer.flush();
            Lookuper lookuper = table.newLookup().lookupBy("p", "g").rangeBy("t").createLookuper();
            List<InternalRow> rows =
                    lookuper.lookup(row("p1", 1), Range.atLeast(key(0)))
                            .get(30, TimeUnit.SECONDS)
                            .getRowList();
            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getString(0).toString()).isEqualTo("p1");
            assertThat(
                            lookuper.lookup(row("missing", 1), Range.atLeast(key(0)))
                                    .get(30, TimeUnit.SECONDS)
                                    .getRowList())
                    .isEmpty();
        }
    }

    @Test
    void testRejectUnsupportedConfigurationsAndCalls() throws Exception {
        try (Table table = create("validation", SCHEMA, true, false)) {
            Lookup base = table.newLookup().lookupBy("g");
            assertThatThrownBy(() -> table.newLookup().rangeBy("t").createLookuper())
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> base.rangeBy("id").createLookuper())
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> base.rangeBy("t").enableInsertIfNotExists().createLookuper())
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> base.enableInsertIfNotExists().rangeBy("t").createLookuper())
                    .isInstanceOf(IllegalArgumentException.class);
            Lookuper bounded = base.rangeBy("t").createLookuper();
            assertThatThrownBy(() -> bounded.lookup(row(1)))
                    .isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> base.createLookuper().lookup(row(1), Range.atLeast(key(0))))
                    .isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(
                            () ->
                                    table.newLookup()
                                            .createLookuper()
                                            .lookup(row(1, 0L, 0), Range.atLeast(key(0))))
                    .isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> bounded.lookup(row(1, 2), Range.atLeast(key(0))))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> bounded.lookup(row((Object) null), Range.atLeast(key(0))))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> bounded.lookup(row(1), Range.atLeast(row((Object) null))))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> bounded.lookup(row(1), Range.atLeast(row(0L, 1L))))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        try (Table table = create("legacy", SCHEMA, false, false)) {
            assertThatThrownBy(() -> table.newLookup().lookupBy("g").rangeBy("t").createLookuper())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("ordered-key");
        }
    }

    private Table create(String name, Schema schema, boolean ordered, boolean partitioned)
            throws Exception {
        TablePath path = TablePath.of("prefix_range", name);
        TableDescriptor.Builder descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(3, "g")
                        .property(ConfigOptions.TABLE_KV_ORDERED_KEY_ENABLED, ordered);
        if (partitioned) {
            descriptor.partitionedBy("p");
        }
        long tableId = createTable(path, descriptor.build(), true);
        if (!partitioned) {
            waitAllReplicasReady(tableId, 3);
        }
        return conn.getTable(path);
    }

    private static GenericRow key(long value) {
        return row(value);
    }

    private static void assertTimes(
            Lookuper lookuper, Range<? extends InternalRow> bounds, long... expected)
            throws Exception {
        List<InternalRow> rows =
                lookuper.lookup(row(1), bounds).get(30, TimeUnit.SECONDS).getRowList();
        assertThat(rows.stream().map(row -> row.getLong(1)).collect(Collectors.toList()))
                .containsExactlyInAnyOrderElementsOf(
                        Arrays.stream(expected).boxed().collect(Collectors.toList()));
        assertThat(rows).allSatisfy(row -> assertThat(row.getInt(0)).isEqualTo(1));
    }

    /** Equality key for a typed range lookup. */
    public static class PrefixKey {
        public Integer g;
        public Long t;
    }
}
