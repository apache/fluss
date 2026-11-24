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

import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Covers the two Hop2 safeguards of a secondary index lookup that end-to-end tests cannot isolate:
 * the recheck that discards stale index pointers, and the candidate deduplication that keeps a
 * recreated partition's old and new physical index rows from emitting the same main row twice.
 *
 * <p>Main table layout is {@code (id INT PK, city STRING, age INT)} with a composite index on
 * {@code (city, age)}, so the recheck is exercised over more than one column.
 */
class SecondaryIndexLookuperTest {

    private static final Schema MAIN_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("city", DataTypes.STRING())
                    .column("age", DataTypes.INT())
                    .primaryKey("id")
                    .build();

    /** Main rows keyed by base primary key, standing in for the main table's point-get. */
    private final Map<Integer, InternalRow> mainRowsByPk = new LinkedHashMap<>();

    private final List<Integer> mainLookupKeys = new ArrayList<>();

    @Test
    void returnsEachCurrentMatchOnceAfterLookingUpDistinctCandidates() throws Exception {
        mainRowsByPk.put(1, row(1, "Beijing", 30));
        mainRowsByPk.put(2, row(2, "Shanghai", 31));
        mainRowsByPk.put(3, row(3, null, 30));
        mainRowsByPk.put(4, row(4, "Shanghai", 30));

        mainRowsByPk.put(6, row(6, "Shanghai", 30));

        // Key 4 has two physical index entries. Key 5 was deleted from the main table.
        LookupResult result = lookup(indexRowsFor(1, 2, 3, 4, 4, 5, 6), row("Shanghai", 30));
        assertThat(mainLookupKeys).containsExactlyInAnyOrder(1, 2, 3, 4, 5, 6);
        assertThat(result.getRowList())
                .extracting(r -> r.getInt(0))
                .containsExactlyInAnyOrder(4, 6);
    }

    @Test
    void closingClientFailsQueuedMainTableLookup() throws Exception {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.CLIENT_LOOKUP_CONTINUATION_THREADS, 1);
        LookupClient client = new LookupClient(conf, mock(MetadataUpdater.class));
        CountDownLatch running = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try {
            Lookuper lookuper =
                    new SecondaryIndexLookuper(
                            MAIN_SCHEMA,
                            Arrays.asList("city", "age"),
                            MAIN_SCHEMA.getRowType().project(Collections.singletonList("id")),
                            ignored ->
                                    CompletableFuture.completedFuture(
                                            new LookupResult(indexRowsFor(4))),
                            ignored -> {
                                running.countDown();
                                try {
                                    release.await();
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                                return CompletableFuture.completedFuture(
                                        new LookupResult(Collections.emptyList()));
                            },
                            client::executeContinuation);
            lookuper.lookup(row("A", 30));
            assertThat(running.await(10, TimeUnit.SECONDS)).isTrue();
            CompletableFuture<LookupResult> queued = lookuper.lookup(row("B", 31));
            client.close(Duration.ZERO);
            assertThat(queued).isCompletedExceptionally();
        } finally {
            release.countDown();
            client.close(Duration.ZERO);
        }
    }

    @Test
    void retainsLookupKeyUntilCandidatesReturn() throws Exception {
        byte[] city = new byte[] {'A'};
        CompletableFuture<LookupResult> candidates = new CompletableFuture<>();
        SecondaryIndexLookuper lookuper =
                new SecondaryIndexLookuper(
                        MAIN_SCHEMA,
                        Arrays.asList("city", "age"),
                        MAIN_SCHEMA.getRowType().project(Collections.singletonList("id")),
                        ignored -> candidates,
                        ignored ->
                                CompletableFuture.completedFuture(
                                        new LookupResult(
                                                Collections.singletonList(row(4, "A", 30)))),
                        Runnable::run);
        CompletableFuture<LookupResult> result =
                lookuper.lookup(GenericRow.of(BinaryString.fromBytes(city), 30));
        city[0] = 'B';
        candidates.complete(new LookupResult(indexRowsFor(4)));
        assertThat(result.get().getRowList()).extracting(r -> r.getInt(0)).containsExactly(4);
    }

    @Test
    void testNoCandidatesYieldsNoMainLookup() throws Exception {
        LookupResult result = lookup(Collections.emptyList(), row("Shanghai", 30));

        assertThat(mainLookupKeys).isEmpty();
        assertThat(result.getRowList()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void testNullLookupDoesNotInvokeEitherHop(int fieldCount) throws Exception {
        DataType[] types = new DataType[fieldCount];
        Arrays.fill(types, DataTypes.INT().copy(false));
        RowType keyType = RowType.of(types);
        Schema schema = Schema.newBuilder().fromRowType(keyType).primaryKey("f0").build();
        Lookuper unexpectedLookup =
                key -> {
                    throw new AssertionError("NULL index keys must not issue lookups");
                };
        SecondaryIndexLookuper lookuper =
                new SecondaryIndexLookuper(
                        schema,
                        keyType.getFieldNames(),
                        keyType,
                        unexpectedLookup,
                        unexpectedLookup,
                        Runnable::run);
        for (int nullField = 0; nullField < fieldCount; nullField++) {
            GenericRow key = new GenericRow(fieldCount);
            for (int i = 0; i < fieldCount; i++) {
                key.setField(i, i == nullField ? null : i);
            }
            assertThat(lookuper.lookup(key).get().getRowList()).isEmpty();
        }
    }

    private LookupResult lookup(List<InternalRow> hop1Rows, InternalRow lookupKey)
            throws Exception {
        Lookuper indexTablePrefixLookuper =
                ignored -> CompletableFuture.completedFuture(new LookupResult(hop1Rows));
        Lookuper mainTablePointLookuper =
                basePk -> {
                    int pk = basePk.getInt(0);
                    mainLookupKeys.add(pk);
                    InternalRow mainRow = mainRowsByPk.get(pk);
                    return CompletableFuture.completedFuture(
                            new LookupResult(
                                    mainRow == null
                                            ? Collections.<InternalRow>emptyList()
                                            : Collections.singletonList(mainRow)));
                };
        SecondaryIndexLookuper lookuper =
                new SecondaryIndexLookuper(
                        MAIN_SCHEMA,
                        Arrays.asList("city", "age"),
                        MAIN_SCHEMA.getRowType().project(Collections.singletonList("id")),
                        indexTablePrefixLookuper,
                        mainTablePointLookuper,
                        Runnable::run);
        return lookuper.lookup(lookupKey).get();
    }

    /** Index rows carry only the base primary key here; Hop1 content beyond it is unused. */
    private static List<InternalRow> indexRowsFor(int... basePks) {
        List<InternalRow> indexRows = new ArrayList<>(basePks.length);
        for (int basePk : basePks) {
            indexRows.add(row(basePk));
        }
        return indexRows;
    }
}
