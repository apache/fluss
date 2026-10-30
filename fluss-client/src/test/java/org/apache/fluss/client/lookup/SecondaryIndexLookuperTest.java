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

import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers the two Hop2 safeguards of a secondary index lookup that end-to-end tests cannot isolate:
 * the recheck that discards stale index pointers, and the candidate deduplication that keeps a
 * recreated partition's old and new physical index rows from emitting the same main row twice.
 *
 * <p>Main table layout is {@code (id INT PK, city STRING, age INT)} with a composite index on
 * {@code (city, age)}, so the recheck is exercised over more than one column.
 */
class SecondaryIndexLookuperTest {

    private static final DataType[] IDX_COLUMN_TYPES = {DataTypes.STRING(), DataTypes.INT()};

    /** Main rows keyed by base primary key, standing in for the main table's point-get. */
    private final Map<Integer, InternalRow> mainRowsByPk = new LinkedHashMap<>();

    private final List<Integer> mainLookupKeys = new ArrayList<>();

    @Test
    void testStaleIndexPointerIsDroppedWhileMatchingCandidateSurvives() throws Exception {
        mainRowsByPk.put(7, row(7, "Beijing", 30));
        mainRowsByPk.put(8, row(8, "Shanghai", 30));

        LookupResult result = lookup(indexRowsFor(7, 8), row("Shanghai", 30));

        // Both candidates were fetched, but only the one whose current row still carries the
        // looked-up index values is returned.
        assertThat(mainLookupKeys).containsExactly(7, 8);
        assertThat(basePksOf(result)).containsExactly(8);
    }

    @Test
    void testRecheckComparesEveryIndexColumn() throws Exception {
        mainRowsByPk.put(7, row(7, "Shanghai", 31));

        LookupResult result = lookup(indexRowsFor(7), row("Shanghai", 30));

        // The first index column still matches, so a recheck that only compared column 0 would
        // wrongly keep this row.
        assertThat(mainLookupKeys).containsExactly(7);
        assertThat(result.getRowList()).isEmpty();
    }

    @Test
    void testNullIndexColumnInMainRowIsDropped() throws Exception {
        mainRowsByPk.put(7, row(7, null, 30));

        LookupResult result = lookup(indexRowsFor(7), row("Shanghai", 30));

        assertThat(result.getRowList()).isEmpty();
    }

    @Test
    void testDuplicateCandidatesYieldOneMainLookupAndOneRow() throws Exception {
        mainRowsByPk.put(7, row(7, "Shanghai", 30));

        // Two physical index rows pointing at the same base primary key, as a recreated partition
        // leaves behind.
        LookupResult result = lookup(indexRowsFor(7, 7), row("Shanghai", 30));

        assertThat(mainLookupKeys).containsExactly(7);
        assertThat(basePksOf(result)).containsExactly(7);
    }

    @Test
    void testDeletedMainRowIsSkipped() throws Exception {
        // No entry for pk 7: the main point-get comes back empty, i.e. the row was deleted while
        // the index pointer still existed.
        LookupResult result = lookup(indexRowsFor(7), row("Shanghai", 30));

        assertThat(mainLookupKeys).containsExactly(7);
        assertThat(result.getRowList()).isEmpty();
    }

    @Test
    void testNoCandidatesYieldsNoMainLookup() throws Exception {
        LookupResult result = lookup(Collections.emptyList(), row("Shanghai", 30));

        assertThat(mainLookupKeys).isEmpty();
        assertThat(result.getRowList()).isEmpty();
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
                        indexTablePrefixLookuper,
                        mainTablePointLookuper,
                        key -> key,
                        new InternalRow.FieldGetter[] {
                            InternalRow.createFieldGetter(DataTypes.STRING(), 0),
                            InternalRow.createFieldGetter(DataTypes.INT(), 1)
                        },
                        new InternalRow.FieldGetter[] {
                            InternalRow.createFieldGetter(DataTypes.STRING(), 1),
                            InternalRow.createFieldGetter(DataTypes.INT(), 2)
                        },
                        IDX_COLUMN_TYPES,
                        SecondaryIndexLookuperTest::basePkOfIndexRow,
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

    private static GenericRow basePkOfIndexRow(InternalRow indexRow) {
        GenericRow basePk = new GenericRow(1);
        basePk.setField(0, indexRow.getInt(0));
        return basePk;
    }

    private static List<Integer> basePksOf(LookupResult result) {
        List<Integer> basePks = new ArrayList<>();
        for (InternalRow mainRow : result.getRowList()) {
            basePks.add(mainRow.getInt(0));
        }
        return basePks;
    }
}
