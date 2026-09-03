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

package org.apache.fluss.server.kv.rowmerger;

import org.apache.fluss.metadata.Schema;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.util.BitSet;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for how {@link SequenceGroups} arbitrates the groups declared on a schema. */
class SequenceGroupsTest {

    /**
     * {@code a} is ordered by {@code g1} and {@code b} by {@code g2}, so the groups are disjoint.
     */
    private static final Schema TWO_GROUPS =
            Schema.newBuilder()
                    .column("k", DataTypes.INT())
                    .column("a", DataTypes.STRING())
                    .column("g1", DataTypes.INT())
                    .column("b", DataTypes.STRING())
                    .column("g2", DataTypes.INT())
                    .sequenceGroup(singletonList("g1"), singletonList("a"))
                    .sequenceGroup(singletonList("g2"), singletonList("b"))
                    .primaryKey("k")
                    .build();

    private static final int A = 1;
    private static final int G1 = 2;
    private static final int B = 3;
    private static final int G2 = 4;

    private static InternalRow twoGroupsRow(@Nullable Integer g1, @Nullable Integer g2) {
        return compactedRow(TWO_GROUPS.getRowType(), new Object[] {1, "a", g1, "b", g2});
    }

    @Test
    void testEachGroupIsArbitratedOnItsOwn() {
        SequenceGroups groups = SequenceGroups.create(TWO_GROUPS);

        // the first group moves forward while the second falls behind
        boolean[] acceptance =
                groups.resolveAcceptance(twoGroupsRow(100, 100), twoGroupsRow(101, 99));
        assertThat(acceptance[A]).isTrue();
        assertThat(acceptance[B]).isFalse();
        // a sequence column follows the group it orders, so that its value stays in step with the
        // columns arbitrated by it
        assertThat(acceptance[G1]).isTrue();
        assertThat(acceptance[G2]).isFalse();
        // the primary key takes part in no group and is never held back
        assertThat(acceptance[0]).isTrue();
    }

    @Test
    void testGroupAdvancesOnAnEqualSequenceButNotWithoutOne() {
        SequenceGroups groups = SequenceGroups.create(TWO_GROUPS);

        // a replayed record still refreshes the group
        assertThat(groups.resolveAcceptance(twoGroupsRow(100, 100), twoGroupsRow(100, 100)))
                .containsOnly(true);

        // the incoming group carries no order information at all, so its values are dropped even
        // though there is no stored row to compare against
        boolean[] acceptance = groups.resolveAcceptance(null, twoGroupsRow(null, 1));
        assertThat(acceptance[A]).isFalse();
        assertThat(acceptance[B]).isTrue();
    }

    @Test
    void testGroupResolutionIsIndependentOfTheDeclarationShape() {
        // the sequence column is declared before the column it orders
        Schema sequenceFirst =
                Schema.newBuilder()
                        .column("k", DataTypes.INT())
                        .column("g", DataTypes.INT())
                        .column("a", DataTypes.STRING())
                        .sequenceGroup(singletonList("g"), singletonList("a"))
                        .primaryKey("k")
                        .build();
        InternalRow storedFirst =
                compactedRow(sequenceFirst.getRowType(), new Object[] {1, 100, "a"});
        InternalRow incomingFirst =
                compactedRow(sequenceFirst.getRowType(), new Object[] {1, 99, "a"});
        // both the sequence column and the column it orders are held back together
        assertThat(
                        SequenceGroups.create(sequenceFirst)
                                .resolveAcceptance(storedFirst, incomingFirst))
                .containsExactly(true, false, false);

        // two columns naming the same sequence column advance as one group
        Schema shared =
                Schema.newBuilder()
                        .column("k", DataTypes.INT())
                        .column("a", DataTypes.STRING())
                        .column("b", DataTypes.STRING())
                        .column("g", DataTypes.INT())
                        .sequenceGroup(singletonList("g"), asList("a", "b"))
                        .primaryKey("k")
                        .build();
        InternalRow storedShared =
                compactedRow(shared.getRowType(), new Object[] {1, "a", "b", 100});
        InternalRow incomingShared =
                compactedRow(shared.getRowType(), new Object[] {1, "a", "b", 99});
        assertThat(SequenceGroups.create(shared).resolveAcceptance(storedShared, incomingShared))
                .containsExactly(true, false, false, false);
    }

    @Test
    void testMissingSequenceColumnInAShorterRowIsTheOldest() {
        // a row written under an older schema carries fewer fields, so the sequence column is
        // absent
        Schema olderSchema =
                Schema.newBuilder()
                        .column("k", DataTypes.INT())
                        .column("a", DataTypes.STRING())
                        .primaryKey("k")
                        .build();
        SequenceGroups groups = SequenceGroups.create(TWO_GROUPS);

        InternalRow shortRow = compactedRow(olderSchema.getRowType(), new Object[] {1, "a"});
        assertThat(groups.resolveAcceptance(shortRow, twoGroupsRow(1, 1))).containsOnly(true);
    }

    // ---------------------------------------------------------------------------------------------
    // composite sequence keys
    // ---------------------------------------------------------------------------------------------

    /** {@code a} is ordered by {@code g1} and {@code g2} together, compared in that order. */
    private static final Schema COMPOSITE =
            Schema.newBuilder()
                    .column("k", DataTypes.INT())
                    .column("a", DataTypes.STRING())
                    .column("g1", DataTypes.INT())
                    .column("g2", DataTypes.INT())
                    .sequenceGroup(asList("g1", "g2"), singletonList("a"))
                    .primaryKey("k")
                    .build();

    private static InternalRow compositeRow(@Nullable Integer g1, @Nullable Integer g2) {
        return compactedRow(COMPOSITE.getRowType(), new Object[] {1, "a", g1, g2});
    }

    private static boolean compositeAdvances(
            @Nullable Integer storedG1,
            @Nullable Integer storedG2,
            @Nullable Integer incomingG1,
            @Nullable Integer incomingG2) {
        return SequenceGroups.create(COMPOSITE)
                .resolveAcceptance(
                        compositeRow(storedG1, storedG2), compositeRow(incomingG1, incomingG2))[A];
    }

    @Test
    void testCompositeKeyComparesInTheDeclaredOrder() {
        // the leading column decides on its own, whatever the trailing one says
        assertThat(compositeAdvances(5, 100, 6, 1)).isTrue();
        assertThat(compositeAdvances(5, 100, 4, 999)).isFalse();
        // the leading columns tie, so the next one decides
        assertThat(compositeAdvances(5, 100, 5, 101)).isTrue();
        assertThat(compositeAdvances(5, 100, 5, 99)).isFalse();
        // every column ties, which still advances the group
        assertThat(compositeAdvances(5, 100, 5, 100)).isTrue();
    }

    @Test
    void testCompositeKeyTreatsNullAsTheOldest() {
        assertThat(compositeAdvances(null, 100, 1, 1)).isTrue();
        assertThat(compositeAdvances(1, 1, null, 999)).isFalse();
        // null equals null, so the leading column decides nothing and the trailing one arbitrates
        assertThat(compositeAdvances(null, 100, null, 101)).isTrue();
        assertThat(compositeAdvances(null, 100, null, 99)).isFalse();
        // the group is dropped only when the incoming row carries no order information at all
        assertThat(compositeAdvances(5, 100, null, null)).isFalse();
        assertThat(compositeAdvances(null, null, null, 1)).isTrue();
    }

    // ---------------------------------------------------------------------------------------------
    // sequence column types
    // ---------------------------------------------------------------------------------------------

    private static Stream<Object[]> supportedSequenceTypes() {
        return Stream.of(
                new Object[] {DataTypes.INT(), 101, 100},
                new Object[] {DataTypes.BIGINT(), 101L, 100L},
                new Object[] {
                    DataTypes.TIMESTAMP(),
                    org.apache.fluss.row.TimestampNtz.fromMillis(101),
                    org.apache.fluss.row.TimestampNtz.fromMillis(100)
                },
                new Object[] {
                    DataTypes.TIMESTAMP_LTZ(),
                    org.apache.fluss.row.TimestampLtz.fromEpochMillis(101),
                    org.apache.fluss.row.TimestampLtz.fromEpochMillis(100)
                });
    }

    @ParameterizedTest
    @MethodSource("supportedSequenceTypes")
    void testSupportedSequenceColumnTypesOrderTheirGroup(
            DataType sequenceType, Object newer, Object older) {
        Schema schema =
                Schema.newBuilder()
                        .column("k", DataTypes.INT())
                        .column("a", DataTypes.STRING())
                        .column("g", sequenceType)
                        .sequenceGroup(singletonList("g"), singletonList("a"))
                        .primaryKey("k")
                        .build();
        SequenceGroups groups = SequenceGroups.create(schema);

        InternalRow stored = compactedRow(schema.getRowType(), new Object[] {1, "a", older});
        InternalRow newerRow = compactedRow(schema.getRowType(), new Object[] {1, "a", newer});
        InternalRow withoutSequence =
                compactedRow(schema.getRowType(), new Object[] {1, "a", null});

        assertThat(groups.resolveAcceptance(stored, newerRow)[A]).isTrue();
        assertThat(groups.resolveAcceptance(newerRow, stored)[A]).isFalse();
        assertThat(groups.resolveAcceptance(stored, withoutSequence)[A]).isFalse();
    }

    @Test
    void testRestrictToLeavesUncoveredGroupsOutOfTheArbitration() {
        BitSet coveringFirstGroup = new BitSet();
        coveringFirstGroup.set(0); // k
        coveringFirstGroup.set(A);
        coveringFirstGroup.set(G1);
        SequenceGroups restricted =
                SequenceGroups.create(TWO_GROUPS).restrictTo(coveringFirstGroup);

        // both groups fall behind, yet only the covered one still holds its fields back
        boolean[] acceptance =
                restricted.resolveAcceptance(twoGroupsRow(100, 100), twoGroupsRow(99, 99));
        assertThat(acceptance).containsExactly(true, false, false, true, true);

        // the uncovered group no longer arbitrates b or g2, so a null sequence cannot hold them
        // back
        assertThat(restricted.resolveAcceptance(twoGroupsRow(100, 100), twoGroupsRow(101, null)))
                .containsExactly(true, true, true, true, true);
    }
}
