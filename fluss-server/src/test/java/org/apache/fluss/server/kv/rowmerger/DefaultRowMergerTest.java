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

import org.apache.fluss.metadata.DeleteBehavior;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.ProjectedRow;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;

import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DefaultRowMerger} delete behavior functionality. */
class DefaultRowMergerTest {

    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("name", DataTypes.STRING())
                    .primaryKey("id")
                    .build();

    private static final Schema SCHEMA_2 =
            Schema.newBuilder()
                    .fromColumns(
                            Arrays.asList(
                                    new Schema.Column("age", DataTypes.STRING(), null, (short) 2),
                                    new Schema.Column("id", DataTypes.INT(), null, (short) 0),
                                    new Schema.Column("name", DataTypes.STRING(), null, (short) 1)))
                    .primaryKey("id")
                    .build();

    private BinaryRow createBinaryRow(int id, String name) {
        return compactedRow(SCHEMA.getRowType(), new Object[] {id, name});
    }

    private BinaryRow createBinaryRow(String age, int id, String name) {
        return compactedRow(SCHEMA_2.getRowType(), new Object[] {age, id, name});
    }

    @ParameterizedTest
    @EnumSource(DeleteBehavior.class)
    void testDefaultRowMerger(DeleteBehavior deleteBehavior) {
        DefaultRowMerger merger = new DefaultRowMerger(KvFormat.COMPACTED, deleteBehavior);
        merger.configureTargetColumns(null, (byte) 1, SCHEMA);

        InternalRow oldRow = createBinaryRow(1, "old");
        BinaryRow newRow = createBinaryRow(1, "new");

        // Test merge operation - should return new row
        InternalRow mergedRow = merger.merge(oldRow, newRow);
        assertThat(mergedRow).isSameAs(newRow);

        // Test delete operation - should return null (deleted)
        InternalRow deletedRow = merger.delete(oldRow);
        assertThat(deletedRow).isNull();

        // Test supportsDelete - should return true
        assertThat(merger.deleteBehavior()).isEqualTo(deleteBehavior);

        // Test schema change.
        merger.configureTargetColumns(null, (byte) 2, SCHEMA_2);
        oldRow = ProjectedRow.from(SCHEMA, SCHEMA_2).replaceRow(oldRow);
        newRow = createBinaryRow("20", 1, "new2");
        assertThat(merger.merge(oldRow, newRow)).isSameAs(newRow);
        assertThat(merger.delete(newRow)).isNull();
    }

    @ParameterizedTest
    @EnumSource(DeleteBehavior.class)
    void testPartialUpdateRowMergerDeleteBehavior(DeleteBehavior deleteBehavior) {
        DefaultRowMerger merger = new DefaultRowMerger(KvFormat.COMPACTED, deleteBehavior);

        // Configure for partial update (only name column)
        RowMerger partialMerger =
                merger.configureTargetColumns(new int[] {0, 1}, (byte) 1, SCHEMA); // id + name

        InternalRow oldRow = createBinaryRow(1, "old");

        InternalRow ignoredRow = partialMerger.delete(oldRow);
        assertThat(ignoredRow).isNull();
        assertThat(partialMerger.deleteBehavior()).isEqualTo(deleteBehavior);

        assertThat(partialMerger.merge(null, oldRow)).isEqualTo(oldRow);

        // schema change then partial update (except name column).
        partialMerger = merger.configureTargetColumns(new int[] {0, 1}, (byte) 2, SCHEMA_2);
        oldRow = ProjectedRow.from(SCHEMA, SCHEMA_2).replaceRow(oldRow);
        BinaryRow newRow = createBinaryRow("20", 1, null);
        BinaryRow mergeRow = createBinaryRow("20", 1, "old");
        assertThat(partialMerger.merge(oldRow, newRow)).isEqualTo(mergeRow);
        assertThat(partialMerger.delete(mergeRow)).isEqualTo(createBinaryRow(null, 1, "old"));
    }
}
