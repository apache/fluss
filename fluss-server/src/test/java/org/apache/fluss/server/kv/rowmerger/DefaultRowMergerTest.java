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
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

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

    private static final RowType ROW_TYPE = SCHEMA.getRowType();

    private BinaryRow createBinaryRow(int id, String name) {
        return compactedRow(ROW_TYPE, new Object[] {id, name});
    }

    @ParameterizedTest
    @EnumSource(DeleteBehavior.class)
    void testDefaultRowMerger(DeleteBehavior deleteBehavior) {
        DefaultRowMerger merger = new DefaultRowMerger(SCHEMA, KvFormat.COMPACTED, deleteBehavior);

        BinaryRow oldRow = createBinaryRow(1, "old");
        BinaryRow newRow = createBinaryRow(1, "new");

        // Test merge operation - should return new row
        BinaryRow mergedRow = merger.merge(oldRow, newRow);
        assertThat(mergedRow).isSameAs(newRow);

        // Test delete operation - should return null (deleted)
        BinaryRow deletedRow = merger.delete(oldRow);
        assertThat(deletedRow).isNull();

        // Test supportsDelete - should return true
        assertThat(merger.deleteBehavior()).isEqualTo(deleteBehavior);
    }

    @ParameterizedTest
    @EnumSource(DeleteBehavior.class)
    void testPartialUpdateRowMergerDeleteBehavior(DeleteBehavior deleteBehavior) {
        DefaultRowMerger merger = new DefaultRowMerger(SCHEMA, KvFormat.COMPACTED, deleteBehavior);

        // Configure for partial update (only name column)
        RowMerger partialMerger = merger.configureTargetColumns(new int[] {0, 1}); // id + name

        BinaryRow oldRow = createBinaryRow(1, "old");

        BinaryRow ignoredRow = partialMerger.delete(oldRow);
        assertThat(ignoredRow).isNull();
        assertThat(partialMerger.deleteBehavior()).isEqualTo(deleteBehavior);
    }
}
