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
import org.apache.fluss.record.BinaryValue;
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
                                    new Schema.Column("id", DataTypes.INT(), null, (short) 0),
                                    new Schema.Column("name", DataTypes.STRING(), null, (short) 1),
                                    // add new column at end
                                    new Schema.Column("age", DataTypes.STRING(), null, (short) 2)))
                    .primaryKey("id")
                    .build();

    private BinaryValue createBinaryValue(int id, String name) {
        return new BinaryValue(
                (short) 1, compactedRow(SCHEMA.getRowType(), new Object[] {id, name}));
    }

    private BinaryValue createBinaryValue(int id, String name, String age) {
        return new BinaryValue(
                (short) 2, compactedRow(SCHEMA_2.getRowType(), new Object[] {id, name, age}));
    }

    @ParameterizedTest
    @EnumSource(DeleteBehavior.class)
    void testDefaultRowMerger(DeleteBehavior deleteBehavior) {
        DefaultRowMerger merger = new DefaultRowMerger(KvFormat.COMPACTED, deleteBehavior);
        merger.configureTargetColumns(null, (byte) 1, SCHEMA);

        BinaryValue oldValue = createBinaryValue(1, "old");
        BinaryValue newValue = createBinaryValue(1, "new");

        // Test merge operation - should return new row
        BinaryValue mergedValue = merger.merge(oldValue, newValue);
        assertThat(mergedValue).isSameAs(newValue);

        // Test delete operation - should return null (deleted)
        BinaryValue deletedValue = merger.delete(oldValue);
        assertThat(deletedValue).isNull();

        // Test supportsDelete - should return true
        assertThat(merger.deleteBehavior()).isEqualTo(deleteBehavior);

        // Test schema change.
        merger.configureTargetColumns(null, (byte) 2, SCHEMA_2);
        newValue = createBinaryValue(1, "new2", "20");
        assertThat(merger.merge(oldValue, newValue)).isSameAs(newValue);
        assertThat(merger.delete(newValue)).isNull();
    }

    @ParameterizedTest
    @EnumSource(DeleteBehavior.class)
    void testPartialUpdateRowMergerDeleteBehavior(DeleteBehavior deleteBehavior) {
        DefaultRowMerger merger = new DefaultRowMerger(KvFormat.COMPACTED, deleteBehavior);

        // Configure for partial update (only name column)
        RowMerger partialMerger =
                merger.configureTargetColumns(new int[] {0, 1}, (byte) 1, SCHEMA); // id + name

        BinaryValue oldValue = createBinaryValue(1, "old");

        BinaryValue ignoredValue = partialMerger.delete(oldValue);
        assertThat(ignoredValue).isNull();
        assertThat(partialMerger.deleteBehavior()).isEqualTo(deleteBehavior);

        assertThat(partialMerger.merge(null, oldValue)).isEqualTo(oldValue);

        // schema change then partial update (except name column).
        partialMerger = merger.configureTargetColumns(new int[] {0, 2}, (byte) 2, SCHEMA_2);
        BinaryValue newValue = createBinaryValue(1, null, "20");
        BinaryValue mergeValue = createBinaryValue(1, "old", "20");
        assertThat(partialMerger.merge(oldValue, newValue)).isEqualTo(mergeValue);
        assertThat(partialMerger.delete(mergeValue)).isEqualTo(createBinaryValue(1, "old", null));
    }
}
