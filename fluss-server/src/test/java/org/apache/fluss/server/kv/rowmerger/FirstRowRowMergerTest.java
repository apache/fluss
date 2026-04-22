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
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FirstRowRowMerger}. */
class FirstRowRowMergerTest {

    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("name", DataTypes.STRING())
                    .primaryKey("id")
                    .build();

    private static final RowType ROW_TYPE = SCHEMA.getRowType();

    private static BinaryValue binaryValue(Object[] objects) {
        return new BinaryValue((short) 1, compactedRow(ROW_TYPE, objects));
    }

    @Test
    void testMergeWithNullOldValueReturnsNewValue() {
        FirstRowRowMerger merger = new FirstRowRowMerger(DeleteBehavior.DISABLE);

        BinaryValue newValue = binaryValue(new Object[] {1, "first"});
        BinaryValue result = merger.merge(null, newValue);
        assertThat(result).isSameAs(newValue);
    }

    @Test
    void testMergeRetainsFirstRow() {
        FirstRowRowMerger merger = new FirstRowRowMerger(DeleteBehavior.DISABLE);

        BinaryValue oldValue = binaryValue(new Object[] {1, "first"});
        BinaryValue newValue = binaryValue(new Object[] {1, "second"});
        BinaryValue result = merger.merge(oldValue, newValue);
        assertThat(result).isSameAs(oldValue);
    }
}
