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

package org.apache.fluss.lake.hudi.utils;

import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

import static org.apache.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link HudiUtils#isLegacyTable(RowType)}. */
class HudiUtilsTest {

    @Test
    void testCleanTableHasNoSystemColumns() {
        RowType clean =
                RowType.of(
                        new LogicalType[] {new IntType(), new VarCharType()},
                        new String[] {"id", "name"});
        assertThat(HudiUtils.isLegacyTable(clean)).isFalse();
    }

    @Test
    void testLegacyTableWithAllThreeSystemColumns() {
        RowType legacy =
                RowType.of(
                        new LogicalType[] {
                            new IntType(),
                            new VarCharType(),
                            new IntType(),
                            new BigIntType(),
                            new TimestampType(6)
                        },
                        new String[] {
                            "id",
                            "name",
                            BUCKET_COLUMN_NAME,
                            OFFSET_COLUMN_NAME,
                            TIMESTAMP_COLUMN_NAME
                        });
        assertThat(HudiUtils.isLegacyTable(legacy)).isTrue();
    }

    @Test
    void testTableWithOnlyTimestampNameIsClean() {
        // A user table that merely reuses the __timestamp name (but is not a full Fluss legacy
        // layout) must be treated as clean, not misdetected as legacy.
        RowType oneMatch =
                RowType.of(
                        new LogicalType[] {new IntType(), new TimestampType(6)},
                        new String[] {"id", TIMESTAMP_COLUMN_NAME});
        assertThat(HudiUtils.isLegacyTable(oneMatch)).isFalse();
    }

    @Test
    void testTableWithTwoSystemColumnNamesIsClean() {
        RowType twoMatch =
                RowType.of(
                        new LogicalType[] {new IntType(), new BigIntType(), new TimestampType(6)},
                        new String[] {"id", OFFSET_COLUMN_NAME, TIMESTAMP_COLUMN_NAME});
        assertThat(HudiUtils.isLegacyTable(twoMatch)).isFalse();
    }
}
