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

package org.apache.fluss.server.kv;

import org.apache.fluss.metadata.Schema;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TargetColumns}. */
class TargetColumnsTest {

    private static final Schema TWO_COL_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("name", DataTypes.STRING())
                    .primaryKey("id")
                    .build();

    @Test
    void testTargetColumns() {
        assertThat(TargetColumns.specifiesAllSchemaFieldIndexes(TWO_COL_SCHEMA, new int[] {0}))
                .isFalse();

        assertThat(TargetColumns.specifiesAllSchemaFieldIndexes(TWO_COL_SCHEMA, new int[] {0, 1}))
                .isTrue();
        assertThat(TargetColumns.specifiesAllSchemaFieldIndexes(TWO_COL_SCHEMA, new int[] {1, 0}))
                .isTrue();
        assertThat(
                        TargetColumns.specifiesAllSchemaFieldIndexes(
                                TWO_COL_SCHEMA, new int[] {0, 1, 0}))
                .isTrue();

        assertThat(TargetColumns.specifiesAllSchemaFieldIndexes(TWO_COL_SCHEMA, new int[] {0, 2}))
                .isFalse();

        assertThatThrownBy(() -> TargetColumns.specifiesAllSchemaFieldIndexes(TWO_COL_SCHEMA, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("targetColumns");

        Schema threeCols =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.INT())
                        .column("c", DataTypes.INT())
                        .primaryKey("a")
                        .build();
        assertThat(TargetColumns.specifiesAllSchemaFieldIndexes(threeCols, new int[] {0, 1}))
                .isFalse();
        assertThat(TargetColumns.specifiesAllSchemaFieldIndexes(threeCols, new int[] {2, 0, 1, 2}))
                .isTrue();

        assertThatThrownBy(() -> TargetColumns.specifiesAllSchemaFieldIndexes(null, new int[] {0}))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("schema");
    }
}
