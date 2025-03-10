/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.utils;

import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.ProjectedRow;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link com.alibaba.fluss.utils.Projection}. */
class ProjectionTest {

    @Test
    void testProjection() {
        Projection projection = Projection.of(new int[] {2, 0, 3});
        assertThat(projection.getProjectionInOrder()).isEqualTo(new int[] {0, 2, 3});

        RowType rowType =
                projection.projectInOrder(
                        DataTypes.ROW(
                                DataTypes.FIELD("f0", DataTypes.INT()),
                                DataTypes.FIELD("f1", DataTypes.BIGINT()),
                                DataTypes.FIELD("f2", DataTypes.STRING()),
                                DataTypes.FIELD("f3", DataTypes.DOUBLE())));
        assertThat(rowType)
                .isEqualTo(
                        DataTypes.ROW(
                                DataTypes.FIELD("f0", DataTypes.INT()),
                                DataTypes.FIELD("f2", DataTypes.STRING()),
                                DataTypes.FIELD("f3", DataTypes.DOUBLE())));

        assertThat(projection.isReorderingNeeded()).isTrue();
        assertThat(projection.getReorderingIndexes()).isEqualTo(new int[] {1, 0, 2});
        assertThat(rowType.project(projection.getReorderingIndexes()))
                .isEqualTo(
                        DataTypes.ROW(
                                DataTypes.FIELD("f2", DataTypes.STRING()),
                                DataTypes.FIELD("f0", DataTypes.INT()),
                                DataTypes.FIELD("f3", DataTypes.DOUBLE())));

        GenericRow row = GenericRow.of(0, 1L, BinaryString.fromString("2"), 3.0d);
        ProjectedRow p1 = ProjectedRow.from(projection.getProjectionInOrder());
        p1.replaceRow(row);
        ProjectedRow p2 = ProjectedRow.from(projection.getReorderingIndexes());
        p2.replaceRow(p1);
        assertThat(p2.getString(0).toString()).isEqualTo("2");
        assertThat(p2.getInt(1)).isEqualTo(0);
        assertThat(p2.getDouble(2)).isEqualTo(3.0d);
    }
}
