/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

import com.alibaba.fluss.types.RowType;

import java.util.Arrays;

import static com.alibaba.fluss.utils.Preconditions.checkState;

/**
 * {@link Projection} represents a list of indexes that can be used to project data types. A row
 * projection includes both reducing the accessible fields and reordering them. Currently, this only
 * supports top-level projection. Nested projection will be supported in the future.
 *
 * <p>For example, given a row with fields [a, b, c, d, e], a projection [2, 0, 3] will project the
 * row to [c, a, d]. The projection indexes are 0-based.
 *
 * <ul>
 *   <li>The {@link #projection} indexes will be [2, 0, 3]
 *   <li>The {@link #projectionInOrder} indexes will be [0, 2, 3]
 *   <li>The {@link #reorderingIndexes} indexes will be [1, 0, 2]
 * </ul>
 *
 * <p>That means <code>projection[i] = projectionInOrder[reorderingIndexes[i]]</code>
 */
public class Projection {
    /** the projection indexes including both selected fields and reordering them. */
    final int[] projection;
    /** the projection indexes that only select fields but not reordering them. */
    final int[] projectionInOrder;
    /** the indexes to reorder the fields of {@link #projectionInOrder} to {@link #projection}. */
    final int[] reorderingIndexes;
    /** the flag to indicate whether reordering is needed. */
    final boolean reorderingNeeded;

    /** Create a {@link Projection} of the provided {@code indexes}. */
    public static Projection of(int[] indexes) {
        return new Projection(indexes);
    }

    private Projection(int[] projection) {
        this.projection = projection;
        this.projectionInOrder = Arrays.copyOf(projection, projection.length);
        Arrays.sort(projectionInOrder);
        this.reorderingNeeded = !Arrays.equals(projection, projectionInOrder);
        this.reorderingIndexes = new int[projection.length];
        for (int i = 0; i < projection.length; i++) {
            int index = Arrays.binarySearch(projectionInOrder, projection[i]);
            checkState(index >= 0, "The projection index is invalid.");
            reorderingIndexes[i] = index;
        }
    }

    public RowType projectInOrder(RowType rowType) {
        return rowType.project(projectionInOrder);
    }

    public int[] getProjection() {
        return projection;
    }

    public int[] getProjectionInOrder() {
        return projectionInOrder;
    }

    public boolean isReorderingNeeded() {
        return reorderingNeeded;
    }

    public int[] getReorderingIndexes() {
        return reorderingIndexes;
    }
}
