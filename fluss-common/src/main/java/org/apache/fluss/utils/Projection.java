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

package org.apache.fluss.utils;

import org.apache.fluss.metadata.Schema;
import org.apache.fluss.types.RowType;

import java.util.Arrays;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * {@link Projection} represents a list of indexes that can be used to project data types. A row
 * projection includes both reducing the accessible fields and reordering them. Currently, this only
 * supports top-level projection. Nested projection will be supported in the future.
 *
 * <p>For example, given a row with fields [a, b, c, d, e] with id [0, 1, 3, 4, 5], a projection [2,
 * 0, 3] will project the row to [c, a, d] with id [3, 0, 4]. The projection indexes are 0-based.
 *
 * <ul>
 *   <li>The {@link #projectionPositions} indexes will be [2, 0, 3]
 *   <li>The {@link #projectionPositions} id will be [3, 0, 4]
 *   <li>The {@link #projectionIdsInOrder} indexes will be [0, 3, 4]
 *   <li>The {@link #projectionPositionsOrderById} indexes will be [0, 2, 3]
 *   <li>The {@link #reorderingIndexes} indexes will be [1, 0, 2]
 * </ul>
 *
 * <p>That means <code>projection[i] = projectionInOrder[reorderingIndexes[i]]</code>
 */
public class Projection {
    /** the projection indexes including both selected fields and reordering them. */
    final int[] projectionPositions;
    /** the projection indexes that only select fields but not reordering them. */
    final int[] projectionPositionsOrderById;
    /** the projection indexes that only select fields but not reordering them. */
    final int[] projectionIdsInOrder;
    /**
     * the indexes to reorder the fields of {@link #projectionPositionsOrderById} to {@link
     * #projectionPositions}.
     */
    final int[] reorderingIndexes;
    /** the flag to indicate whether reordering is needed. */
    final boolean reorderingNeeded;

    public static Projection of(int[] indexes, Schema schema) {
        int[] ids = new int[indexes.length];
        List<Integer> columnIds = schema.getColumnIds();
        for (int i = 0; i < indexes.length; i++) {
            ids[i] = columnIds.get(indexes[i]);
        }

        return new Projection(indexes, ids);
    }

    /**
     * Create a {@link Projection} of the provided {@code indexes} and {@code projectionIds}.
     *
     * <p>Typically, {@code projectionIndexes} and {@code projectionIds} are the same. But when
     * removing a middle column or reordering columns, they won't be the same. In this case, when
     * the schema on the fluss server side differs from the schema on the client side during user
     * queries, using {@code projectionIds} ensures correctness by mapping to the actual server-side
     * column positions.
     *
     * @param projectionIndexes the indexes of the projection, which is used for user to read data
     *     from client side. These indexes are based on the schema visible to the client side and
     *     are used to parse the data returned by the fluss server.
     * @param projectionIds the ids of the projection, which is used for fluss server scan. These
     *     ids are based on the actual schema on the fluss server side and are pushed down to the
     *     server for querying.
     */
    private Projection(int[] projectionIndexes, int[] projectionIds) {
        checkState(
                projectionIds.length == projectionIndexes.length,
                "The number of ids must be equal to the number of indexes.");
        this.projectionPositions = projectionIndexes;

        // reorder the projection id for lookup.
        this.projectionIdsInOrder = Arrays.copyOf(projectionIds, projectionIds.length);
        Arrays.sort(projectionIdsInOrder);
        this.reorderingNeeded = !Arrays.equals(projectionIds, projectionIdsInOrder);
        this.reorderingIndexes = new int[projectionPositions.length];
        this.projectionPositionsOrderById = new int[projectionPositions.length];
        for (int i = 0; i < projectionIds.length; i++) {
            int index = Arrays.binarySearch(projectionIdsInOrder, projectionIds[i]);
            checkState(index >= 0, "The projection index is invalid.");
            reorderingIndexes[i] = index;
            projectionPositionsOrderById[i] = projectionIndexes[index];
        }
    }

    public RowType projectInOrder(RowType rowType) {
        return rowType.project(projectionPositionsOrderById);
    }

    public int[] getProjectionPositions() {
        return projectionPositions;
    }

    /**
     * The id of the projection, which is used for fluss server scan.
     *
     * @return
     */
    public int[] getProjectionIdInOrder() {
        return projectionIdsInOrder;
    }

    public boolean isReorderingNeeded() {
        return reorderingNeeded;
    }

    public int[] getReorderingIndexes() {
        return reorderingIndexes;
    }
}
