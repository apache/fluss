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

package org.apache.fluss.flink.adapter;

import org.apache.flink.table.data.RowData;

import java.io.Serializable;
import java.util.Optional;

/**
 * Version-neutral adapter for Flink's lookup custom shuffle ability.
 *
 * <p>The Flink 2.x implementation of this interface extends {@code SupportsLookupCustomShuffle};
 * lower Flink versions use this interface only as an internal connector contract.
 *
 * <p>TODO: remove this adapter when no longer supporting Flink 1.x.
 */
public interface SupportsLookupCustomShuffleAdapter {

    /**
     * Returns the custom partitioner prepared for the current lookup, or empty to keep the probe
     * stream in its original distribution.
     */
    Optional<InputDataPartitionerAdapter> getPartitionerAdapter();

    /** Version-neutral custom partitioner for lookup join keys. */
    interface InputDataPartitionerAdapter extends Serializable {

        /**
         * Returns the target partition for the normalized lookup keys.
         *
         * @param joinKeys lookup keys extracted from the probe row
         * @param numPartitions number of lookup join subtasks
         */
        int partition(RowData joinKeys, int numPartitions);

        /** Returns whether the partitioning result is deterministic for the same lookup keys. */
        default boolean isDeterministic() {
            return true;
        }
    }
}
