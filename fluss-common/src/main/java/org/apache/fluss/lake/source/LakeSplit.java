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

package org.apache.fluss.lake.source;

import org.apache.fluss.annotation.PublicEvolving;

import java.util.List;

/**
 * Represents a logical partition or segment of data in data-lake.
 *
 * @since 0.8
 */
@PublicEvolving
public interface LakeSplit {

    /**
     * Returns the bucket id for this data split. Any data split in lake must belong to a Fluss
     * bucket. The bucket id is used to aggregate splits that in same Fluss bucket into same reader
     * in bucket-aware table, such primary key table, log table with pre-defined bucket keys. If
     * it's not bucket-aware table, it's also feasible to return -1 directly for all data splits.
     *
     * @return the bucket id
     */
    int bucket();

    /**
     * Returns the hierarchical partition values for this split, or an empty list if the split
     * doesn't belong to a specific partition in non-partitioned table.
     *
     * <p>The returned list represents the complete partition path, with each element corresponding
     * to one level of the partitioning hierarchy in order. For example, in a table partitioned by
     * {@code dt=20250101/hr=12}, this method would return {@code ["20250101", "12"]}.
     *
     * <p>The list size should match the table's partition column count, and each element's position
     * corresponds to the declared partition column order. Values should be in their
     * string-represented form as they would appear in the filesystem path.
     *
     * @return the resolved partition values specification, or {@code null} if this split doesn't
     *     belong to a specific partition in non-partitioned table.
     */
    List<String> partition();
}
