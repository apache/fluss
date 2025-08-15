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

package com.alibaba.fluss.flink.source.enumerator;

import com.alibaba.fluss.metadata.TableBucket;

/** An assigner for table bucket to assign a table bucket to a subtask. */
public class TableBucketAssigner {

    /**
     * Returns the index of the target subtask that a specific table bucket should be assigned to.
     *
     * <p>The resulting distribution of table buckets of a single table has the following contract:
     *
     * <ul>
     *   <li>1. Splits in same bucket are assigned to same subtask
     *   <li>2. Uniformly distributed across subtasks
     *   <li>3. For partitioned table, the buckets in same partition are round-robin distributed
     *       (strictly clockwise w.r.t. ascending subtask indices) by using the partition id as the
     *       offset from a starting index. The starting index is the index of the subtask which
     *       bucket 0 of the partition will be assigned to, determined using the partition id to
     *       make sure the partitions' buckets of a table are distributed uniformly
     * </ul>
     *
     * @param tableBucket the bucket to assign.
     * @param subTasks the subTasks
     * @return the id of the subtask that owns the bucket.
     */
    public static int assignTableBucket(TableBucket tableBucket, int subTasks) {
        int startIndex =
                tableBucket.getPartitionId() == null
                        ? 0
                        : ((tableBucket.getPartitionId().hashCode() * 31) & 0x7FFFFFFF) % subTasks;

        return (startIndex + tableBucket.getBucket()) % subTasks;
    }
}
