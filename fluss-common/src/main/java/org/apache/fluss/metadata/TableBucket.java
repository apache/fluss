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

package org.apache.fluss.metadata;

import org.apache.fluss.annotation.PublicEvolving;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

/**
 * A class to identify a table bucket, containing:
 *
 * <ul>
 *   <li>the table id
 *   <li>the bucket num
 *   <li>the partition id of the table bucket. if the table bucket doesn't belong to a partition,
 *       this field will be null
 * </ul>
 *
 * @since 0.1
 */
@PublicEvolving
public class TableBucket implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long tableId;

    private final int bucket;

    // will be null if the bucket doesn't belong to a partition
    private final @Nullable Long partitionId;

    // Cache hashCode as it is called in performance sensitive parts of the code (e.g.
    // RecordAccumulator.ready)
    private Integer hash;

    public TableBucket(long tableId, int bucket) {
        this(tableId, null, bucket);
    }

    public TableBucket(long tableId, @Nullable Long partitionId, int bucket) {
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.bucket = bucket;
    }

    public int getBucket() {
        return bucket;
    }

    public long getTableId() {
        return tableId;
    }

    @Nullable
    public Long getPartitionId() {
        return partitionId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableBucket that = (TableBucket) o;
        return tableId == that.tableId
                && bucket == that.bucket
                && Objects.equals(partitionId, that.partitionId);
    }

    @Override
    public int hashCode() {
        Integer h = this.hash;
        if (h == null) {
            int result = Objects.hash(tableId, bucket, partitionId);
            this.hash = result;
            return result;
        } else {
            return h;
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("TableBucket{tableId=");
        builder.append(tableId);
        if (partitionId == null) {
            builder.append(", bucket=").append(bucket);
        } else {
            builder.append(", partitionId=").append(partitionId).append(", bucket=").append(bucket);
        }
        builder.append('}');
        return builder.toString();
    }
}
