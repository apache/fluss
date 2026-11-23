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

import java.io.Serializable;
import java.util.Objects;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * An exclusive log position in one physical bucket. The bucket identity is local to a Fluss cluster
 * and includes the partition ID for a partitioned table.
 */
@PublicEvolving
public final class BucketLogOffset implements Serializable {
    private static final long serialVersionUID = 1L;

    private final TableBucket tableBucket;
    private final long offset;

    private BucketLogOffset(TableBucket tableBucket, long offset) {
        this.tableBucket = Objects.requireNonNull(tableBucket);
        checkArgument(offset >= 0, "The next offset must be non-negative.");
        this.offset = offset;
    }

    /** Creates an exclusive read position for a specific bucket. */
    public static BucketLogOffset of(TableBucket tableBucket, long offset) {
        return new BucketLogOffset(tableBucket, offset);
    }

    /** Returns the full bucket identity in which the offset has meaning. */
    public TableBucket getTableBucket() {
        return tableBucket;
    }

    /** Returns the first offset excluded by this read. */
    public long getOffset() {
        return offset;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof BucketLogOffset)) {
            return false;
        }
        BucketLogOffset position = (BucketLogOffset) other;
        return offset == position.offset && tableBucket.equals(position.tableBucket);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableBucket, offset);
    }

    @Override
    public String toString() {
        return tableBucket + " at next offset " + offset;
    }
}
