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

import java.util.Objects;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** A point-in-time observation of the inclusive historical interval for a bucket. */
@PublicEvolving
public final class BucketReadRange {
    private final BucketReadPosition earliestPosition;
    private final BucketReadPosition latestPosition;

    /** Creates an observed range of readable positions for one bucket. */
    public BucketReadRange(TableBucket bucket, long earliestNextOffset, long latestNextOffset) {
        checkArgument(latestNextOffset >= earliestNextOffset, "Invalid bucket read range.");
        this.earliestPosition = BucketReadPosition.of(bucket, earliestNextOffset);
        this.latestPosition = BucketReadPosition.of(bucket, latestNextOffset);
    }

    /** Returns the bucket to which both positions belong. */
    public TableBucket getTableBucket() {
        return earliestPosition.getTableBucket();
    }

    /** Returns the earliest readable position, included in this range. */
    public BucketReadPosition getEarliestPosition() {
        return earliestPosition;
    }

    /** Returns the latest readable position, included in this range. */
    public BucketReadPosition getLatestPosition() {
        return latestPosition;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof BucketReadRange)) {
            return false;
        }
        BucketReadRange that = (BucketReadRange) other;
        return earliestPosition.equals(that.earliestPosition)
                && latestPosition.equals(that.latestPosition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(earliestPosition, latestPosition);
    }

    @Override
    public String toString() {
        return getTableBucket()
                + " ["
                + earliestPosition.getNextOffset()
                + ", "
                + latestPosition.getNextOffset()
                + "]";
    }
}
