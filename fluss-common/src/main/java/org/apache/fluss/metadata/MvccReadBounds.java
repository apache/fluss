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

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Inclusive bounds of the readable MVCC offsets of a bucket. */
@PublicEvolving
public final class MvccReadBounds {
    private final long earliestOffset;
    private final long latestOffset;

    private MvccReadBounds(long earliestOffset, long latestOffset) {
        this.earliestOffset = earliestOffset;
        this.latestOffset = latestOffset;
    }

    /** Creates a non-negative, inclusive interval. */
    public static MvccReadBounds of(long earliestOffset, long latestOffset) {
        checkArgument(
                earliestOffset >= 0 && earliestOffset <= latestOffset, "Invalid MVCC read bounds.");
        return new MvccReadBounds(earliestOffset, latestOffset);
    }

    /** Returns the earliest readable offset. */
    public long getEarliestOffset() {
        return earliestOffset;
    }

    /** Returns the latest readable offset. */
    public long getLatestOffset() {
        return latestOffset;
    }
}
