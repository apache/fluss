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

package org.apache.fluss.remote;

import org.apache.fluss.annotation.Internal;

import java.util.Objects;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** A runtime logical reference to a physical {@link RemoteLogSegment}. */
@Internal
public final class RemoteLogSegmentReference {
    private final RemoteLogSegment remoteLogSegment;
    private final long logicalStartOffset;
    private final long logicalEndOffset;

    public RemoteLogSegmentReference(
            RemoteLogSegment remoteLogSegment, long logicalStartOffset, long logicalEndOffset) {
        this.remoteLogSegment = checkNotNull(remoteLogSegment);
        checkArgument(
                remoteLogSegment.remoteLogStartOffset() <= logicalStartOffset,
                "Logical start offset %s is before physical start offset %s",
                logicalStartOffset,
                remoteLogSegment.remoteLogStartOffset());
        checkArgument(
                logicalStartOffset < logicalEndOffset,
                "Logical range [%s, %s) must not be empty",
                logicalStartOffset,
                logicalEndOffset);
        checkArgument(
                logicalEndOffset <= remoteLogSegment.remoteLogEndOffset(),
                "Logical end offset %s is after physical end offset %s",
                logicalEndOffset,
                remoteLogSegment.remoteLogEndOffset());
        this.logicalStartOffset = logicalStartOffset;
        this.logicalEndOffset = logicalEndOffset;
    }

    public RemoteLogSegment remoteLogSegment() {
        return remoteLogSegment;
    }

    public long logicalStartOffset() {
        return logicalStartOffset;
    }

    public long logicalEndOffset() {
        return logicalEndOffset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RemoteLogSegmentReference that = (RemoteLogSegmentReference) o;
        return logicalStartOffset == that.logicalStartOffset
                && logicalEndOffset == that.logicalEndOffset
                && Objects.equals(remoteLogSegment, that.remoteLogSegment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(remoteLogSegment, logicalStartOffset, logicalEndOffset);
    }

    @Override
    public String toString() {
        return "RemoteLogSegmentReference{"
                + "remoteLogSegment="
                + remoteLogSegment
                + ", logicalStartOffset="
                + logicalStartOffset
                + ", logicalEndOffset="
                + logicalEndOffset
                + '}';
    }
}
