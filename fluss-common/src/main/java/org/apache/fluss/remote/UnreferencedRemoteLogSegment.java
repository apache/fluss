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

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.UUID;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Metadata retained for a remote segment that no longer participates in logical reads. */
@Internal
public final class UnreferencedRemoteLogSegment {
    /** Timestamp sentinel used until the unreferenced transition is observed as authoritative. */
    public static final long GC_INELIGIBLE_TIMESTAMP = Long.MAX_VALUE;

    /** The reason why a physical remote segment stopped participating in logical reads. */
    public enum Reason {
        REPLACED,
        EXPIRED
    }

    private final RemoteLogSegment remoteLogSegment;
    private final long unreferencedAtMs;
    private final Reason reason;
    private final @Nullable UUID replacementSegmentId;

    public UnreferencedRemoteLogSegment(
            RemoteLogSegment remoteLogSegment,
            long unreferencedAtMs,
            Reason reason,
            @Nullable UUID replacementSegmentId) {
        this.remoteLogSegment = checkNotNull(remoteLogSegment);
        checkArgument(
                unreferencedAtMs >= 0,
                "Unreferenced timestamp must not be negative: %s",
                unreferencedAtMs);
        this.unreferencedAtMs = unreferencedAtMs;
        this.reason = checkNotNull(reason);
        this.replacementSegmentId = replacementSegmentId;
    }

    public RemoteLogSegment remoteLogSegment() {
        return remoteLogSegment;
    }

    public long unreferencedAtMs() {
        return unreferencedAtMs;
    }

    /** Returns whether the grace-period clock has been started for this segment. */
    public boolean isGcEligible() {
        return unreferencedAtMs != GC_INELIGIBLE_TIMESTAMP;
    }

    public Reason reason() {
        return reason;
    }

    @Nullable
    public UUID replacementSegmentId() {
        return replacementSegmentId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UnreferencedRemoteLogSegment that = (UnreferencedRemoteLogSegment) o;
        return unreferencedAtMs == that.unreferencedAtMs
                && Objects.equals(remoteLogSegment, that.remoteLogSegment)
                && reason == that.reason
                && Objects.equals(replacementSegmentId, that.replacementSegmentId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(remoteLogSegment, unreferencedAtMs, reason, replacementSegmentId);
    }

    @Override
    public String toString() {
        return "UnreferencedRemoteLogSegment{"
                + "remoteLogSegment="
                + remoteLogSegment
                + ", unreferencedAtMs="
                + unreferencedAtMs
                + ", reason="
                + reason
                + ", replacementSegmentId="
                + replacementSegmentId
                + '}';
    }
}
