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

package org.apache.fluss.server.metadata;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;

/** This entity used to describe the bucket metadata. */
public class BucketMetadata {
    public static final int NO_LEADER_ISR_STATE_EPOCH = -1;

    private final int bucketId;
    private final @Nullable Integer leaderId;
    private final @Nullable Integer leaderEpoch;
    private final List<Integer> replicas;
    private final List<Integer> isr;
    private final @Nullable Integer bucketEpoch;

    /**
     * Creates legacy bucket metadata without authoritative leader/ISR state.
     *
     * <p>The empty ISR and absent bucket epoch distinguish this metadata from an authoritative
     * state with an empty ISR.
     */
    public BucketMetadata(
            int bucketId,
            @Nullable Integer leaderId,
            @Nullable Integer leaderEpoch,
            List<Integer> replicas) {
        this(bucketId, leaderId, leaderEpoch, replicas, Collections.emptyList(), null);
    }

    public BucketMetadata(
            int bucketId,
            @Nullable Integer leaderId,
            @Nullable Integer leaderEpoch,
            List<Integer> replicas,
            List<Integer> isr,
            @Nullable Integer bucketEpoch) {
        this.bucketId = bucketId;
        this.leaderId = leaderId;
        this.leaderEpoch = leaderEpoch;
        this.replicas = Collections.unmodifiableList(new ArrayList<>(replicas));
        this.isr = Collections.unmodifiableList(new ArrayList<>(isr));
        this.bucketEpoch = bucketEpoch;
    }

    public int getBucketId() {
        return bucketId;
    }

    public OptionalInt getLeaderId() {
        return leaderId == null ? OptionalInt.empty() : OptionalInt.of(leaderId);
    }

    public OptionalInt getLeaderEpoch() {
        return leaderEpoch == null ? OptionalInt.empty() : OptionalInt.of(leaderEpoch);
    }

    public List<Integer> getReplicas() {
        return replicas;
    }

    public List<Integer> getIsr() {
        return isr;
    }

    public @Nullable Integer getBucketEpoch() {
        return bucketEpoch;
    }

    @Override
    public String toString() {
        return "BucketMetadata{"
                + "bucketId="
                + bucketId
                + ", leaderId="
                + leaderId
                + ", leaderEpoch="
                + leaderEpoch
                + ", replicas="
                + replicas
                + ", isr="
                + isr
                + ", bucketEpoch="
                + bucketEpoch
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BucketMetadata that = (BucketMetadata) o;
        return bucketId == that.bucketId
                && Objects.equals(leaderId, that.leaderId)
                && Objects.equals(leaderEpoch, that.leaderEpoch)
                && replicas.equals(that.replicas)
                && isr.equals(that.isr)
                && Objects.equals(bucketEpoch, that.bucketEpoch);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucketId, leaderId, leaderEpoch, replicas, isr, bucketEpoch);
    }
}
