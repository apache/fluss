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

/** A named MVCC read snapshot and its registered offset for one bucket. */
@PublicEvolving
public final class MvccSnapshotBinding {
    private final MvccSnapshotRef snapshot;
    private final BucketLogOffset offset;

    private MvccSnapshotBinding(MvccSnapshotRef snapshot, BucketLogOffset offset) {
        this.snapshot = Objects.requireNonNull(snapshot);
        this.offset = Objects.requireNonNull(offset);
        if (snapshot.getTableId() != offset.getTableBucket().getTableId()) {
            throw new IllegalArgumentException(
                    "Snapshot and bucket must belong to the same physical table");
        }
    }

    /** Creates a binding for a bucket of the snapshot's physical table. */
    public static MvccSnapshotBinding of(MvccSnapshotRef snapshot, BucketLogOffset offset) {
        return new MvccSnapshotBinding(snapshot, offset);
    }

    /** Returns the snapshot identity. */
    public MvccSnapshotRef getSnapshot() {
        return snapshot;
    }

    /** Returns the registered bucket log offset. */
    public BucketLogOffset getBucketLogOffset() {
        return offset;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof MvccSnapshotBinding)) {
            return false;
        }
        MvccSnapshotBinding binding = (MvccSnapshotBinding) other;
        return snapshot.equals(binding.snapshot) && offset.equals(binding.offset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshot, offset);
    }

    @Override
    public String toString() {
        return snapshot + " at " + offset;
    }
}
