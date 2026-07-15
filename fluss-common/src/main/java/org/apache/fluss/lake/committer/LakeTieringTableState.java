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

package org.apache.fluss.lake.committer;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.utils.json.JsonSerdeUtils;
import org.apache.fluss.utils.json.LakeTieringTableStateJsonSerde;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Table-level tiering state carried by the lake offsets file as an opaque {@code tiering_state}
 * payload (transported as {@code bytes tiering_state_json} in RPC); owned by the lake side and
 * passed through by the offsets serde without parsing.
 *
 * <p>Partition mark-done is the first consumer (delete-on-done model):
 *
 * <ul>
 *   <li>{@code partitionDoneInitialized}: table-level one-shot flag, {@code true} once the
 *       full-table cold-start back-fill has completed; disambiguates "all done" from "first run".
 *   <li>{@code partitionUpdateTimes}: last update time of only the not-yet-done partitions, keyed
 *       by partitionId. A partition is removed once marked done; no {@code doneTime} is persisted.
 * </ul>
 *
 * <p>This is a flat, versioned model. Evolve it by adding fields and bumping {@link
 * #CURRENT_VERSION} (e.g. a future watermark mode). To stay safe when a newer and an older tiering
 * job run against the same table across rounds, a consumer that reads a {@code version} higher than
 * its own {@link #CURRENT_VERSION} MUST NOT overwrite the persisted state (leave it untouched, i.e.
 * do not send {@code tiering_state} back), so it never drops fields written by a newer build.
 *
 * @since 0.9
 */
@PublicEvolving
public class LakeTieringTableState {

    /** The tiering-state format version 1. */
    public static final int VERSION_1 = 1;

    /** The current tiering-state format version written by this build. */
    public static final int CURRENT_VERSION = VERSION_1;

    private final int version;
    private final boolean partitionDoneInitialized;
    private final Map<Long, Long> partitionUpdateTimes;

    public LakeTieringTableState(
            boolean partitionDoneInitialized, Map<Long, Long> partitionUpdateTimes) {
        this(CURRENT_VERSION, partitionDoneInitialized, partitionUpdateTimes);
    }

    public LakeTieringTableState(
            int version, boolean partitionDoneInitialized, Map<Long, Long> partitionUpdateTimes) {
        this.version = version;
        this.partitionDoneInitialized = partitionDoneInitialized;
        this.partitionUpdateTimes =
                partitionUpdateTimes == null
                        ? Collections.emptyMap()
                        : new HashMap<>(partitionUpdateTimes);
    }

    /** Returns the tiering-state format version. */
    public int getVersion() {
        return version;
    }

    /** Returns whether the full-table cold-start back-fill has completed. */
    public boolean isPartitionDoneInitialized() {
        return partitionDoneInitialized;
    }

    /** Returns the last update time of the not-yet-done partitions, keyed by partitionId. */
    public Map<Long, Long> getPartitionUpdateTimes() {
        return Collections.unmodifiableMap(partitionUpdateTimes);
    }

    /**
     * Serialize to a JSON byte array.
     *
     * @see LakeTieringTableStateJsonSerde
     */
    public byte[] toJsonBytes() {
        return JsonSerdeUtils.writeValueAsBytes(this, LakeTieringTableStateJsonSerde.INSTANCE);
    }

    /**
     * Deserialize from a JSON byte array.
     *
     * @see LakeTieringTableStateJsonSerde
     */
    public static LakeTieringTableState fromJsonBytes(byte[] json) {
        return JsonSerdeUtils.readValue(json, LakeTieringTableStateJsonSerde.INSTANCE);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LakeTieringTableState that = (LakeTieringTableState) o;
        return version == that.version
                && partitionDoneInitialized == that.partitionDoneInitialized
                && Objects.equals(partitionUpdateTimes, that.partitionUpdateTimes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, partitionDoneInitialized, partitionUpdateTimes);
    }

    @Override
    public String toString() {
        return "LakeTieringTableState{"
                + "version="
                + version
                + ", partitionDoneInitialized="
                + partitionDoneInitialized
                + ", partitionUpdateTimes="
                + partitionUpdateTimes
                + '}';
    }
}
