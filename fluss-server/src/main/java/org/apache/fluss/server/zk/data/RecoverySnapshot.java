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

package org.apache.fluss.server.zk.data;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.utils.json.JsonSerdeUtils;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** The durable snapshot identity selected for a physical bucket's recovery. */
@Internal
public final class RecoverySnapshot {

    private final long snapshotId;

    /** Creates a selection without duplicating the ordinary reference's offset or path. */
    public RecoverySnapshot(long snapshotId) {
        checkArgument(snapshotId >= 0, "Invalid snapshot ID: %s", snapshotId);
        this.snapshotId = snapshotId;
    }

    /** Returns the selected snapshot identity. */
    public long getSnapshotId() {
        return snapshotId;
    }

    /** Encodes the persistent version 1 record. */
    public byte[] toJson() {
        return JsonSerdeUtils.writeValueAsBytes(
                this,
                (value, out) -> {
                    out.writeStartObject();
                    out.writeNumberField("version", 1);
                    out.writeNumberField("snapshot_id", value.snapshotId);
                    out.writeEndObject();
                });
    }

    /** Decodes a supported record, failing closed on invalid formats. */
    public static RecoverySnapshot fromJson(byte[] bytes) {
        return JsonSerdeUtils.readValue(
                bytes,
                node -> {
                    checkArgument(
                            node.has("version")
                                    && node.get("version").isIntegralNumber()
                                    && node.get("version").canConvertToInt()
                                    && node.get("version").asInt() == 1,
                            "Unsupported recovery snapshot format.");
                    checkArgument(
                            node.has("snapshot_id")
                                    && node.get("snapshot_id").isIntegralNumber()
                                    && node.get("snapshot_id").canConvertToLong(),
                            "Invalid recovery snapshot ID.");
                    return new RecoverySnapshot(node.get("snapshot_id").asLong());
                });
    }
}
