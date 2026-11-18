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

package org.apache.fluss.server.zk.data.bulkload;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.utils.json.JsonSerdeUtils;

/** Immutable allocation and commit result of one transaction bucket. */
@Internal
public final class BulkLoadBucket {
    public static final int ALLOCATED = 0;
    public static final int COMMITTED = 1;

    public final long snapshotId;
    public final long logOffset;
    public final int state;

    /** Creates an allocated or committed bucket record. */
    public BulkLoadBucket(long snapshotId, long logOffset, int state) {
        this.snapshotId = snapshotId;
        this.logOffset = logOffset;
        this.state = state;
    }

    /** Encodes the version 1 JSON record. */
    public byte[] toJson() {
        return JsonSerdeUtils.writeValueAsBytes(
                this,
                (value, out) -> {
                    out.writeStartObject();
                    out.writeNumberField("version", 1);
                    out.writeNumberField("snapshot_id", snapshotId);
                    out.writeNumberField("log_offset", logOffset);
                    out.writeNumberField("state", state);
                    out.writeEndObject();
                });
    }

    /** Decodes an allocated or committed bucket record. */
    public static BulkLoadBucket fromJson(byte[] bytes) {
        return JsonSerdeUtils.readValue(
                bytes,
                node ->
                        new BulkLoadBucket(
                                node.get("snapshot_id").asLong(),
                                node.get("log_offset").asLong(),
                                node.get("state").asInt()));
    }
}
