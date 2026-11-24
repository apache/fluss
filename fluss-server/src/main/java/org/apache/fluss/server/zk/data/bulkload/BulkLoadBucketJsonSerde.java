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

import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.utils.json.JsonDeserializer;
import org.apache.fluss.utils.json.JsonSerializer;

import java.io.IOException;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** JSON format of a BulkLoad bucket allocation and commit result. */
public final class BulkLoadBucketJsonSerde
        implements JsonSerializer<BulkLoadBucket>, JsonDeserializer<BulkLoadBucket> {
    public static final BulkLoadBucketJsonSerde INSTANCE = new BulkLoadBucketJsonSerde();
    private static final int VERSION = 1;
    private static final String SNAPSHOT_ID = "snapshot_id";
    private static final String LOG_OFFSET = "log_offset";
    private static final String STATE = "state";

    @Override
    public void serialize(BulkLoadBucket bucket, JsonGenerator out) throws IOException {
        out.writeStartObject();
        out.writeNumberField("version", VERSION);
        out.writeNumberField(SNAPSHOT_ID, bucket.snapshotId);
        out.writeNumberField(LOG_OFFSET, bucket.logOffset);
        out.writeNumberField(STATE, bucket.state);
        out.writeEndObject();
    }

    @Override
    public BulkLoadBucket deserialize(JsonNode node) {
        JsonNode version = node.path("version");
        checkArgument(
                version.isInt() && version.intValue() == VERSION,
                "Unsupported BulkLoad bucket record version: %s",
                version);
        JsonNode state = node.path(STATE);
        checkArgument(
                state.isInt()
                        && (state.intValue() == BulkLoadBucket.ALLOCATED
                                || state.intValue() == BulkLoadBucket.COMMITTED),
                "Invalid BulkLoad bucket state: %s",
                state);
        JsonNode snapshotId = node.path(SNAPSHOT_ID);
        JsonNode logOffset = node.path(LOG_OFFSET);
        checkArgument(
                snapshotId.isIntegralNumber() && snapshotId.canConvertToLong(),
                "Invalid BulkLoad bucket snapshot ID.");
        checkArgument(
                logOffset.isIntegralNumber() && logOffset.canConvertToLong(),
                "Invalid BulkLoad bucket log offset.");
        return new BulkLoadBucket(snapshotId.longValue(), logOffset.longValue(), state.intValue());
    }
}
