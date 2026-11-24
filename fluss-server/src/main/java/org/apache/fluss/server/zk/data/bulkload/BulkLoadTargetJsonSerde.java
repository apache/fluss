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

/** JSON format of a BulkLoad target. */
public final class BulkLoadTargetJsonSerde
        implements JsonSerializer<BulkLoadTarget>, JsonDeserializer<BulkLoadTarget> {
    public static final BulkLoadTargetJsonSerde INSTANCE = new BulkLoadTargetJsonSerde();
    private static final String PARTITION_ID = "partition_id";
    private static final int VERSION = 1;
    private static final String PARTITION_NAME = "partition_name";
    private static final String BUCKET_COUNT = "bucket_count";
    private static final String REMOTE_DATA_DIR = "remote_data_dir";
    private static final String WITHDRAWN = "withdrawn";

    private BulkLoadTargetJsonSerde() {}

    @Override
    public void serialize(BulkLoadTarget target, JsonGenerator out) throws IOException {
        out.writeStartObject();
        out.writeNumberField("version", VERSION);
        if (target.partitionId != null) {
            out.writeNumberField(PARTITION_ID, target.partitionId);
            out.writeStringField(PARTITION_NAME, target.partitionName);
        }
        out.writeNumberField(BUCKET_COUNT, target.bucketCount);
        out.writeStringField(REMOTE_DATA_DIR, target.remoteDataDir);
        out.writeBooleanField(WITHDRAWN, target.withdrawn);
        out.writeEndObject();
    }

    @Override
    public BulkLoadTarget deserialize(JsonNode record) {
        JsonNode version = record.path("version");
        checkArgument(
                version.isInt() && version.intValue() == VERSION,
                "Unsupported BulkLoad target version.");
        JsonNode id = record.path(PARTITION_ID);
        String partitionName = record.path(PARTITION_NAME).textValue();
        checkArgument(
                id.isMissingNode()
                        ? !record.has(PARTITION_NAME)
                        : id.isIntegralNumber() && id.canConvertToLong() && partitionName != null,
                "BulkLoad target partition ID and name must be specified together.");
        JsonNode count = record.path(BUCKET_COUNT);
        checkArgument(count.isInt(), "Invalid BulkLoad target bucket count.");
        String remoteDataDir = record.path(REMOTE_DATA_DIR).textValue();
        checkArgument(remoteDataDir != null, "Invalid BulkLoad target directory.");
        JsonNode withdrawn = record.path(WITHDRAWN);
        checkArgument(withdrawn.isBoolean(), "Invalid BulkLoad target withdrawal state.");
        return new BulkLoadTarget(
                id.isMissingNode() ? null : id.longValue(),
                partitionName,
                count.intValue(),
                remoteDataDir,
                withdrawn.booleanValue());
    }
}
