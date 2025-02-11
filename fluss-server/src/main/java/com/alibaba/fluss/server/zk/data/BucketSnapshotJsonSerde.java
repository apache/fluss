/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.zk.data;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import com.alibaba.fluss.utils.json.JsonDeserializer;
import com.alibaba.fluss.utils.json.JsonSerializer;

import java.io.IOException;

/** Json serializer and deserializer for {@link BucketSnapshot}. */
@Internal
public class BucketSnapshotJsonSerde
        implements JsonSerializer<BucketSnapshot>, JsonDeserializer<BucketSnapshot> {

    public static final BucketSnapshotJsonSerde INSTANCE = new BucketSnapshotJsonSerde();

    private static final String SNAPSHOT_ID = "snapshot_id";
    private static final String LOG_OFFSET = "log_offset";
    private static final String METADATA_PATH = "metadata_path";

    private static final String VERSION_KEY = "version";
    private static final int VERSION = 1;

    @Override
    public BucketSnapshot deserialize(JsonNode node) {
        long snapshotId = node.get(SNAPSHOT_ID).asLong();
        long logOffset = node.get(LOG_OFFSET).asLong();
        String metadataPath = node.get(METADATA_PATH).asText();
        return new BucketSnapshot(snapshotId, logOffset, metadataPath);
    }

    @Override
    public void serialize(BucketSnapshot bucketSnapshot, JsonGenerator generator)
            throws IOException {
        generator.writeStartObject();

        // serialize data version.
        generator.writeNumberField(VERSION_KEY, VERSION);
        generator.writeNumberField(SNAPSHOT_ID, bucketSnapshot.getSnapshotId());
        generator.writeNumberField(LOG_OFFSET, bucketSnapshot.getLogOffset());
        generator.writeStringField(METADATA_PATH, bucketSnapshot.getMetadataPath());

        generator.writeEndObject();
    }
}
