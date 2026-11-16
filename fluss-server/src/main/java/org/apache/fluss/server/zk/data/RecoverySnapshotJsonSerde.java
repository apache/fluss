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
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.utils.json.JsonDeserializer;
import org.apache.fluss.utils.json.JsonSerializer;

import java.io.IOException;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** JSON serializer and deserializer for a bucket's selected recovery snapshot ID. */
@Internal
public final class RecoverySnapshotJsonSerde
        implements JsonSerializer<Long>, JsonDeserializer<Long> {

    public static final RecoverySnapshotJsonSerde INSTANCE = new RecoverySnapshotJsonSerde();

    private static final String VERSION_KEY = "version";
    private static final int VERSION = 1;
    private static final String SNAPSHOT_ID = "snapshot_id";

    @Override
    public void serialize(Long snapshotId, JsonGenerator generator) throws IOException {
        checkArgument(snapshotId >= 0, "Invalid snapshot ID: %s", snapshotId);
        generator.writeStartObject();
        generator.writeNumberField(VERSION_KEY, VERSION);
        generator.writeNumberField(SNAPSHOT_ID, snapshotId);
        generator.writeEndObject();
    }

    @Override
    public Long deserialize(JsonNode node) {
        checkArgument(
                node.has(SNAPSHOT_ID)
                        && node.get(SNAPSHOT_ID).isIntegralNumber()
                        && node.get(SNAPSHOT_ID).canConvertToLong(),
                "Invalid recovery snapshot ID.");
        long snapshotId = node.get(SNAPSHOT_ID).asLong();
        checkArgument(snapshotId >= 0, "Invalid snapshot ID: %s", snapshotId);
        return snapshotId;
    }
}
