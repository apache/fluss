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
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.utils.json.JsonDeserializer;
import org.apache.fluss.utils.json.JsonSerializer;

import java.io.IOException;

/** Json serializer and deserializer for {@link PartitionRegistration}. */
@Internal
public class PartitionRegistrationJsonSerde
        implements JsonSerializer<PartitionRegistration>, JsonDeserializer<PartitionRegistration> {

    public static final PartitionRegistrationJsonSerde INSTANCE =
            new PartitionRegistrationJsonSerde();

    private static final String VERSION_KEY = "version";
    private static final String TABLE_ID_KEY = "table_id";
    private static final String PARTITION_ID_KEY = "partition_id";
    private static final String REMOTE_DATA_DIR_KEY = "remote_data_dir";
    private static final int VERSION = 1;

    @Override
    public void serialize(PartitionRegistration registration, JsonGenerator generator)
            throws IOException {
        generator.writeStartObject();
        generator.writeNumberField(VERSION_KEY, VERSION);
        generator.writeNumberField(TABLE_ID_KEY, registration.getTableId());
        generator.writeNumberField(PARTITION_ID_KEY, registration.getPartitionId());
        generator.writeStringField(REMOTE_DATA_DIR_KEY, registration.getRemoteDataDir().toString());
        generator.writeEndObject();
    }

    @Override
    public PartitionRegistration deserialize(JsonNode node) {
        long tableId = node.get(TABLE_ID_KEY).asLong();
        long partitionId = node.get(PARTITION_ID_KEY).asLong();
        // When deserialize from an old version, the remote data dir may not exist.
        // But we will fill it with ConfigOptions.REMOTE_DATA_DIR immediately.
        FsPath remoteDataDir = null;
        if (node.has(REMOTE_DATA_DIR_KEY)) {
            remoteDataDir = new FsPath(node.get(REMOTE_DATA_DIR_KEY).asText());
        }
        return new PartitionRegistration(tableId, partitionId, remoteDataDir);
    }
}
