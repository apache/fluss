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
import org.apache.fluss.server.zk.data.bulkload.BulkLoadDataState;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.utils.json.JsonDeserializer;
import org.apache.fluss.utils.json.JsonSerializer;

import java.io.IOException;

import static org.apache.fluss.utils.Preconditions.checkArgument;

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
    private static final String BUCKET_COUNT_KEY = "bucket_count";
    private static final String DATA_STATE_KEY = "data_state";
    private static final String BULK_LOAD_ID_KEY = "bulk_load_id";
    static final int VERSION = 3;
    private static final int LEGACY_VERSION = 2;

    @Override
    public void serialize(PartitionRegistration registration, JsonGenerator generator)
            throws IOException {
        generator.writeStartObject();
        generator.writeNumberField(
                VERSION_KEY, registration.getBulkLoadId() == null ? LEGACY_VERSION : VERSION);
        generator.writeNumberField(TABLE_ID_KEY, registration.getTableId());
        generator.writeNumberField(PARTITION_ID_KEY, registration.getPartitionId());
        if (registration.getRemoteDataDir() != null) {
            generator.writeStringField(REMOTE_DATA_DIR_KEY, registration.getRemoteDataDir());
        }
        if (registration.getBucketCount() != null) {
            generator.writeNumberField(BUCKET_COUNT_KEY, registration.getBucketCount());
        }
        if (registration.getBulkLoadId() != null) {
            generator.writeNumberField(DATA_STATE_KEY, registration.getDataState().getCode());
            generator.writeStringField(BULK_LOAD_ID_KEY, registration.getBulkLoadId());
        }
        generator.writeEndObject();
    }

    @Override
    public PartitionRegistration deserialize(JsonNode node) {
        int version = readVersion(node);
        long tableId = node.get(TABLE_ID_KEY).asLong();
        long partitionId = node.get(PARTITION_ID_KEY).asLong();
        // When deserialize from an old version, the remote data dir may not exist.
        // But we will fill it with ConfigOptions.REMOTE_DATA_DIR immediately.
        String remoteDataDir = null;
        if (node.has(REMOTE_DATA_DIR_KEY)) {
            remoteDataDir = node.get(REMOTE_DATA_DIR_KEY).asText();
        }
        // When deserialize from an old version (v1), bucket_count may not exist.
        // Callers should fall back to table-level bucket count when this is null.
        Integer bucketCount = null;
        if (node.has(BUCKET_COUNT_KEY)) {
            bucketCount = node.get(BUCKET_COUNT_KEY).asInt();
        }
        BulkLoadDataState dataState = BulkLoadDataState.ACTIVE;
        String bulkLoadId = null;
        if (version == VERSION) {
            JsonNode dataStateNode = node.get(DATA_STATE_KEY);
            JsonNode bulkLoadIdNode = node.get(BULK_LOAD_ID_KEY);
            checkArgument(
                    dataStateNode != null
                            && dataStateNode.isIntegralNumber()
                            && dataStateNode.canConvertToInt(),
                    "PartitionRegistration version 3 data_state must be an integer.");
            checkArgument(
                    bulkLoadIdNode != null && bulkLoadIdNode.isTextual(),
                    "PartitionRegistration version 3 bulk_load_id must be a string.");
            dataState = BulkLoadDataState.fromCode(dataStateNode.intValue());
            bulkLoadId = bulkLoadIdNode.textValue();
        }
        return new PartitionRegistration(
                tableId, partitionId, remoteDataDir, bucketCount, dataState, bulkLoadId);
    }

    private static int readVersion(JsonNode node) {
        JsonNode versionNode = node.get(VERSION_KEY);
        String version = versionNode == null ? "missing" : versionNode.toString();
        checkArgument(
                node.has(VERSION_KEY)
                        && versionNode.isIntegralNumber()
                        && versionNode.canConvertToInt(),
                "PartitionRegistration version must be an integer: %s.",
                version);
        int versionValue = versionNode.intValue();
        checkArgument(
                versionValue > 0 && versionValue <= VERSION,
                "Unsupported PartitionRegistration version %s.",
                versionValue);
        return versionValue;
    }
}
