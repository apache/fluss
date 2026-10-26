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
import org.apache.fluss.metadata.BulkLoadAbortReason;
import org.apache.fluss.metadata.BulkLoadHandle;
import org.apache.fluss.metadata.BulkLoadState;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.utils.json.JsonDeserializer;
import org.apache.fluss.utils.json.JsonSerializer;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Strict deterministic JSON serde for {@link BulkLoadTransaction}. */
@Internal
public final class BulkLoadTransactionJsonSerde
        implements JsonSerializer<BulkLoadTransaction>, JsonDeserializer<BulkLoadTransaction> {

    public static final BulkLoadTransactionJsonSerde INSTANCE = new BulkLoadTransactionJsonSerde();
    private static final int VERSION = 1;

    private static final String VERSION_KEY = "version";
    private static final String BULK_LOAD_ID = "bulk_load_id";
    private static final String STATE = "state";
    private static final String DATABASE_NAME = "database_name";
    private static final String TABLE_NAME = "table_name";
    private static final String PARTITION_NAME = "partition_name";
    private static final String TABLE_ID = "table_id";
    private static final String PARTITION_ID = "partition_id";
    private static final String CREATOR_NAME = "creator_name";
    private static final String CREATOR_TYPE = "creator_type";
    private static final String REMOTE_DATA_DIR = "remote_data_dir";
    private static final String SCHEMA_ID = "schema_id";
    private static final String METADATA_VERSION = "metadata_version";
    private static final String SNAPSHOT_IDS = "snapshot_ids";
    private static final String DEADLINE_MS = "deadline_ms";
    private static final String INPUT_SHA256 = "input_sha256";
    private static final String ABORT_REASON = "abort_reason";
    private static final String ABORT_MESSAGE = "abort_message";

    @Override
    public void serialize(BulkLoadTransaction transaction, JsonGenerator generator)
            throws IOException {
        generator.writeStartObject();
        generator.writeNumberField(VERSION_KEY, VERSION);
        generator.writeStringField(BULK_LOAD_ID, transaction.getBulkLoadId());
        generator.writeNumberField(STATE, transaction.getState().getCode());
        generator.writeStringField(DATABASE_NAME, transaction.getDatabaseName());
        generator.writeStringField(TABLE_NAME, transaction.getTableName());
        if (transaction.getPartitionName() != null) {
            generator.writeStringField(PARTITION_NAME, transaction.getPartitionName());
        }
        generator.writeNumberField(TABLE_ID, transaction.getTableId());
        if (transaction.getPartitionId() != null) {
            generator.writeNumberField(PARTITION_ID, transaction.getPartitionId());
        }
        generator.writeStringField(CREATOR_NAME, transaction.getCreatorName());
        generator.writeStringField(CREATOR_TYPE, transaction.getCreatorType());
        generator.writeStringField(REMOTE_DATA_DIR, transaction.getRemoteDataDir());
        generator.writeNumberField(SCHEMA_ID, transaction.getSchemaId());
        generator.writeNumberField(METADATA_VERSION, transaction.getMetadataVersion());
        long[] snapshotIds = transaction.getSnapshotIds();
        if (snapshotIds != null) {
            generator.writeArrayFieldStart(SNAPSHOT_IDS);
            for (long snapshotId : snapshotIds) {
                generator.writeNumber(snapshotId);
            }
            generator.writeEndArray();
        }
        generator.writeNumberField(DEADLINE_MS, transaction.getDeadlineMs());
        if (transaction.getInputSha256() != null) {
            generator.writeStringField(INPUT_SHA256, transaction.getInputSha256());
        }
        if (transaction.getAbortReason() != null) {
            generator.writeNumberField(ABORT_REASON, transaction.getAbortReason().getCode());
        }
        if (transaction.getAbortMessage() != null) {
            generator.writeStringField(ABORT_MESSAGE, transaction.getAbortMessage());
        }
        generator.writeEndObject();
    }

    @Override
    public BulkLoadTransaction deserialize(JsonNode node) {
        checkArgument(
                node != null && node.isObject(), "BulkLoad transaction must be a JSON object.");
        checkVersion(node);
        BulkLoadHandle handle =
                new BulkLoadHandle(
                        PhysicalTablePath.of(
                                requiredString(node, DATABASE_NAME),
                                requiredString(node, TABLE_NAME),
                                optionalString(node, PARTITION_NAME)),
                        requiredLong(node, TABLE_ID),
                        optionalLong(node, PARTITION_ID),
                        requiredString(node, BULK_LOAD_ID));
        return new BulkLoadTransaction(
                handle,
                BulkLoadState.fromCode(requiredInt(node, STATE)),
                requiredString(node, CREATOR_NAME),
                requiredString(node, CREATOR_TYPE),
                requiredString(node, REMOTE_DATA_DIR),
                requiredInt(node, SCHEMA_ID),
                requiredInt(node, METADATA_VERSION),
                optionalSnapshotIds(node),
                requiredLong(node, DEADLINE_MS),
                optionalString(node, INPUT_SHA256),
                node.has(ABORT_REASON)
                        ? BulkLoadAbortReason.fromCode(requiredInt(node, ABORT_REASON))
                        : null,
                optionalString(node, ABORT_MESSAGE));
    }

    private static void checkVersion(JsonNode node) {
        int version = requiredInt(node, VERSION_KEY);
        checkArgument(version == VERSION, "Unsupported BulkLoad transaction version %s.", version);
    }

    private static JsonNode requiredNode(JsonNode node, String field) {
        JsonNode value = node.get(field);
        checkArgument(value != null, "Missing required BulkLoad field %s.", field);
        return value;
    }

    private static String requiredString(JsonNode node, String field) {
        JsonNode value = requiredNode(node, field);
        checkArgument(value.isTextual(), "BulkLoad field %s must be a string.", field);
        return value.textValue();
    }

    @Nullable
    private static String optionalString(JsonNode node, String field) {
        return node.has(field) ? requiredString(node, field) : null;
    }

    private static int requiredInt(JsonNode node, String field) {
        JsonNode value = requiredNode(node, field);
        checkArgument(
                value.isIntegralNumber() && value.canConvertToInt(),
                "BulkLoad field %s must be an in-range integer.",
                field);
        return value.intValue();
    }

    private static long requiredLong(JsonNode node, String field) {
        JsonNode value = requiredNode(node, field);
        checkArgument(
                value.isIntegralNumber() && value.canConvertToLong(),
                "BulkLoad field %s must be an in-range long.",
                field);
        return value.longValue();
    }

    @Nullable
    private static Long optionalLong(JsonNode node, String field) {
        return node.has(field) ? requiredLong(node, field) : null;
    }

    @Nullable
    private static long[] optionalSnapshotIds(JsonNode node) {
        if (!node.has(SNAPSHOT_IDS)) {
            return null;
        }
        JsonNode value = requiredNode(node, SNAPSHOT_IDS);
        checkArgument(value.isArray(), "BulkLoad field %s must be an array.", SNAPSHOT_IDS);
        long[] snapshotIds = new long[value.size()];
        for (int i = 0; i < value.size(); i++) {
            JsonNode snapshotId = value.get(i);
            checkArgument(
                    snapshotId.isIntegralNumber() && snapshotId.canConvertToLong(),
                    "BulkLoad field %s must contain in-range long values.",
                    SNAPSHOT_IDS);
            snapshotIds[i] = snapshotId.longValue();
        }
        return snapshotIds;
    }
}
