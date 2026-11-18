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
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.utils.json.JsonSerdeUtils;
import org.apache.fluss.utils.json.TableDescriptorJsonSerde;

import javax.annotation.Nullable;

/** Durable transaction facts; target and bucket records are stored separately. */
@Internal
public final class BulkLoadTransaction {
    public static final int RUNNING = 0;
    public static final int COMMITTED = 1;
    public static final int ABORTED = 2;

    public final long tableId;
    public final String bulkLoadId;
    public final TableInfo tableInfo;
    public final int state;
    public final long createdTime;
    public final @Nullable Long finishedTime;
    public final @Nullable String abortMessage;

    /** Creates a transaction record without embedding its task set. */
    public BulkLoadTransaction(
            long tableId,
            String bulkLoadId,
            TableInfo tableInfo,
            int state,
            long createdTime,
            @Nullable Long finishedTime,
            @Nullable String abortMessage) {
        this.tableId = tableId;
        this.bulkLoadId = bulkLoadId;
        this.tableInfo = tableInfo;
        this.state = state;
        this.createdTime = createdTime;
        this.finishedTime = finishedTime;
        this.abortMessage = abortMessage;
    }

    /** Returns a terminal record, preserving the original build context. */
    public BulkLoadTransaction finish(int terminalState, long now, @Nullable String message) {
        return new BulkLoadTransaction(
                tableId, bulkLoadId, tableInfo, terminalState, createdTime, now, message);
    }

    /** Encodes the version 1 JSON record. Physical identity is carried by the path. */
    public byte[] toJson() {
        return JsonSerdeUtils.writeValueAsBytes(
                this,
                (value, out) -> {
                    out.writeStartObject();
                    out.writeNumberField("version", 1);
                    out.writeNumberField("state", state);
                    out.writeObjectFieldStart("table_path");
                    out.writeStringField(
                            "database_name", tableInfo.getTablePath().getDatabaseName());
                    out.writeStringField("table_name", tableInfo.getTablePath().getTableName());
                    out.writeEndObject();
                    out.writeObjectFieldStart("table_info");
                    out.writeNumberField("schema_id", tableInfo.getSchemaId());
                    out.writeFieldName("table_json");
                    TableDescriptorJsonSerde.INSTANCE.serialize(tableInfo.toTableDescriptor(), out);
                    out.writeNumberField("created_time", tableInfo.getCreatedTime());
                    out.writeNumberField("modified_time", tableInfo.getModifiedTime());
                    if (tableInfo.getRemoteDataDir() != null) {
                        out.writeStringField("remote_data_dir", tableInfo.getRemoteDataDir());
                    }
                    out.writeNumberField("bucket_count_epoch", tableInfo.getBucketCountEpoch());
                    out.writeEndObject();
                    out.writeNumberField("created_time", createdTime);
                    if (finishedTime != null) {
                        out.writeNumberField("finished_time", finishedTime);
                    }
                    if (abortMessage != null) {
                        out.writeStringField("abort_message", abortMessage);
                    }
                    out.writeEndObject();
                });
    }

    /** Decodes a transaction using its physical identity from the path. */
    public static BulkLoadTransaction fromJson(long tableId, String bulkLoadId, byte[] bytes) {
        return JsonSerdeUtils.readValue(
                bytes,
                node -> {
                    JsonNode info = node.get("table_info");
                    JsonNode path = node.get("table_path");
                    TableInfo table =
                            TableInfo.of(
                                    TablePath.of(
                                            path.get("database_name").asText(),
                                            path.get("table_name").asText()),
                                    tableId,
                                    info.get("schema_id").asInt(),
                                    TableDescriptorJsonSerde.INSTANCE.deserialize(
                                            info.get("table_json")),
                                    info.has("remote_data_dir")
                                            ? info.get("remote_data_dir").asText()
                                            : null,
                                    info.get("created_time").asLong(),
                                    info.get("modified_time").asLong(),
                                    info.path("bucket_count_epoch").asLong());
                    return new BulkLoadTransaction(
                            tableId,
                            bulkLoadId,
                            table,
                            node.get("state").asInt(),
                            node.get("created_time").asLong(),
                            node.has("finished_time") ? node.get("finished_time").asLong() : null,
                            node.has("abort_message") ? node.get("abort_message").asText() : null);
                });
    }
}
