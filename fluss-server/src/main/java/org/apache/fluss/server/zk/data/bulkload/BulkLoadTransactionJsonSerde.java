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

import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.utils.json.JsonDeserializer;
import org.apache.fluss.utils.json.JsonSerializer;

import java.io.IOException;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** JSON format of a BulkLoad transaction. */
public final class BulkLoadTransactionJsonSerde
        implements JsonSerializer<BulkLoadTransaction>, JsonDeserializer<BulkLoadTransaction> {
    public static final BulkLoadTransactionJsonSerde INSTANCE = new BulkLoadTransactionJsonSerde();

    private BulkLoadTransactionJsonSerde() {}

    @Override
    public void serialize(BulkLoadTransaction txn, JsonGenerator out) throws IOException {
        out.writeStartObject();
        out.writeNumberField("version", 1);
        out.writeNumberField("table_id", txn.tableId);
        out.writeStringField("bulk_load_id", txn.bulkLoadId);
        out.writeStringField("database_name", txn.tablePath.getDatabaseName());
        out.writeStringField("table_name", txn.tablePath.getTableName());
        out.writeNumberField("schema_id", txn.schemaId);
        out.writeStringField("remote_data_dir", txn.remoteDataDir);
        out.writeNumberField("state", txn.state);
        out.writeNumberField("created_time", txn.createdTime);
        if (txn.finishedTime != null) {
            out.writeNumberField("finished_time", txn.finishedTime);
        }
        if (txn.abortMessage != null) {
            out.writeStringField("abort_message", txn.abortMessage);
        }
        out.writeEndObject();
    }

    @Override
    public BulkLoadTransaction deserialize(JsonNode record) {
        JsonNode version = record.path("version");
        checkArgument(
                version.isInt() && version.intValue() == 1,
                "Unsupported BulkLoad transaction record version: %s",
                version);
        JsonNode state = record.path("state");
        checkArgument(
                state.isInt()
                        && (state.intValue() == BulkLoadTransaction.RUNNING
                                || state.intValue() == BulkLoadTransaction.COMMITTED
                                || state.intValue() == BulkLoadTransaction.ABORTED),
                "Invalid BulkLoad transaction state: %s",
                state);

        return new BulkLoadTransaction(
                record.get("table_id").longValue(),
                record.get("bulk_load_id").textValue(),
                TablePath.of(
                        record.get("database_name").asText(), record.get("table_name").asText()),
                record.get("schema_id").asInt(),
                record.get("remote_data_dir").textValue(),
                state.intValue(),
                record.get("created_time").longValue(),
                record.has("finished_time") ? record.get("finished_time").longValue() : null,
                record.path("abort_message").textValue());
    }
}
