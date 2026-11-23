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
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.utils.json.JsonSerdeUtils;

import javax.annotation.Nullable;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Durable transaction facts; target and bucket records are stored separately. */
@Internal
public final class BulkLoadTransaction {
    public static final int RUNNING = 0;

    private static final int COMMITTED = 1;
    private static final int ABORTED = 2;

    public final String bulkLoadId;
    public final long tableId;
    public final TablePath tablePath;
    public final int schemaId;
    public final @Nullable String remoteDataDir;
    public final int state;

    private BulkLoadTransaction(long tableId, String bulkLoadId, JsonNode record) {
        JsonNode version = record.path("version");
        checkArgument(
                version.isInt() && version.intValue() == 1,
                "Unsupported BulkLoad transaction record version: %s",
                version);
        JsonNode state = record.path("state");
        checkArgument(
                state.isInt()
                        && (state.intValue() == RUNNING
                                || state.intValue() == COMMITTED
                                || state.intValue() == ABORTED),
                "Invalid BulkLoad transaction state: %s",
                state);

        this.tableId = tableId;
        this.bulkLoadId = bulkLoadId;
        this.state = state.intValue();
        JsonNode path = record.get("table_path");
        this.tablePath =
                TablePath.of(path.get("database_name").asText(), path.get("table_name").asText());
        JsonNode info = record.get("table_info");
        this.schemaId = info.get("schema_id").asInt();
        this.remoteDataDir =
                info.has("remote_data_dir") ? info.get("remote_data_dir").asText() : null;
    }

    /** Reads the transaction fields needed to allocate or commit a bucket. */
    public static BulkLoadTransaction fromJson(long tableId, String bulkLoadId, byte[] bytes) {
        return JsonSerdeUtils.readValue(
                bytes, node -> new BulkLoadTransaction(tableId, bulkLoadId, node));
    }
}
