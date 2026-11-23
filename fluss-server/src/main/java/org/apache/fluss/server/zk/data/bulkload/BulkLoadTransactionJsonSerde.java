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
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Reads the bucket operations' view of a BulkLoad transaction. */
public final class BulkLoadTransactionJsonSerde {
    private static final int COMMITTED = 1;
    private static final int ABORTED = 2;

    private BulkLoadTransactionJsonSerde() {}

    /** Decodes the persisted fields and takes the transaction identity from the znode path. */
    public static BulkLoadTransaction deserialize(
            long tableId, String bulkLoadId, JsonNode record) {
        JsonNode version = record.path("version");
        checkArgument(
                version.isInt() && version.intValue() == 1,
                "Unsupported BulkLoad transaction record version: %s",
                version);
        JsonNode state = record.path("state");
        checkArgument(
                state.isInt()
                        && (state.intValue() == BulkLoadTransaction.RUNNING
                                || state.intValue() == COMMITTED
                                || state.intValue() == ABORTED),
                "Invalid BulkLoad transaction state: %s",
                state);

        JsonNode path = record.get("table_path");
        return new BulkLoadTransaction(
                tableId,
                bulkLoadId,
                TablePath.of(path.get("database_name").asText(), path.get("table_name").asText()),
                record.get("schema_id").asInt(),
                record.get("remote_data_dir").textValue(),
                state.intValue());
    }
}
