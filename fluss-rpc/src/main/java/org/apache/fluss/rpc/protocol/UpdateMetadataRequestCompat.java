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

package org.apache.fluss.rpc.protocol;

import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.PbTableMetadata;
import org.apache.fluss.rpc.messages.PbTablePath;
import org.apache.fluss.rpc.messages.UpdateMetadataRequest;

/** Legacy table deletion markers used by UPDATE_METADATA v0. */
public final class UpdateMetadataRequestCompat {

    /** Marks a deletion by ID when the coordinator no longer knows the table path. */
    public static final TablePath DELETED_TABLE_PATH = TablePath.of("__UNKNOWN__", "__delete__");

    /** Marks a deletion by path when the coordinator knows the table path. */
    public static final long DELETED_TABLE_ID = -2L;

    private UpdateMetadataRequestCompat() {}

    /** Returns the v0 wire representation without modifying a request shared across connections. */
    public static UpdateMetadataRequest toVersion0(UpdateMetadataRequest request) {
        UpdateMetadataRequest legacy = null;
        for (int i = 0; i < request.getTableMetadatasCount(); i++) {
            PbTableMetadata table = request.getTableMetadataAt(i);
            if (!table.hasDeleted()) {
                continue;
            }
            if (legacy == null) {
                legacy = new UpdateMetadataRequest().copyFrom(request);
            }
            PbTableMetadata legacyTable = legacy.getTableMetadataAt(i);
            PbTablePath path = table.getTablePath();
            if (table.isDeleted()
                    && !DELETED_TABLE_PATH.equals(
                            TablePath.of(path.getDatabaseName(), path.getTableName()))) {
                legacyTable.setTableId(DELETED_TABLE_ID);
            }
            legacyTable.clearDeleted();
        }
        return legacy == null ? request : legacy;
    }
}
