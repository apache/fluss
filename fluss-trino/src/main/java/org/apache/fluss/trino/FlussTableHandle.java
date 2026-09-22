/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.trino;

import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import io.trino.spi.connector.ConnectorTableHandle;

import java.util.Objects;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Immutable Fluss table handle. */
public final class FlussTableHandle implements ConnectorTableHandle {

    // Trino-visible logical identity.
    private final String schemaName;
    private final String tableName;

    // Physical Fluss identity.
    private final String flussDatabaseName;
    private final String flussTableName;

    // Stable Fluss identity used for stale-handle detection.
    private final long tableId;
    private final int schemaId;

    @JsonCreator
    public FlussTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("flussDatabaseName") String flussDatabaseName,
            @JsonProperty("flussTableName") String flussTableName,
            @JsonProperty("tableId") long tableId,
            @JsonProperty("schemaId") int schemaId) {
        this.schemaName = checkNotNull(schemaName, "schemaName is null");
        this.tableName = checkNotNull(tableName, "tableName is null");
        this.flussDatabaseName = checkNotNull(flussDatabaseName, "flussDatabaseName is null");
        this.flussTableName = checkNotNull(flussTableName, "flussTableName is null");
        checkArgument(tableId >= 0, "tableId must be non-negative");
        checkArgument(schemaId >= 0, "schemaId must be non-negative");
        this.tableId = tableId;
        this.schemaId = schemaId;
    }

    @JsonProperty
    public String getSchemaName() {
        return schemaName;
    }

    @JsonProperty
    public String getTableName() {
        return tableName;
    }

    @JsonProperty
    public String getFlussDatabaseName() {
        return flussDatabaseName;
    }

    @JsonProperty
    public String getFlussTableName() {
        return flussTableName;
    }

    @JsonProperty
    public long getTableId() {
        return tableId;
    }

    @JsonProperty
    public int getSchemaId() {
        return schemaId;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof FlussTableHandle)) {
            return false;
        }

        FlussTableHandle that = (FlussTableHandle) obj;
        return tableId == that.tableId
                && schemaId == that.schemaId
                && schemaName.equals(that.schemaName)
                && tableName.equals(that.tableName)
                && flussDatabaseName.equals(that.flussDatabaseName)
                && flussTableName.equals(that.flussTableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                schemaName, tableName, flussDatabaseName, flussTableName, tableId, schemaId);
    }

    @Override
    public String toString() {
        if (schemaName.equals(flussDatabaseName) && tableName.equals(flussTableName)) {
            return schemaName + ":" + tableName;
        }

        return schemaName + ":" + tableName + " -> " + flussDatabaseName + ":" + flussTableName;
    }
}
