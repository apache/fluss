/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.metadata;

import com.alibaba.fluss.annotation.PublicEvolving;

import java.util.Objects;

/**
 * Information of a table metadata, includes {@link TableDescriptor} and the table id that
 * represents the unique identifier of the table. This makes sure that tables recreated with the
 * same name will always have unique table identifiers. It also contains a schema id which
 * identifies the schema of the table like {@link SchemaInfo} does.
 *
 * @since 0.1
 */
@PublicEvolving
public final class TableInfo {

    public static final long UNKNOWN_TABLE_ID = -1L;

    private final TablePath tablePath;
    private final TableDescriptor tableDescriptor;
    private final long tableId;
    private final int schemaId;
    private final long createdTime;
    private final long modifiedTime;

    public TableInfo(
            TablePath tablePath,
            long tableId,
            TableDescriptor tableDescriptor,
            int schemaId,
            long createdTime,
            long modifiedTime) {
        this.tablePath = tablePath;
        this.tableId = tableId;
        this.tableDescriptor = tableDescriptor;
        this.schemaId = schemaId;
        this.createdTime = createdTime;
        this.modifiedTime = modifiedTime;
    }

    public TablePath getTablePath() {
        return tablePath;
    }

    public TableDescriptor getTableDescriptor() {
        return tableDescriptor;
    }

    public long getTableId() {
        return tableId;
    }

    public int getSchemaId() {
        return schemaId;
    }

    public long getCreatedTime() {
        return createdTime;
    }

    public long getModifiedTime() {
        return modifiedTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableInfo tableInfo = (TableInfo) o;
        return tableId == tableInfo.tableId
                && schemaId == tableInfo.schemaId
                && Objects.equals(tablePath, tableInfo.tablePath)
                && Objects.equals(tableDescriptor, tableInfo.tableDescriptor);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tablePath, tableDescriptor, tableId, schemaId);
    }

    @Override
    public String toString() {
        return "TableInfo{"
                + "tablePath="
                + tablePath
                + ", tableDescriptor="
                + tableDescriptor
                + ", tableId="
                + tableId
                + ", schemaId="
                + schemaId
                + ", createdTime="
                + createdTime
                + ", modifiedTime="
                + modifiedTime
                + '}';
    }
}
