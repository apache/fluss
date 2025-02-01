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

package com.alibaba.fluss.client.table;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.client.FlussConnection;
import com.alibaba.fluss.client.lookup.Lookup;
import com.alibaba.fluss.client.lookup.TableLookup;
import com.alibaba.fluss.client.table.scanner.Scan;
import com.alibaba.fluss.client.table.scanner.TableScan;
import com.alibaba.fluss.client.table.writer.Append;
import com.alibaba.fluss.client.table.writer.TableAppend;
import com.alibaba.fluss.client.table.writer.TableUpsert;
import com.alibaba.fluss.client.table.writer.Upsert;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;

import static com.alibaba.fluss.utils.Preconditions.checkState;

/**
 * The base impl of {@link Table}.
 *
 * @since 0.1
 */
@PublicEvolving
public class FlussTable implements Table {

    private final FlussConnection conn;
    private final TablePath tablePath;
    private final TableInfo tableInfo;
    private final boolean hasPrimaryKey;

    public FlussTable(FlussConnection conn, TablePath tablePath, TableInfo tableInfo) {
        this.conn = conn;
        this.tablePath = tablePath;
        this.tableInfo = tableInfo;
        this.hasPrimaryKey = tableInfo.getTableDescriptor().hasPrimaryKey();
    }

    @Override
    public TableDescriptor getDescriptor() {
        return tableInfo.getTableDescriptor();
    }

    @Override
    public Scan newScan() {
        return new TableScan(conn, tableInfo);
    }

    @Override
    public Lookup newLookup() {
        return new TableLookup(
                tableInfo, conn.getMetadataUpdater(), conn.getOrCreateLookupClient());
    }

    @Override
    public Append newAppend() {
        checkState(
                !hasPrimaryKey,
                "Table %s is not a Log Table and doesn't support AppendWriter.",
                tablePath);
        return new TableAppend(
                tablePath,
                tableInfo.getTableDescriptor(),
                conn.getMetadataUpdater(),
                conn.getOrCreateWriterClient());
    }

    @Override
    public Upsert newUpsert() {
        checkState(
                hasPrimaryKey,
                "Table %s is not a Primary Key Table and doesn't support UpsertWriter.",
                tablePath);
        return new TableUpsert(
                tablePath,
                tableInfo.getTableDescriptor(),
                conn.getMetadataUpdater(),
                conn.getOrCreateWriterClient());
    }

    @Override
    public void close() throws Exception {
        // do nothing
    }
}
