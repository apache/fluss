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

package org.apache.fluss.client.table;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.client.FlussConnection;
import org.apache.fluss.client.lookup.Lookup;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.lookup.SecondaryIndexLookuper;
import org.apache.fluss.client.lookup.TableLookup;
import org.apache.fluss.client.metadata.ClientSchemaGetter;
import org.apache.fluss.client.table.scanner.Scan;
import org.apache.fluss.client.table.scanner.TableScan;
import org.apache.fluss.client.table.writer.Append;
import org.apache.fluss.client.table.writer.TableAppend;
import org.apache.fluss.client.table.writer.TableUpsert;
import org.apache.fluss.client.table.writer.Upsert;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.IndexTableUtils;

import static org.apache.fluss.utils.Preconditions.checkState;

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
    private final SchemaGetter schemaGetter;

    public FlussTable(FlussConnection conn, TablePath tablePath, TableInfo tableInfo) {
        this.conn = conn;
        this.tablePath = tablePath;
        this.tableInfo = tableInfo;
        this.hasPrimaryKey = tableInfo.hasPrimaryKey();
        this.schemaGetter =
                new ClientSchemaGetter(tablePath, tableInfo.getSchemaInfo(), conn.getAdmin());
    }

    @Override
    public TableInfo getTableInfo() {
        return tableInfo;
    }

    @Override
    public Scan newScan() {
        return new TableScan(conn, tableInfo, schemaGetter);
    }

    @Override
    public Lookup newLookup() {
        return new TableLookup(
                tableInfo, schemaGetter, conn.getMetadataUpdater(), conn.getOrCreateLookupClient());
    }

    @Override
    public Append newAppend() {
        checkState(
                !hasPrimaryKey,
                "Table %s is not a Log Table and doesn't support AppendWriter.",
                tablePath);
        return new TableAppend(tablePath, tableInfo, conn.getOrCreateWriterClient());
    }

    @Override
    public Upsert newUpsert() {
        checkState(
                !tableInfo.isIndexTable(),
                "Table %s is an internal secondary index table and doesn't support public UpsertWriter.",
                tablePath);
        checkState(
                hasPrimaryKey,
                "Table %s is not a Primary Key Table and doesn't support UpsertWriter.",
                tablePath);
        return new TableUpsert(tablePath, tableInfo, conn.getOrCreateWriterClient());
    }

    /** The returned lookuper can be reused by concurrent callers. */
    @Override
    public Lookuper getSecondaryIndexLookuper(String indexName) {
        Schema mainSchema = tableInfo.getSchema();
        Schema.Index index = findIndexOrThrow(mainSchema, tablePath, indexName);
        TablePath indexTablePath = IndexTableUtils.indexTablePath(tablePath, indexName);
        return SecondaryIndexLookuper.create(
                mainSchema,
                index.getColumnNames(),
                conn.getTable(indexTablePath),
                this.newLookup().createLookuper(),
                conn.getOrCreateLookupClient());
    }

    private static Schema.Index findIndexOrThrow(
            Schema schema, TablePath tablePath, String indexName) {
        for (Schema.Index i : schema.getIndexes()) {
            if (i.getIndexName().equals(indexName)) {
                return i;
            }
        }
        throw new IllegalArgumentException(
                "Unknown secondary index '" + indexName + "' on table " + tablePath);
    }

    @Override
    public void close() throws Exception {
        // do nothing
        schemaGetter.release();
    }
}
