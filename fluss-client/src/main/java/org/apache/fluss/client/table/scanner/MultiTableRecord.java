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

package org.apache.fluss.client.table.scanner;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.client.table.scanner.log.MultiTableLogScanner;
import org.apache.fluss.client.table.writer.MultiTableWriter;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.InternalRow;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * A record produced by a {@link MultiTableLogScanner}. Wraps an unchanged {@link ScanRecord} and
 * enriches it with the source table identity (path, id) and schema (schema object, schema id).
 *
 * <p>The underlying {@link ScanRecord} structure is NOT modified.
 *
 * <p>Read-side only. For writes via {@link MultiTableWriter}, use {@link
 * org.apache.fluss.client.table.writer.MultiTableWriteRecord} which carries only the write-relevant
 * fields.
 *
 * @since 0.7
 */
@PublicEvolving
public final class MultiTableRecord {

    private final TablePath tablePath;
    private final long tableId;
    private final Schema schema;
    private final int schemaId;
    private final ScanRecord scanRecord;

    public MultiTableRecord(
            TablePath tablePath, long tableId, int schemaId, Schema schema, ScanRecord scanRecord) {
        this.tablePath = checkNotNull(tablePath, "tablePath");
        this.tableId = tableId;
        this.schema = checkNotNull(schema, "schema");
        this.schemaId = schemaId;
        this.scanRecord = checkNotNull(scanRecord, "scanRecord");
    }

    // ---- table identity ----

    public TablePath getTablePath() {
        return tablePath;
    }

    public long getTableId() {
        return tableId;
    }

    // ---- schema info ----

    public Schema getSchema() {
        return schema;
    }

    public int getSchemaId() {
        return schemaId;
    }

    // ---- underlying record (unchanged) ----

    public ScanRecord getScanRecord() {
        return scanRecord;
    }

    // ---- convenience delegates ----

    public long logOffset() {
        return scanRecord.logOffset();
    }

    public long timestamp() {
        return scanRecord.timestamp();
    }

    public ChangeType getChangeType() {
        return scanRecord.getChangeType();
    }

    public InternalRow getRow() {
        return scanRecord.getRow();
    }

    @Override
    public String toString() {
        return "MultiTableRecord{"
                + "tablePath="
                + tablePath
                + ", tableId="
                + tableId
                + ", schemaId="
                + schemaId
                + ", scanRecord="
                + scanRecord
                + '}';
    }
}
