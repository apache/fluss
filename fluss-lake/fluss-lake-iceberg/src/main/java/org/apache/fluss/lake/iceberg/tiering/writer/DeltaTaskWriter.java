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

package org.apache.fluss.lake.iceberg.tiering.writer;

import org.apache.fluss.lake.iceberg.tiering.RecordWriter;
import org.apache.fluss.lake.writer.WriterInitContext;
import org.apache.fluss.record.LogRecord;

import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.TaskWriter;

/** A {@link RecordWriter} to write to Iceberg's primary-key table. */
public class DeltaTaskWriter extends RecordWriter {

    public DeltaTaskWriter(
            Table icebergTable,
            WriterInitContext writerInitContext,
            TaskWriter<Record> taskWriter) {
        super(
                taskWriter,
                icebergTable.schema(),
                writerInitContext.tableInfo().getRowType(),
                writerInitContext.tableBucket());
    }

    @Override
    public void write(LogRecord record) throws Exception {
        GenericRecordDeltaWriter deltaWriter = (GenericRecordDeltaWriter) taskWriter;
        flussRecordAsIcebergRecord.setFlussRecord(record);
        switch (record.getChangeType()) {
            case INSERT:
            case UPDATE_AFTER:
                deltaWriter.write(flussRecordAsIcebergRecord);
                break;
            case UPDATE_BEFORE:
            case DELETE:
                // TODO we can project the record and only write the equality delete fields
                deltaWriter.delete(flussRecordAsIcebergRecord);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unknown row kind: " + record.getChangeType());
        }
    }
}
