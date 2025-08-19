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

package com.alibaba.fluss.lake.iceberg.tiering.writer;

import com.alibaba.fluss.lake.iceberg.tiering.RecordWriter;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.types.RowType;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.TaskWriter;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/** A {@link RecordWriter} to write to Iceberg's primary-key table. */
public class DeltaTaskWriter extends RecordWriter {

    public DeltaTaskWriter(
            Table icebergTable,
            RowType flussRowType,
            TableBucket tableBucket,
            @Nullable String partition,
            List<String> partitionKeys,
            FileFormat format,
            OutputFileFactory outputFileFactory,
            long targetFileSize) {
        super(
                createTaskWriter(icebergTable, format, outputFileFactory, targetFileSize),
                icebergTable.schema(),
                flussRowType,
                tableBucket,
                partition,
                partitionKeys);
    }

    private static TaskWriter<Record> createTaskWriter(
            Table icebergTable,
            FileFormat format,
            OutputFileFactory outputFileFactory,
            long targetFileSize) {
        int[] equalityFieldIds =
                icebergTable.schema().identifierFieldIds().stream()
                        .mapToInt(Integer::intValue)
                        .toArray();
        FileAppenderFactory<Record> appenderFactory =
                new GenericAppenderFactory(
                        icebergTable.schema(),
                        icebergTable.spec(),
                        equalityFieldIds,
                        icebergTable.schema(),
                        null);

        List<String> columns = new ArrayList<>();
        for (Integer fieldId : icebergTable.schema().identifierFieldIds()) {
            columns.add(icebergTable.schema().findField(fieldId).name());
        }
        Schema deleteSchema = icebergTable.schema().select(columns);
        return new GenericTaskDeltaWriter(
                icebergTable.schema(),
                deleteSchema,
                icebergTable.spec(),
                format,
                appenderFactory,
                outputFileFactory,
                icebergTable.io(),
                targetFileSize);
    }

    @Override
    public void write(LogRecord record) throws Exception {
        GenericTaskDeltaWriter deltaWriter = (GenericTaskDeltaWriter) taskWriter;
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
