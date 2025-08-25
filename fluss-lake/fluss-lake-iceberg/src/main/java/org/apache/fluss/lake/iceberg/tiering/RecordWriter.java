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

package org.apache.fluss.lake.iceberg.tiering;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.types.RowType;

import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;

/** A base interface to write {@link LogRecord} to Iceberg. */
public abstract class RecordWriter implements AutoCloseable {

    protected final TaskWriter<Record> taskWriter;
    protected final Schema icebergSchema;
    protected final int bucket;
    protected final FlussRecordAsIcebergRecord flussRecordAsIcebergRecord;

    public RecordWriter(
            TaskWriter<Record> taskWriter,
            Schema icebergSchema,
            RowType flussRowType,
            TableBucket tableBucket) {
        this.taskWriter = taskWriter;
        this.icebergSchema = icebergSchema;
        this.bucket = tableBucket.getBucket();
        this.flussRecordAsIcebergRecord =
                new FlussRecordAsIcebergRecord(
                        tableBucket.getBucket(), icebergSchema, flussRowType);
    }

    public abstract void write(LogRecord record) throws Exception;

    public WriteResult complete() throws Exception {
        // Complete the task writer and get write result
        return taskWriter.complete();
    }

    @Override
    public void close() throws Exception {
        if (taskWriter != null) {
            taskWriter.close();
        }
    }
}
