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

package org.apache.fluss.lake.iceberg.tiering.writer;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toPartition;

/** A generic task writer to write {@link Record} for append-only table. * */
public class GenericRecordAppendOnlyWriter extends BaseTaskWriter<Record> {

    private final RollingFileWriter currentWriter;

    protected GenericRecordAppendOnlyWriter(
            Table icebergTable,
            FileFormat format,
            FileAppenderFactory<Record> appenderFactory,
            OutputFileFactory fileFactory,
            FileIO io,
            long targetFileSize,
            @Nullable String partitionName,
            int bucket) {
        super(icebergTable.spec(), format, appenderFactory, fileFactory, io, targetFileSize);
        currentWriter = new RollingFileWriter(toPartition(icebergTable, partitionName, bucket));
    }

    @Override
    public void write(Record record) throws IOException {
        currentWriter.write(record);
    }

    @Override
    public void close() throws IOException {
        currentWriter.close();
    }
}
