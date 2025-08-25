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

import org.apache.fluss.lake.writer.WriterInitContext;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;

import java.io.IOException;

import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toPartition;

/** A generic task equality delta writer. * */
class GenericRecordDeltaWriter extends BaseTaskWriter<Record> {
    private final GenericEqualityDeltaWriter deltaWriter;

    public GenericRecordDeltaWriter(
            Table icebergTable,
            Schema deleteSchema,
            FileFormat format,
            FileAppenderFactory<Record> appenderFactory,
            OutputFileFactory fileFactory,
            FileIO io,
            long targetFileSize,
            WriterInitContext writerInitContext) {
        super(icebergTable.spec(), format, appenderFactory, fileFactory, io, targetFileSize);
        this.deltaWriter =
                new GenericEqualityDeltaWriter(
                        toPartition(
                                icebergTable,
                                writerInitContext.partition(),
                                writerInitContext.tableBucket().getBucket()),
                        icebergTable.schema(),
                        deleteSchema);
    }

    @Override
    public void write(Record row) throws IOException {
        deltaWriter.write(row);
    }

    public void delete(Record row) throws IOException {
        deltaWriter.delete(row);
    }

    @Override
    public void close() throws IOException {
        deltaWriter.close();
    }

    private class GenericEqualityDeltaWriter extends BaseEqualityDeltaWriter {
        private GenericEqualityDeltaWriter(
                PartitionKey partition, Schema schema, Schema eqDeleteSchema) {
            super(partition, schema, eqDeleteSchema);
        }

        @Override
        protected StructLike asStructLike(Record record) {
            return record;
        }

        @Override
        protected StructLike asStructLikeKey(Record record) {
            return record;
        }
    }
}
