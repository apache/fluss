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

package org.apache.fluss.lake.paimon.tiering.append;

import org.apache.fluss.lake.paimon.tiering.RecordWriter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.LogRecord;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TableWriteImpl;

import javax.annotation.Nullable;

import java.util.List;

import static org.apache.fluss.lake.paimon.tiering.PaimonLakeTieringFactory.FLUSS_LAKE_TIERING_COMMIT_USER;

/** A {@link RecordWriter} to write to Paimon's append-only table. */
public class AppendOnlyWriter extends RecordWriter<InternalRow> {

    private final FileStoreTable fileStoreTable;

    public AppendOnlyWriter(
            FileStoreTable fileStoreTable,
            TableBucket tableBucket,
            @Nullable String partition,
            List<String> partitionKeys) {
        //noinspection unchecked
        super(
                (TableWriteImpl<InternalRow>)
                        // todo: set ioManager to support write-buffer-spillable
                        fileStoreTable.newWrite(FLUSS_LAKE_TIERING_COMMIT_USER),
                fileStoreTable.rowType(),
                tableBucket,
                partition,
                partitionKeys); // Pass to parent
        this.fileStoreTable = fileStoreTable;
    }

    @Override
    public void write(LogRecord record) throws Exception {
        flussRecordAsPaimonRow.setFlussRecord(record);

        // get partition once
        if (partition == null) {
            partition = tableWrite.getPartition(flussRecordAsPaimonRow);
        }

        // hacky, call internal method tableWrite.getWrite() to support
        // to write to given partition, otherwise, it'll always extract a partition from Paimon row
        // which may be costly
        int writtenBucket = bucket;
        // if bucket-unaware mode, we have to use bucket = 0 to write to follow paimon best practice
        if (fileStoreTable.store().bucketMode() == BucketMode.BUCKET_UNAWARE) {
            writtenBucket = 0;
        }
        tableWrite.getWrite().write(partition, writtenBucket, flussRecordAsPaimonRow);
    }
}
