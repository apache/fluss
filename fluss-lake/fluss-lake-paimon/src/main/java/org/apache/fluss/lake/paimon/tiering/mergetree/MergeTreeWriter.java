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

package org.apache.fluss.lake.paimon.tiering.mergetree;

import org.apache.fluss.lake.paimon.tiering.RecordWriter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.LogRecord;

import org.apache.paimon.KeyValue;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.RowKeyExtractor;
import org.apache.paimon.table.sink.TableWriteImpl;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

import static org.apache.fluss.lake.paimon.tiering.PaimonLakeTieringFactory.FLUSS_LAKE_TIERING_COMMIT_USER;
import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toRowKind;

/** A {@link RecordWriter} to write to Paimon's primary-key table. */
public class MergeTreeWriter extends RecordWriter<KeyValue> {

    // the option key to configure the temporary directory used by fluss tiering
    private static final String FLUSS_TIERING_TMP_DIR_KEY = "fluss.tiering.io-tmpdir";

    private final KeyValue keyValue = new KeyValue();

    private final RowKeyExtractor rowKeyExtractor;

    public MergeTreeWriter(
            FileStoreTable fileStoreTable,
            TableBucket tableBucket,
            @Nullable String partition,
            List<String> partitionKeys) {
        super(
                createTableWrite(fileStoreTable),
                fileStoreTable.rowType(),
                tableBucket,
                partition,
                partitionKeys);
        this.rowKeyExtractor = fileStoreTable.createRowKeyExtractor();
    }

    private static TableWriteImpl<KeyValue> createTableWrite(FileStoreTable fileStoreTable) {
        // we allow users to configure the temporary directory used by fluss tiering
        // since the default java.io.tmpdir may not be suitable.
        // currently, we don't expose the option, as a workaround way, maybe in the future we can
        // expose it if it's needed
        Map<String, String> props = fileStoreTable.options();
        String tmpDir =
                props.getOrDefault(FLUSS_TIERING_TMP_DIR_KEY, System.getProperty("java.io.tmpdir"));
        //noinspection unchecked
        return (TableWriteImpl<KeyValue>)
                fileStoreTable
                        .newWrite(FLUSS_LAKE_TIERING_COMMIT_USER)
                        .withIOManager(IOManager.create(tmpDir));
    }

    @Override
    public void write(LogRecord record) throws Exception {
        flussRecordAsPaimonRow.setFlussRecord(record);
        rowKeyExtractor.setRecord(flussRecordAsPaimonRow);
        keyValue.replace(
                rowKeyExtractor.trimmedPrimaryKey(),
                KeyValue.UNKNOWN_SEQUENCE,
                toRowKind(record.getChangeType()),
                flussRecordAsPaimonRow);
        // hacky, call internal method tableWrite.getWrite() to support
        // to write to given partition, otherwise, it'll always extract a partition from Paimon row
        // which may be costly
        tableWrite.getWrite().write(partition, bucket, keyValue);
    }
}
