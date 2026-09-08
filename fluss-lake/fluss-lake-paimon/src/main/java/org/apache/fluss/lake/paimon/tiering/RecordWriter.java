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

package org.apache.fluss.lake.paimon.tiering;

import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.LogRecord;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.List;

import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimonPartition;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/** A base interface to write {@link LogRecord} to Paimon. */
public abstract class RecordWriter<T> implements AutoCloseable {

    protected final TableWriteImpl<T> tableWrite;
    protected final RowType tableRowType;
    protected final int bucket;
    protected final List<String> partitionKeys;
    protected final boolean historicalPartition;
    // Null for historical writers, which derive the original partition from each record.
    protected final @Nullable BinaryRow fixedPartition;
    protected final FlussRecordAsPaimonRow flussRecordAsPaimonRow;

    public RecordWriter(
            TableWriteImpl<T> tableWrite,
            RowType tableRowType,
            TableBucket tableBucket,
            @Nullable String partition,
            List<String> partitionKeys,
            org.apache.fluss.types.RowType flussRowType,
            boolean paimonIncludingSystemColumns,
            boolean historicalPartition) {
        this.tableWrite = tableWrite;
        this.tableRowType = tableRowType;
        this.bucket = tableBucket.getBucket();
        this.partitionKeys = partitionKeys;
        this.historicalPartition = historicalPartition;
        if (historicalPartition) {
            this.fixedPartition = null;
        } else if (partition == null || partitionKeys.isEmpty()) {
            // non-partitioned table
            this.fixedPartition = BinaryRow.EMPTY_ROW;
        } else {
            // eagerly resolve BinaryRow partition from partition name string
            this.fixedPartition = resolvePartition(partition, partitionKeys, flussRowType);
        }
        this.flussRecordAsPaimonRow =
                new FlussRecordAsPaimonRow(
                        tableBucket.getBucket(), tableRowType, paimonIncludingSystemColumns);
    }

    public abstract void write(LogRecord record) throws Exception;

    List<CommitMessage> complete() throws Exception {
        List<CommitMessage> commitMessages = tableWrite.prepareCommit();
        // A normal writer targets one fixed partition, while a historical writer may write to
        // multiple original partitions and therefore produce multiple commit messages.
        if (!historicalPartition) {
            checkState(
                    commitMessages.size() == 1,
                    "The size of CommitMessage must be 1, but got %s.",
                    commitMessages);
        }
        return commitMessages;
    }

    public void close() throws Exception {
        tableWrite.close();
    }

    /** Sets the current Fluss record and returns the Paimon partition it should be written to. */
    protected BinaryRow prepareRecordAndGetPartition(LogRecord record) {
        flussRecordAsPaimonRow.setFlussRecord(record);
        return historicalPartition
                ? tableWrite.getPartition(flussRecordAsPaimonRow)
                : checkNotNull(fixedPartition);
    }

    /**
     * Resolves a Paimon {@link BinaryRow} partition from the partition name string by parsing each
     * partition value to its typed Fluss representation, constructing a synthetic row, and
     * delegating to Paimon's partition extraction.
     */
    private BinaryRow resolvePartition(
            String partitionName,
            List<String> partitionKeys,
            org.apache.fluss.types.RowType flussRowType) {
        ResolvedPartitionSpec spec =
                ResolvedPartitionSpec.fromPartitionName(partitionKeys, partitionName);
        return toPaimonPartition(spec, flussRowType, tableRowType, tableWrite::getPartition);
    }
}
