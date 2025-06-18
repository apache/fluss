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

package com.alibaba.fluss.client.table.writer;

import com.alibaba.fluss.client.table.getter.PartitionGetter;
import com.alibaba.fluss.client.write.WriteRecord;
import com.alibaba.fluss.client.write.WriterClient;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

/** A base class for {@link AppendWriter} and {@link UpsertWriter} to write data to table. */
public abstract class AbstractTableWriter implements TableWriter {

    // the table path that the data will write to
    protected final TablePath tablePath;
    protected final WriterClient writerClient;
    protected final int fieldCount;
    private final @Nullable PartitionGetter partitionFieldGetter;

    protected AbstractTableWriter(
            TablePath tablePath, TableInfo tableInfo, WriterClient writerClient) {
        this.tablePath = tablePath;
        this.writerClient = writerClient;
        this.fieldCount = tableInfo.getRowType().getFieldCount();
        this.partitionFieldGetter =
                tableInfo.isPartitioned()
                        ? new PartitionGetter(tableInfo.getRowType(), tableInfo.getPartitionKeys())
                        : null;
    }

    /**
     * Flush data written that have not yet been sent to the server, forcing the client to send the
     * requests to server and blocks on the completion of the requests associated with these
     * records. A request is considered completed when it is successfully acknowledged according to
     * the {@link ConfigOptions#CLIENT_WRITER_ACKS} configuration you have specified or else it
     * results in an error.
     */
    public void flush() {
        writerClient.flush();
    }

    protected CompletableFuture<Void> send(WriteRecord record) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        writerClient.send(
                record,
                (exception) -> {
                    if (exception == null) {
                        future.complete(null);
                    } else {
                        future.completeExceptionally(exception);
                    }
                });
        return future;
    }

    protected PhysicalTablePath getPhysicalPath(InternalRow row) {
        // not partitioned table, return the original physical path
        if (partitionFieldGetter == null) {
            return PhysicalTablePath.of(tablePath);
        } else {
            // partitioned table, extract partition from the row
            String partition = partitionFieldGetter.getPartition(row);
            return PhysicalTablePath.of(tablePath, partition);
        }
    }

    protected void checkFieldCount(InternalRow row) {
        if (row.getFieldCount() != fieldCount) {
            throw new IllegalArgumentException(
                    "The field count of the row does not match the table schema. "
                            + "Expected: "
                            + fieldCount
                            + ", Actual: "
                            + row.getFieldCount());
        }
    }
}
