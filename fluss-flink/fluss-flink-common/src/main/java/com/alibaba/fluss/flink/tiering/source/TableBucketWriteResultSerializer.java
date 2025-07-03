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

package com.alibaba.fluss.flink.tiering.source;

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;

/** The serializer for {@link TableBucketWriteResult}. */
public class TableBucketWriteResultSerializer<WriteResult>
        implements SimpleVersionedSerializer<TableBucketWriteResult<WriteResult>> {

    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));

    private static final int CURRENT_VERSION = 1;

    private final com.alibaba.fluss.lake.serializer.SimpleVersionedSerializer<WriteResult>
            writeResultSerializer;

    public TableBucketWriteResultSerializer(
            com.alibaba.fluss.lake.serializer.SimpleVersionedSerializer<WriteResult>
                    writeResultSerializer) {
        this.writeResultSerializer = writeResultSerializer;
    }

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(TableBucketWriteResult<WriteResult> tableBucketWriteResult)
            throws IOException {
        final DataOutputSerializer out = SERIALIZER_CACHE.get();
        // serialize table path
        TablePath tablePath = tableBucketWriteResult.tablePath();
        out.writeUTF(tablePath.getDatabaseName());
        out.writeUTF(tablePath.getTableName());

        // serialize bucket
        TableBucket tableBucket = tableBucketWriteResult.tableBucket();
        out.writeLong(tableBucket.getTableId());
        // write partition
        if (tableBucket.getPartitionId() != null) {
            out.writeBoolean(true);
            out.writeLong(tableBucket.getPartitionId());
        } else {
            out.writeBoolean(false);
        }
        out.writeInt(tableBucket.getBucket());

        // serialize write result
        WriteResult writeResult = tableBucketWriteResult.writeResult();
        if (writeResult == null) {
            // write -1 to mark write result as null
            out.writeInt(-1);
        } else {
            byte[] serializeBytes = writeResultSerializer.serialize(writeResult);
            out.writeInt(serializeBytes.length);
            out.write(serializeBytes);
        }

        // serialize log end offset
        out.writeLong(tableBucketWriteResult.logEndOffset());

        // serialize number of write results
        out.writeInt(tableBucketWriteResult.numberOfWriteResults());

        final byte[] result = out.getCopyOfBuffer();
        out.clear();
        return result;
    }

    @Override
    public TableBucketWriteResult<WriteResult> deserialize(int version, byte[] serialized)
            throws IOException {
        if (version != CURRENT_VERSION) {
            throw new IOException("Unknown version " + version);
        }
        final DataInputDeserializer in = new DataInputDeserializer(serialized);
        // deserialize table path
        String databaseName = in.readUTF();
        String tableName = in.readUTF();
        TablePath tablePath = new TablePath(databaseName, tableName);

        // deserialize bucket
        long tableId = in.readLong();
        Long partitionId = null;
        if (in.readBoolean()) {
            partitionId = in.readLong();
        }
        int bucketId = in.readInt();
        TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);

        // deserialize write result
        int writeResultLength = in.readInt();
        WriteResult writeResult;
        if (writeResultLength >= 0) {
            byte[] writeResultBytes = new byte[writeResultLength];
            in.readFully(writeResultBytes);
            writeResult = writeResultSerializer.deserialize(version, writeResultBytes);
        } else {
            writeResult = null;
        }

        // deserialize log end offset
        long logEndOffset = in.readLong();
        // deserialize number of write results
        int numberOfWriteResults = in.readInt();
        return new TableBucketWriteResult<>(
                tablePath, tableBucket, writeResult, logEndOffset, numberOfWriteResults);
    }
}
