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

package org.apache.fluss.flink.tiering.source;

import org.apache.fluss.client.tiering.TableBucketWriteResult;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;

/**
 * The serializer for {@link TableBucketWriteResult}.
 *
 * <h3>Version Evolution:</h3>
 *
 * <ul>
 *   <li><b>Version 1:</b> Initial version. The version of the lake write result serializer was not
 *       persisted; the write result was deserialized with this serializer's version instead.
 *   <li><b>Version 2 (Current):</b> Persists the version of the lake write result serializer before
 *       the write result bytes and passes it to the lake serializer on deserialization, so that
 *       lake formats can evolve their write result format independently.
 * </ul>
 */
public class TableBucketWriteResultSerializer<WriteResult>
        implements SimpleVersionedSerializer<TableBucketWriteResult<WriteResult>> {

    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));

    private static final int VERSION_1 = 1;
    private static final int VERSION_2 = 2;

    private static final int CURRENT_VERSION = VERSION_2;

    private final org.apache.fluss.lake.serializer.SimpleVersionedSerializer<WriteResult>
            writeResultSerializer;

    public TableBucketWriteResultSerializer(
            org.apache.fluss.lake.serializer.SimpleVersionedSerializer<WriteResult>
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
            out.writeUTF(tableBucketWriteResult.partitionName());
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
            // since VERSION_2
            out.writeInt(writeResultSerializer.getVersion());
            out.write(serializeBytes);
        }

        // serialize log end offset
        out.writeLong(tableBucketWriteResult.logEndOffset());

        // serialize max timestamp
        out.writeLong(tableBucketWriteResult.maxTimestamp());

        // serialize number of write results
        out.writeInt(tableBucketWriteResult.numberOfWriteResults());

        final byte[] result = out.getCopyOfBuffer();
        out.clear();
        return result;
    }

    @Override
    public TableBucketWriteResult<WriteResult> deserialize(int version, byte[] serialized)
            throws IOException {
        if (version != VERSION_1 && version != VERSION_2) {
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
        String partitionName = null;
        if (in.readBoolean()) {
            partitionId = in.readLong();
            partitionName = in.readUTF();
        }
        int bucketId = in.readInt();
        TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);

        // deserialize write result
        int writeResultLength = in.readInt();
        WriteResult writeResult;
        if (writeResultLength >= 0) {
            // VERSION_1 did not persist the lake serializer version and passed its own version
            // to the lake serializer, keep that behavior for data written with VERSION_1
            int writeResultVersion = version >= VERSION_2 ? in.readInt() : VERSION_1;
            byte[] writeResultBytes = new byte[writeResultLength];
            in.readFully(writeResultBytes);
            writeResult = writeResultSerializer.deserialize(writeResultVersion, writeResultBytes);
        } else {
            writeResult = null;
        }

        // deserialize log end offset
        long logEndOffset = in.readLong();
        // deserialize max timestamp
        long maxTimestamp = in.readLong();
        // deserialize number of write results
        int numberOfWriteResults = in.readInt();
        return new TableBucketWriteResult<>(
                tablePath,
                tableBucket,
                partitionName,
                writeResult,
                logEndOffset,
                maxTimestamp,
                numberOfWriteResults);
    }
}
