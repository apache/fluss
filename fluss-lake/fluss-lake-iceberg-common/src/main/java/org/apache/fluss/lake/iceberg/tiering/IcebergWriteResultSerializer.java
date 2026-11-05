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

package org.apache.fluss.lake.iceberg.tiering;

import org.apache.fluss.lake.iceberg.maintenance.RewriteDataFileResult;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.utils.InstantiationUtils;

import org.apache.iceberg.io.WriteResult;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import static org.apache.fluss.lake.iceberg.utils.IcebergJsonSerdeUtils.fromJsonBytes;
import static org.apache.fluss.lake.iceberg.utils.IcebergJsonSerdeUtils.toJsonBytes;

/**
 * Serializer for {@link IcebergWriteResult}.
 *
 * <h3>Version Evolution:</h3>
 *
 * <ul>
 *   <li><b>Version 1:</b> Initial version. Java serialization of the {@link WriteResult} and the
 *       optional {@link RewriteDataFileResult}, which can only be read by the Iceberg version that
 *       wrote them.
 *   <li><b>Version 2 (Current):</b> A JSON document written by {@link IcebergWriteResultJsonSerde},
 *       so that the write result can be read across Iceberg versions.
 * </ul>
 *
 * <p>This serializer can deserialize write results of all previous versions, but always produces
 * the current version during serialization.
 */
public class IcebergWriteResultSerializer implements SimpleVersionedSerializer<IcebergWriteResult> {

    private static final int VERSION_1 = 1;
    private static final int VERSION_2 = 2;
    private static final int CURRENT_VERSION = VERSION_2;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(IcebergWriteResult value) throws IOException {
        return toJsonBytes(value, IcebergWriteResultJsonSerde.INSTANCE);
    }

    @Override
    public IcebergWriteResult deserialize(int version, byte[] serialized) throws IOException {
        switch (version) {
            case VERSION_1:
                return deserializeV1(serialized);
            case VERSION_2:
                return fromJsonBytes(
                        serialized, IcebergWriteResultJsonSerde.INSTANCE, "IcebergWriteResult");
            default:
                throw new IOException(
                        String.format(
                                "Unsupported IcebergWriteResult serializer version %s, the supported versions are %s and %s.",
                                version, VERSION_1, VERSION_2));
        }
    }

    private IcebergWriteResult deserializeV1(byte[] serialized) throws IOException {
        WriteResult writeResult;
        RewriteDataFileResult rewriteDataFileResult;
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(serialized))) {
            int wrLen = dis.readInt();
            if (wrLen < 0 || wrLen > serialized.length) {
                throw new IOException(
                        "Corrupted serialization: invalid WriteResult length " + wrLen);
            }
            byte[] wrBytes = new byte[wrLen];
            dis.readFully(wrBytes);
            writeResult =
                    InstantiationUtils.deserializeObject(wrBytes, getClass().getClassLoader());

            boolean hasCompaction = dis.readBoolean();
            if (hasCompaction) {
                int crLen = dis.readInt();
                if (crLen < 0 || crLen > serialized.length) {
                    throw new IOException(
                            "Corrupted serialization: invalid compactionResult length " + crLen);
                }
                byte[] crBytes = new byte[crLen];
                dis.readFully(crBytes);
                rewriteDataFileResult =
                        InstantiationUtils.deserializeObject(crBytes, getClass().getClassLoader());
            } else {
                rewriteDataFileResult = null;
            }
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
        // version 1 did not persist the partition specs
        return new IcebergWriteResult(writeResult, null, rewriteDataFileResult);
    }
}
