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
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** Serializer for {@link IcebergWriteResult}. */
public class IcebergWriteResultSerializer implements SimpleVersionedSerializer<IcebergWriteResult> {

    private static final int CURRENT_VERSION = 1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(IcebergWriteResult icebergWriteResult) throws IOException {
        byte[] writeResultBytes =
                InstantiationUtils.serializeObject(icebergWriteResult.getWriteResult());

        RewriteDataFileResult rewriteDataFileResult = icebergWriteResult.rewriteDataFileResult();
        byte[] rewriteResultBytes =
                rewriteDataFileResult == null
                        ? null
                        : InstantiationUtils.serializeObject(rewriteDataFileResult);

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(writeResultBytes.length);
                DataOutputStream dos = new DataOutputStream(baos)) {
            // Frame WriteResult
            dos.writeInt(writeResultBytes.length);
            dos.write(writeResultBytes);

            // optional, rewrite
            boolean hasRewrite = rewriteResultBytes != null;
            dos.writeBoolean(hasRewrite);
            if (hasRewrite) {
                dos.writeInt(rewriteResultBytes.length);
                dos.write(rewriteResultBytes);
            }
            return baos.toByteArray();
        }
    }

    @Override
    public IcebergWriteResult deserialize(int version, byte[] serialized) throws IOException {
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
        return new IcebergWriteResult(writeResult, rewriteDataFileResult);
    }
}
