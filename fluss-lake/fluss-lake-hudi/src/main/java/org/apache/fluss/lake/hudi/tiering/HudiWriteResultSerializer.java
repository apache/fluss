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

package org.apache.fluss.lake.hudi.tiering;

import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.utils.InstantiationUtils;

import org.apache.hudi.client.WriteStatus;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/** Serializer for {@link HudiWriteResult}. */
public class HudiWriteResultSerializer implements SimpleVersionedSerializer<HudiWriteResult> {

    private static final int CURRENT_VERSION = 1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(HudiWriteResult hudiWriteResult) throws IOException {
        byte[] writeResultBytes =
                InstantiationUtils.serializeObject(hudiWriteResult.getWriteStatuses());
        byte[] compactionWriteResultBytes =
                InstantiationUtils.serializeObject(hudiWriteResult.getCompactionWriteStatuses());

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeInt(writeResultBytes.length);
            dos.write(writeResultBytes);
            dos.writeInt(compactionWriteResultBytes.length);
            dos.write(compactionWriteResultBytes);
            return baos.toByteArray();
        }
    }

    @Override
    public HudiWriteResult deserialize(int version, byte[] serialized) throws IOException {
        if (version != CURRENT_VERSION) {
            throw new IOException(
                    "Unsupported HudiWriteResult version "
                            + version
                            + ", expected "
                            + CURRENT_VERSION);
        }

        Map<String, List<WriteStatus>> writeResult;
        Map<String, List<WriteStatus>> compactionWriteResult;
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(serialized))) {
            byte[] writeResultBytes = readBytes(dis, "WriteResult");
            writeResult =
                    InstantiationUtils.deserializeObject(
                            writeResultBytes, getClass().getClassLoader());

            byte[] compactionWriteResultBytes = readBytes(dis, "CompactionWriteResult");
            compactionWriteResult =
                    InstantiationUtils.deserializeObject(
                            compactionWriteResultBytes, getClass().getClassLoader());
            if (dis.available() > 0) {
                throw new IOException("Corrupted serialization: trailing bytes " + dis.available());
            }
        } catch (ClassNotFoundException e) {
            throw new IOException("Couldn't deserialize HudiWriteResult.", e);
        }
        return new HudiWriteResult(writeResult, compactionWriteResult);
    }

    private static byte[] readBytes(DataInputStream dis, String field) throws IOException {
        int length = dis.readInt();
        validateLength(length, dis.available(), field);
        byte[] bytes = new byte[length];
        dis.readFully(bytes);
        return bytes;
    }

    private static void validateLength(int length, int remainingLength, String field)
            throws IOException {
        if (length < 0 || length > remainingLength) {
            throw new IOException(
                    "Corrupted serialization: invalid " + field + " length " + length);
        }
    }
}
