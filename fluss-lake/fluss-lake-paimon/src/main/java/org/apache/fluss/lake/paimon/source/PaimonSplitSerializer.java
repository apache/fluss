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

package org.apache.fluss.lake.paimon.source;

import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;

import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.utils.InstantiationUtil;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;

/** Serializer for paimon split. */
public class PaimonSplitSerializer implements SimpleVersionedSerializer<PaimonSplit> {

    private static final int VERSION_1 = 1;

    @Override
    public int getVersion() {
        return VERSION_1;
    }

    @Override
    public byte[] serialize(PaimonSplit paimonSplit) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
        DataSplit dataSplit = paimonSplit.dataSplit();
        InstantiationUtil.serializeObject(view, dataSplit);
        view.writeBoolean(paimonSplit.isBucketUnAware());
        return out.toByteArray();
    }

    @Override
    public PaimonSplit deserialize(int version, byte[] serialized) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(serialized);
        DataSplit dataSplit;
        try {
            dataSplit = InstantiationUtil.deserializeObject(in, getClass().getClassLoader());

            if (version == VERSION_1) {
                DataInputStream dis = new DataInputStream(in);
                boolean isBucketUnAware = dis.readBoolean();
                return new PaimonSplit(dataSplit, isBucketUnAware);
            } else {
                throw new IOException("Unsupported PaimonSplit serialization version: " + version);
            }
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize PaimonSplit", e);
        }
    }
}
