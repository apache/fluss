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

import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.utils.InstantiationUtils;

import org.apache.iceberg.io.WriteResult;

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
        return InstantiationUtils.serializeObject(icebergWriteResult.getWriteResult());
    }

    @Override
    public IcebergWriteResult deserialize(int version, byte[] serialized) throws IOException {
        WriteResult writeResult;
        try {
            writeResult =
                    InstantiationUtils.deserializeObject(serialized, getClass().getClassLoader());
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
        return new IcebergWriteResult(writeResult);
    }
}
