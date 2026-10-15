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

package org.apache.fluss.lake.iceberg.source;

import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.utils.InstantiationUtils;

import java.io.IOException;

import static org.apache.fluss.lake.iceberg.utils.IcebergJsonSerdeUtils.fromJsonBytes;
import static org.apache.fluss.lake.iceberg.utils.IcebergJsonSerdeUtils.toJsonBytes;

/**
 * Serializer for {@link IcebergSplit}.
 *
 * <h3>Version Evolution:</h3>
 *
 * <ul>
 *   <li><b>Version 1:</b> Initial version. Java serialization of {@link IcebergSplit}, which can
 *       only be read by the Iceberg version that wrote it.
 *   <li><b>Version 2 (Current):</b> A JSON document written by {@link IcebergSplitJsonSerde}, so
 *       that the split can be read across Iceberg versions.
 * </ul>
 *
 * <p>This serializer can deserialize splits of all previous versions, but always produces the
 * current version during serialization.
 */
public class IcebergSplitSerializer implements SimpleVersionedSerializer<IcebergSplit> {

    private static final int VERSION_1 = 1;
    private static final int VERSION_2 = 2;
    private static final int CURRENT_VERSION = VERSION_2;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(IcebergSplit icebergSplit) throws IOException {
        return toJsonBytes(icebergSplit, IcebergSplitJsonSerde.INSTANCE);
    }

    @Override
    public IcebergSplit deserialize(int version, byte[] serialized) throws IOException {
        switch (version) {
            case VERSION_1:
                return deserializeV1(serialized);
            case VERSION_2:
                return fromJsonBytes(serialized, IcebergSplitJsonSerde.INSTANCE, "IcebergSplit");
            default:
                throw new IOException(
                        String.format(
                                "Unsupported IcebergSplit serializer version %s, the supported versions are %s and %s.",
                                version, VERSION_1, VERSION_2));
        }
    }

    private IcebergSplit deserializeV1(byte[] serialized) throws IOException {
        try {
            return InstantiationUtils.deserializeObject(serialized, getClass().getClassLoader());
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }
}
