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

import java.io.IOException;

import static org.apache.fluss.lake.iceberg.utils.IcebergJsonSerdeUtils.fromJsonBytes;
import static org.apache.fluss.lake.iceberg.utils.IcebergJsonSerdeUtils.toJsonBytes;

/**
 * Iceberg 1.12 serializer for {@link IcebergSplit}. It replaces the class of the same name in
 * fluss-lake-iceberg-common.
 *
 * <p>Version 1 is Java serialization of the Iceberg classes of an earlier Iceberg plugin, which
 * Iceberg 1.12 cannot read, so this serializer rejects it. Version 2 is read and written as in
 * fluss-lake-iceberg-common.
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
        throw new IOException(
                "Cannot restore a lake split of serializer version 1 with the Iceberg 1.12 plugin, "
                        + "because it was written with Java serialization by an earlier Iceberg plugin. "
                        + "Restore the job with the Iceberg 1.10 plugin (fluss-lake-iceberg) "
                        + "of this Fluss version and take a savepoint, which writes version 2, "
                        + "then switch to the Iceberg 1.12 plugin.");
    }
}
