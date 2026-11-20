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

package org.apache.fluss.lake.delta.source;

import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Serializer for {@link DeltaSplit}.
 *
 * <p>Version 1 writes the four JSON strings of the split and its partition values, each string as
 * its UTF-8 length followed by the bytes. {@code writeUTF} is not used because the scan state holds
 * the table schema, which can exceed its 64 KB limit.
 */
public class DeltaSplitSerializer implements SimpleVersionedSerializer<DeltaSplit> {

    private static final int VERSION_1 = 1;
    private static final int CURRENT_VERSION = VERSION_1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(DeltaSplit split) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            writeString(out, split.scanStateSchema());
            writeString(out, split.scanState());
            writeString(out, split.scanFileSchema());
            writeString(out, split.scanFile());
            out.writeInt(split.partition().size());
            for (String value : split.partition()) {
                writeString(out, value);
            }
        }
        return bytes.toByteArray();
    }

    @Override
    public DeltaSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION_1) {
            throw new IOException(
                    String.format(
                            "Unsupported DeltaSplit serializer version %s, the supported version is %s.",
                            version, VERSION_1));
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized))) {
            String scanStateSchema = readString(in);
            String scanState = readString(in);
            String scanFileSchema = readString(in);
            String scanFile = readString(in);
            int partitionSize = in.readInt();
            List<String> partition = new ArrayList<>(partitionSize);
            for (int i = 0; i < partitionSize; i++) {
                partition.add(readString(in));
            }
            return new DeltaSplit(scanStateSchema, scanState, scanFileSchema, scanFile, partition);
        }
    }

    private static void writeString(DataOutputStream out, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static String readString(DataInputStream in) throws IOException {
        byte[] bytes = new byte[in.readInt()];
        in.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
