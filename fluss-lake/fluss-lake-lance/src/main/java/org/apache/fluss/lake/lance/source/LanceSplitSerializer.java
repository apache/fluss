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

package org.apache.fluss.lake.lance.source;

import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Serializer for Lance split. */
public class LanceSplitSerializer implements SimpleVersionedSerializer<LanceSplit> {
    private static final int VERSION_1 = 1;

    @Override
    public int getVersion() {
        return VERSION_1;
    }

    @Override
    public byte[] serialize(LanceSplit split) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);

        out.writeInt(split.fragmentId());
        out.writeLong(split.snapshotId());
        out.writeLong(split.fragmentRows());
        out.writeLong(split.scanLimit());
        out.writeInt(split.bucket());

        List<String> partition = split.partition();
        out.writeInt(partition.size());
        for (String part : partition) {
            out.writeUTF(part == null ? "" : part);
        }
        out.flush();
        return baos.toByteArray();
    }

    @Override
    public LanceSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION_1) {
            throw new IOException("Unsupported LanceSplit serialization version: " + version);
        }

        DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized));
        int fragmentId = in.readInt();
        long snapshotId = in.readLong();
        long fragmentRows = in.readLong();
        long scanLimit = in.readLong();
        int bucket = in.readInt();

        int partitionSize = in.readInt();
        List<String> partition;
        if (partitionSize <= 0) {
            partition = Collections.emptyList();
        } else {
            partition = new ArrayList<String>(partitionSize);
            for (int i = 0; i < partitionSize; i++) {
                partition.add(in.readUTF());
            }
        }

        return new LanceSplit(fragmentId, snapshotId, fragmentRows, scanLimit, bucket, partition);
    }
}
