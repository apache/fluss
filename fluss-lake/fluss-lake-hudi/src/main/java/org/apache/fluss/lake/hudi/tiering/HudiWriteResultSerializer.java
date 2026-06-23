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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
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
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos)) {
            HudiWriteStatsSerde.writeStatsMap(dos, hudiWriteResult.getWriteStats());
            HudiWriteStatsSerde.writeStatsMap(dos, hudiWriteResult.getCompactionWriteStats());
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

        Map<String, HudiWriteStats> writeStats;
        Map<String, HudiWriteStats> compactionWriteStats;
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(serialized))) {
            writeStats = HudiWriteStatsSerde.readStatsMap(dis, "WriteStats");
            compactionWriteStats = HudiWriteStatsSerde.readStatsMap(dis, "CompactionWriteStats");
            if (dis.available() > 0) {
                throw new IOException("Corrupted serialization: trailing bytes " + dis.available());
            }
        }
        return new HudiWriteResult(writeStats, compactionWriteStats);
    }
}
