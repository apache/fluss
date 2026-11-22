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

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Utilities to serialize Hudi write stats without Java object serialization. */
final class HudiWriteStatsSerde {

    private HudiWriteStatsSerde() {}

    static void writeStatsMap(DataOutputStream dos, Map<String, HudiWriteStats> statsByInstant)
            throws IOException {
        dos.writeInt(statsByInstant.size());
        for (Map.Entry<String, HudiWriteStats> entry : statsByInstant.entrySet()) {
            writeString(dos, entry.getKey());
            HudiWriteStats writeStats = entry.getValue();
            dos.writeLong(writeStats.getTotalErrorRecords());
            writeString(dos, toCommitMetadataJson(writeStats.getWriteStats()));
        }
    }

    static Map<String, HudiWriteStats> readStatsMap(DataInputStream dis, String field)
            throws IOException {
        int mapSize = dis.readInt();
        if (mapSize < 0 || mapSize > dis.available()) {
            throw new IOException("Corrupted serialization: invalid " + field + " size " + mapSize);
        }

        Map<String, HudiWriteStats> statsByInstant = new LinkedHashMap<>();
        for (int i = 0; i < mapSize; i++) {
            String instant = readString(dis, field + " instant");
            long totalErrorRecords = dis.readLong();
            if (totalErrorRecords < 0) {
                throw new IOException(
                        "Corrupted serialization: invalid "
                                + field
                                + " total error records "
                                + totalErrorRecords);
            }
            String commitMetadataJson = readString(dis, field + " write stats");
            statsByInstant.put(
                    instant,
                    new HudiWriteStats(
                            fromCommitMetadataJson(commitMetadataJson), totalErrorRecords));
        }
        return statsByInstant;
    }

    private static String toCommitMetadataJson(List<HoodieWriteStat> writeStats)
            throws IOException {
        HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
        for (HoodieWriteStat writeStat : writeStats) {
            commitMetadata.addWriteStat(writeStat.getPartitionPath(), writeStat);
        }
        return commitMetadata.toJsonString();
    }

    private static List<HoodieWriteStat> fromCommitMetadataJson(String commitMetadataJson)
            throws IOException {
        try {
            HoodieCommitMetadata commitMetadata =
                    HoodieCommitMetadata.fromJsonString(
                            commitMetadataJson, HoodieCommitMetadata.class);
            if (commitMetadata == null) {
                throw new IOException("Failed to deserialize Hudi commit metadata.");
            }
            return commitMetadata.getWriteStats();
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Failed to deserialize Hudi commit metadata.", e);
        }
    }

    private static void writeString(DataOutputStream dos, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        dos.writeInt(bytes.length);
        dos.write(bytes);
    }

    private static String readString(DataInputStream dis, String field) throws IOException {
        int length = dis.readInt();
        validateLength(length, dis.available(), field);
        byte[] bytes = new byte[length];
        dis.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static void validateLength(int length, int remainingLength, String field)
            throws IOException {
        if (length < 0 || length > remainingLength) {
            throw new IOException(
                    "Corrupted serialization: invalid " + field + " length " + length);
        }
    }
}
