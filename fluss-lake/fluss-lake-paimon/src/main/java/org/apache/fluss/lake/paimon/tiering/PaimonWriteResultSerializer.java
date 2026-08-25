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

package org.apache.fluss.lake.paimon.tiering;

import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;

import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;

import java.io.IOException;

/**
 * Serializer for {@link PaimonWriteResult}.
 *
 * <p>Each Fluss serializer version maps to one Paimon {@link CommitMessageSerializer} version:
 * Fluss version 1 uses Paimon version 11 (Paimon 1.4.2), and Fluss version 2 uses Paimon version 12
 * (Paimon 2.0.0). The Paimon serializer version is not stored in the payload. When Paimon changes
 * its serializer version, a new Fluss serializer version and mapping must be added.
 */
public class PaimonWriteResultSerializer implements SimpleVersionedSerializer<PaimonWriteResult> {

    private static final int VERSION_1 = 1;
    private static final int VERSION_2 = 2;
    private static final int VERSION_1_PAIMON_SERIALIZER_VERSION = 11;
    private static final int VERSION_2_PAIMON_SERIALIZER_VERSION = 12;

    private final CommitMessageSerializer messageSer = new CommitMessageSerializer();

    @Override
    public int getVersion() {
        return VERSION_2;
    }

    @Override
    public byte[] serialize(PaimonWriteResult paimonWriteResult) throws IOException {
        int expectedPaimonVersion = getPaimonSerializerVersion(getVersion());
        if (messageSer.getVersion() != expectedPaimonVersion) {
            throw new IOException(
                    "Paimon CommitMessage version "
                            + messageSer.getVersion()
                            + " requires a new PaimonWriteResult version.");
        }
        CommitMessage commitMessage = paimonWriteResult.commitMessage();
        return messageSer.serialize(commitMessage);
    }

    @Override
    public PaimonWriteResult deserialize(int version, byte[] serialized) throws IOException {
        int paimonSerializerVersion = getPaimonSerializerVersion(version);
        CommitMessage commitMessage = messageSer.deserialize(paimonSerializerVersion, serialized);
        return new PaimonWriteResult(commitMessage);
    }

    private int getPaimonSerializerVersion(int flussVersion) throws IOException {
        if (flussVersion == VERSION_1) {
            return VERSION_1_PAIMON_SERIALIZER_VERSION;
        } else if (flussVersion == VERSION_2) {
            return VERSION_2_PAIMON_SERIALIZER_VERSION;
        }
        throw new IOException("Unsupported PaimonWriteResult version: " + flussVersion);
    }
}
