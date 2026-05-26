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

import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/** The {@link SimpleVersionedSerializer} for {@link PaimonWriteResult}. */
public class PaimonWriteResultSerializer implements SimpleVersionedSerializer<PaimonWriteResult> {

    private static final int CURRENT_VERSION = 2;

    private final CommitMessageSerializer messageSer = new CommitMessageSerializer();

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(PaimonWriteResult paimonWriteResult) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);

        Long watermark = paimonWriteResult.getWatermark();
        if (watermark == null) {
            view.writeBoolean(false);
        } else {
            view.writeBoolean(true);
            view.writeLong(watermark);
        }

        byte[] messageBytes = messageSer.serialize(paimonWriteResult.commitMessage());
        view.writeInt(messageBytes.length);
        view.write(messageBytes);

        return out.toByteArray();
    }

    @Override
    public PaimonWriteResult deserialize(int version, byte[] serialized) throws IOException {
        switch (version) {
            case 1:
                {
                    CommitMessage commitMessage =
                            messageSer.deserialize(messageSer.getVersion(), serialized);
                    return new PaimonWriteResult(commitMessage, null);
                }
            case 2:
                {
                    DataInputView view = new DataInputDeserializer(serialized);

                    Long watermark = null;
                    boolean watermarkNonNull = view.readBoolean();
                    if (watermarkNonNull) {
                        watermark = view.readLong();
                    }

                    int len = view.readInt();
                    byte[] messageBytes = new byte[len];
                    view.read(messageBytes);
                    CommitMessage commitMessage =
                            messageSer.deserialize(messageSer.getVersion(), messageBytes);

                    return new PaimonWriteResult(commitMessage, watermark);
                }
            default:
                throw new UnsupportedOperationException(
                        "Expecting PaimonWriteResult version to be less than or equal to "
                                + CURRENT_VERSION
                                + ", but found "
                                + version
                                + ".");
        }
    }
}
