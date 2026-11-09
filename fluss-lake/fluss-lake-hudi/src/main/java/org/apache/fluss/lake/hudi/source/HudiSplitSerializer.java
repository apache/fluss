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

package org.apache.fluss.lake.hudi.source;

import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.utils.InstantiationUtils;

import java.io.IOException;

/** Serializer for {@link HudiSplit}. */
public class HudiSplitSerializer implements SimpleVersionedSerializer<HudiSplit> {

    private static final int CURRENT_VERSION = 1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(HudiSplit hudiSplit) throws IOException {
        return InstantiationUtils.serializeObject(hudiSplit);
    }

    @Override
    public HudiSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != CURRENT_VERSION) {
            throw new IOException("Unsupported HudiSplit serialization version: " + version);
        }
        try {
            Object deserialized =
                    InstantiationUtils.deserializeObject(serialized, getClass().getClassLoader());
            if (!(deserialized instanceof HudiSplit)) {
                throw new IOException(
                        "Expected HudiSplit but found "
                                + (deserialized == null
                                        ? "null"
                                        : deserialized.getClass().getName())
                                + " during deserialization.");
            }
            return (HudiSplit) deserialized;
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize HudiSplit.", e);
        }
    }
}
