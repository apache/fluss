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

package com.alibaba.fluss.flink.tiering.source.state;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

/** Serializer for {@link TieringSourceEnumeratorState}. */
public class TieringSourceEnumeratorStateSerializer
        implements SimpleVersionedSerializer<TieringSourceEnumeratorState> {

    public static final TieringSourceEnumeratorStateSerializer INSTANCE =
            new TieringSourceEnumeratorStateSerializer();

    private static final int VERSION = 0;
    private static final int CURRENT_VERSION = VERSION;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(TieringSourceEnumeratorState obj) throws IOException {
        // no need to store anything
        return new byte[0];
    }

    @Override
    public TieringSourceEnumeratorState deserialize(int version, byte[] serialized)
            throws IOException {
        // new state
        return new TieringSourceEnumeratorState();
    }
}
