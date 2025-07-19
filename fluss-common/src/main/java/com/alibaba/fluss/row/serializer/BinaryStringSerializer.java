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

package com.alibaba.fluss.row.serializer;

import com.alibaba.fluss.memory.InputView;
import com.alibaba.fluss.memory.OutputView;
import com.alibaba.fluss.row.BinarySegmentUtils;
import com.alibaba.fluss.row.BinaryString;

import java.io.IOException;

import static com.alibaba.fluss.utils.VarLengthIntUtils.decodeInt;
import static com.alibaba.fluss.utils.VarLengthIntUtils.encodeInt;

/** Serializer for {@link BinaryString}. */
public final class BinaryStringSerializer extends SerializerSingleton<BinaryString> {

    private static final long serialVersionUID = 1L;

    public static final BinaryStringSerializer INSTANCE = new BinaryStringSerializer();

    private BinaryStringSerializer() {}

    @Override
    public BinaryString copy(BinaryString from) {
        // BinaryString is the only implementation of BinaryString
        return from.copy();
    }

    @Override
    public void serialize(BinaryString string, OutputView target) throws IOException {
        encodeInt(target, string.getSizeInBytes());
        BinarySegmentUtils.copyToView(
                string.getSegments(), string.getOffset(), string.getSizeInBytes(), target);
    }

    @Override
    public BinaryString deserialize(InputView source) throws IOException {
        return deserializeInternal(source);
    }

    public static BinaryString deserializeInternal(InputView source) throws IOException {
        int length = decodeInt(source);
        byte[] bytes = new byte[length];
        source.readFully(bytes);
        return BinaryString.fromBytes(bytes);
    }

    @Override
    public String serializeToString(BinaryString record) {
        return record.toString();
    }

    @Override
    public BinaryString deserializeFromString(String s) {
        return BinaryString.fromString(s);
    }
}
