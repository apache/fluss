/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.record.send;

import com.alibaba.fluss.record.bytesview.BytesView;
import com.alibaba.fluss.rpc.messages.ApiMessage;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.ByteBuf;

/** A writable output for writing {@link ApiMessage}. */
public interface WritableOutput {

    void writeBoolean(boolean val);

    void writeVarInt(int val);

    void writeVarInt64(long val);

    void writeSignedVarInt(int val);

    void writeSignedVarInt64(long val);

    void writeFixedInt32(int val);

    void writeFixedInt64(long val);

    void writeFloat(float val);

    void writeDouble(double val);

    void writeString(String val, int bytesCount);

    void writeByteArray(byte[] val, int offset, int length);

    void writeByteBuf(ByteBuf buf, int offset, int length);

    void writeBytes(BytesView val);
}
