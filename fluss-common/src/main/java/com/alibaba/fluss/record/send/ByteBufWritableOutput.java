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

package com.alibaba.fluss.record.send;

import com.alibaba.fluss.record.bytesview.BytesView;
import com.alibaba.fluss.utils.ProtoCodecUtils;

import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;

/**
 * A basic implementation of {@link WritableOutput} that writes data into a {@link ByteBuf}.
 *
 * <p>Note: Please make sure the allocated {@link ByteBuf} has enough space to write in.
 */
public class ByteBufWritableOutput implements WritableOutput {

    protected final ByteBuf buf;

    public ByteBufWritableOutput(ByteBuf buf) {
        this.buf = buf;
    }

    @Override
    public void writeBoolean(boolean val) {
        buf.writeBoolean(val);
    }

    @Override
    public void writeVarInt(int val) {
        ProtoCodecUtils.writeVarInt(buf, val);
    }

    @Override
    public void writeVarInt64(long val) {
        ProtoCodecUtils.writeVarInt64(buf, val);
    }

    @Override
    public void writeSignedVarInt(int val) {
        ProtoCodecUtils.writeSignedVarInt(buf, val);
    }

    @Override
    public void writeSignedVarInt64(long val) {
        ProtoCodecUtils.writeSignedVarInt64(buf, val);
    }

    @Override
    public void writeFixedInt32(int val) {
        ProtoCodecUtils.writeFixedInt32(buf, val);
    }

    @Override
    public void writeFixedInt64(long val) {
        ProtoCodecUtils.writeFixedInt64(buf, val);
    }

    @Override
    public void writeFloat(float val) {
        ProtoCodecUtils.writeFloat(buf, val);
    }

    @Override
    public void writeDouble(double val) {
        ProtoCodecUtils.writeDouble(buf, val);
    }

    @Override
    public void writeString(String val, int length) {
        ProtoCodecUtils.writeString(buf, val, length);
    }

    @Override
    public void writeByteArray(byte[] val, int offset, int length) {
        buf.writeBytes(val, offset, length);
    }

    @Override
    public void writeByteBuf(ByteBuf val, int offset, int length) {
        buf.writeBytes(val, offset, length);
    }

    @Override
    public void writeBytes(BytesView val) {
        buf.writeBytes(val.getByteBuf(), 0, val.getBytesLength());
    }
}
