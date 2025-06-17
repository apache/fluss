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

package com.alibaba.fluss.protogen.generator.generator;

import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.record.bytesview.ByteBufBytesView;
import com.alibaba.fluss.record.bytesview.BytesView;
import com.alibaba.fluss.record.bytesview.FileRegionBytesView;
import com.alibaba.fluss.record.bytesview.MemorySegmentBytesView;

import io.protostuff.parser.Field;

import java.io.PrintWriter;

/** Generator for a byte[] records field. */
public class ProtobufRecordsField extends ProtobufField<Field.Bytes> {

    private static final String BYTES_VIEW_CLASS = BytesView.class.getSimpleName();

    public ProtobufRecordsField(Field.Bytes field, int index) {
        super(field, index, false);
    }

    @Override
    public void declaration(PrintWriter w) {
        w.format("private %s %s = null;\n", BYTES_VIEW_CLASS, ccName);
        w.format("private int _%sIdx = -1;\n", ccName);
        w.format("private int _%sLen = -1;\n", ccName);
    }

    @Override
    public void parse(PrintWriter w) {
        w.format("_%sLen = ProtoCodecUtils.readVarInt(_buffer);\n", ccName);
        w.format("_%sIdx = _buffer.readerIndex();\n", ccName);
        w.format("_buffer.skipBytes(_%sLen);\n", ccName);
    }

    @Override
    public void copy(PrintWriter w) {
        w.format(
                "%s(_other.%s());\n",
                ProtoGenUtil.camelCase("set", ccName), ProtoGenUtil.camelCase("get", ccName));
    }

    @Override
    public void setter(PrintWriter w, String enclosingType) {
        // set byte[]
        w.format(
                "public %s %s(byte[] %s) {\n",
                enclosingType, ProtoGenUtil.camelCase("set", ccName), ccName);
        w.format(
                "    %sBytesView(new %s(%s));\n",
                ProtoGenUtil.camelCase("set", ccName),
                ByteBufBytesView.class.getSimpleName(),
                ccName);
        w.format("    return this;\n");
        w.format("}\n");

        // set ByteBuf
        w.format(
                "public %s %s(ByteBuf %s) {\n",
                enclosingType, ProtoGenUtil.camelCase("set", ccName), ccName);
        w.format(
                "    %sBytesView(new %s(%s));\n",
                ProtoGenUtil.camelCase("set", ccName),
                ByteBufBytesView.class.getSimpleName(),
                ccName);
        w.format("    return this;\n");
        w.format("}\n");

        // set MemorySegment
        w.format(
                "public %s %s(%s %s, int position, int size) {\n",
                enclosingType,
                ProtoGenUtil.camelCase("set", ccName),
                MemorySegment.class.getName(),
                ccName);
        w.format(
                "    %sBytesView(new %s(%s, position, size));\n",
                ProtoGenUtil.camelCase("set", ccName),
                MemorySegmentBytesView.class.getSimpleName(),
                ccName);
        w.format("    return this;\n");
        w.format("}\n");

        // set FileChannel
        w.format(
                "public %s %s(java.nio.channels.FileChannel %s, long position, int size) {\n",
                enclosingType, ProtoGenUtil.camelCase("set", ccName), ccName);
        w.format(
                "    %sBytesView(new %s(%s, position, size));\n",
                ProtoGenUtil.camelCase("set", ccName),
                FileRegionBytesView.class.getSimpleName(),
                ccName);
        w.format("    return this;\n");
        w.format("}\n");

        // set BytesView
        w.format(
                "public %s %sBytesView(%s %s) {\n",
                enclosingType, ProtoGenUtil.camelCase("set", ccName), BYTES_VIEW_CLASS, ccName);
        w.format("    this.%s = %s;\n", ccName, ccName);
        w.format("    _bitField%d |= %s;\n", bitFieldIndex(), fieldMask());
        w.format("    _%sIdx = -1;\n", ccName);
        w.format("    _%sLen = %s.getBytesLength();\n", ccName, ccName);
        w.format("    _cachedSize = -1;\n");
        w.format("    return this;\n");
        w.format("}\n");
    }

    @Override
    public void getter(PrintWriter w) {
        // get size
        w.format("public int %s() {\n", ProtoGenUtil.camelCase("get", ccName, "size"));
        w.format("    if (!%s()) {\n", ProtoGenUtil.camelCase("has", ccName));
        w.format(
                "        throw new IllegalStateException(\"Field '%s' is not set\");\n",
                field.getName());
        w.format("    }\n");
        w.format("    return _%sLen;\n", ccName);
        w.format("}\n");

        // get byte[]
        w.format("public byte[] %s() {\n", ProtoGenUtil.camelCase("get", ccName));
        w.format("    ByteBuf _b = %s();\n", ProtoGenUtil.camelCase("get", ccName, "slice"));
        w.format("    byte[] res = new byte[_b.readableBytes()];\n");
        w.format("    _b.getBytes(0, res);\n");
        w.format("    return res;\n");
        w.format("}\n");

        // get ByteBuf
        w.format("public ByteBuf %s() {\n", ProtoGenUtil.camelCase("get", ccName, "slice"));
        w.format("    if (!%s()) {\n", ProtoGenUtil.camelCase("has", ccName));
        w.format(
                "        throw new IllegalStateException(\"Field '%s' is not set\");\n",
                field.getName());
        w.format("    }\n");
        w.format("    if (%s == null) {\n", ccName);
        w.format("        return _parsedBuffer.slice(_%sIdx, _%sLen);\n", ccName, ccName);
        w.format("    } else {\n");
        w.format("        ByteBuf _b = %s.getByteBuf();", ccName);
        w.format("        return _b.slice(0, _%sLen);\n", ccName);
        w.format("    }\n");
        w.format("}\n");
    }

    @Override
    public void clear(PrintWriter w) {
        w.format("%s = null;\n", ccName);
        w.format("_%sIdx = -1;\n", ccName);
        w.format("_%sLen = -1;\n", ccName);
    }

    @Override
    public void totalSize(PrintWriter w) {
        w.format("_size += %s_SIZE;\n", tagName());
        w.format("_size += ProtoCodecUtils.computeVarIntSize(_%sLen) + _%sLen;\n", ccName, ccName);
    }

    public void zeroCopySize(PrintWriter w) {
        w.format(
                " _size += (%s() && _%sIdx == -1) ? this.%s.getZeroCopyLength() : 0;\n",
                ProtoGenUtil.camelCase("has", ccName), ccName, ccName);
    }

    @Override
    public void serialize(PrintWriter w) {
        w.format("_w.writeVarInt(%s);\n", tagName());
        w.format("_w.writeVarInt(_%sLen);\n", ccName);

        w.format("if (_%sIdx == -1) {\n", ccName);
        w.format("    _w.writeBytes(%s);\n", ccName);
        w.format("} else {\n");
        w.format("    _w.writeByteBuf(_parsedBuffer, _%sIdx, _%sLen);\n", ccName, ccName);
        w.format("}\n");
    }

    @Override
    protected String typeTag() {
        return "ProtoCodecUtils.WIRETYPE_LENGTH_DELIMITED";
    }
}
