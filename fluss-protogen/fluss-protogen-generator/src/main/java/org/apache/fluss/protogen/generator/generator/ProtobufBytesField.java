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

package org.apache.fluss.protogen.generator.generator;

import io.protostuff.parser.Field;

import java.io.PrintWriter;

/** Generator for a byte[] field. */
public class ProtobufBytesField extends ProtobufField<Field.Bytes> {

    public ProtobufBytesField(Field.Bytes field, int index) {
        super(field, index, false);
    }

    @Override
    public void declaration(PrintWriter w) {
        w.format("private byte[] %s = null;\n", ccName);
        w.format("private int _%sLen = -1;\n", ccName);
    }

    @Override
    public void parse(PrintWriter w) {
        w.format("_%sLen = ProtoCodecUtils.readVarInt(_buffer);\n", ccName);
        w.format("%s = new byte[_%sLen];\n", ccName, ccName);
        w.format("_buffer.readBytes(%s);\n", ccName);
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
        w.format("    this.%s = %s;\n", ccName, ccName);
        w.format("    _bitField%d |= %s;\n", bitFieldIndex(), fieldMask());
        w.format("    _%sLen = %s.length;\n", ccName, ccName);
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
        w.format("    return %s;\n", ccName);
        w.format("}\n");
    }

    @Override
    public void clear(PrintWriter w) {
        w.format("%s = null;\n", ccName);
        w.format("_%sLen = -1;\n", ccName);
    }

    @Override
    public void totalSize(PrintWriter w) {
        w.format("_size += %s_SIZE;\n", tagName());
        w.format("_size += ProtoCodecUtils.computeVarIntSize(_%sLen) + _%sLen;\n", ccName, ccName);
    }

    @Override
    public void serialize(PrintWriter w) {
        w.format("_w.writeVarInt(%s);\n", tagName());
        w.format("_w.writeVarInt(_%sLen);\n", ccName);
        w.format("_w.writeByteArray(%s, 0, _%sLen);\n", ccName, ccName);
    }

    @Override
    protected String typeTag() {
        return "ProtoCodecUtils.WIRETYPE_LENGTH_DELIMITED";
    }
}
