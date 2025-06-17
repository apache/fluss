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

import io.protostuff.parser.Field;

import java.io.PrintWriter;

/** Describes a bytes field in a protobuf message. */
public class ProtobufRepeatedBytesField extends ProtobufAbstractRepeated<Field.Bytes> {

    protected final String pluralName;
    protected final String singularName;

    public ProtobufRepeatedBytesField(Field.Bytes field, int index) {
        super(field, index);
        this.pluralName = ProtoGenUtil.plural(ccName);
        this.singularName = ProtoGenUtil.singular(ccName);
    }

    @Override
    public void declaration(PrintWriter w) {
        w.format("private java.util.List<ProtoCodecUtils.BytesHolder> %s = null;\n", pluralName);
        w.format("private int _%sCount = 0;\n", pluralName);
    }

    @Override
    public void parse(PrintWriter w) {
        w.format(
                "ProtoCodecUtils.BytesHolder _%sBh = _%sBytesHolder();\n",
                ccName, ProtoGenUtil.camelCase("new", singularName));
        w.format("_%sBh.len = ProtoCodecUtils.readVarInt(_buffer);\n", ccName);
        w.format("_%sBh.b = new byte[_%sBh.len];\n", ccName, ccName);
        w.format("_buffer.readBytes(_%sBh.b);\n", ccName);
    }

    @Override
    public void getter(PrintWriter w) {
        // get count
        w.format("public int %s() {\n", ProtoGenUtil.camelCase("get", pluralName, "count"));
        w.format("    return _%sCount;\n", pluralName);
        w.format("}\n");
        w.println();

        // get length at
        w.format(
                "public int %s(int idx) {\n",
                ProtoGenUtil.camelCase("get", singularName, "size", "at"));
        w.format("    if (idx < 0 || idx >= _%sCount) {\n", pluralName);
        w.format(
                "        throw new IndexOutOfBoundsException(\"Index \" + idx + \" is out of the list size (\" + _%sCount + \") for field '%s'\");\n",
                pluralName, field.getName());
        w.format("    }\n");
        w.format("    return %s.get(idx).len;\n", pluralName);
        w.format("}\n");
        w.println();

        // get at
        w.format(
                "public byte[] %s(int idx) {\n", ProtoGenUtil.camelCase("get", singularName, "at"));
        w.format("    if (idx < 0 || idx >= _%sCount) {\n", pluralName);
        w.format(
                "        throw new IndexOutOfBoundsException(\"Index \" + idx + \" is out of the list size (\" + _%sCount + \") for field '%s'\");\n",
                pluralName, field.getName());
        w.format("    }\n");
        w.format("    return %s.get(idx).b;\n", pluralName);
        w.format("}\n");
    }

    @Override
    public void serialize(PrintWriter w) {
        w.format("for (int i = 0; i < _%sCount; i++) {\n", pluralName);
        w.format("    ProtoCodecUtils.BytesHolder _bh = %s.get(i);\n", pluralName);
        w.format("    _w.writeVarInt(%s);\n", tagName());
        w.format("    _w.writeVarInt(_bh.len);\n");
        w.format("    _w.writeByteArray(_bh.b, 0, _bh.len);\n");
        w.format("}\n");
    }

    @Override
    public void setter(PrintWriter w, String enclosingType) {
        // add byte[]
        w.format(
                "public void %s(byte[] %s) {\n",
                ProtoGenUtil.camelCase("add", singularName), singularName);
        w.format("    if (%s == null) {\n", pluralName);
        w.format(
                "        %s = new java.util.ArrayList<ProtoCodecUtils.BytesHolder>();\n",
                pluralName);
        w.format("    }\n");
        w.format(
                "    ProtoCodecUtils.BytesHolder _bh = _%sBytesHolder();\n",
                ProtoGenUtil.camelCase("new", singularName));
        w.format("    _cachedSize = -1;\n");
        w.format("    _bh.b = %s;\n", singularName);
        w.format("    _bh.len = %s.length;\n", singularName);
        w.format("}\n");
        w.println();

        // new BytesHolder
        w.format(
                "private ProtoCodecUtils.BytesHolder _%sBytesHolder() {\n",
                ProtoGenUtil.camelCase("new", singularName));
        w.format("    if (%s == null) {\n", pluralName);
        w.format(
                "         %s = new java.util.ArrayList<ProtoCodecUtils.BytesHolder>();\n",
                pluralName);
        w.format("    }\n");
        w.format("    ProtoCodecUtils.BytesHolder _bh = new ProtoCodecUtils.BytesHolder();\n");
        w.format("    %s.add(_bh);\n", pluralName);
        w.format("    _%sCount++;\n", pluralName);
        w.format("    return _bh;\n");
        w.format("}\n");
    }

    @Override
    public void copy(PrintWriter w) {
        w.format(
                "for (int i = 0; i < _other.%s(); i++) {\n",
                ProtoGenUtil.camelCase("get", pluralName, "count"));
        w.format(
                "    %s(_other.%s(i));\n",
                ProtoGenUtil.camelCase("add", singularName),
                ProtoGenUtil.camelCase("get", singularName, "at"));
        w.format("}\n");
    }

    @Override
    public void totalSize(PrintWriter w) {
        w.format("for (int i = 0; i < _%sCount; i++) {\n", pluralName);
        w.format("    ProtoCodecUtils.BytesHolder _bh = %s.get(i);\n", pluralName);
        w.format("    _size += %s_SIZE;\n", tagName());
        w.format("    _size += ProtoCodecUtils.computeVarIntSize(_bh.len) + _bh.len;\n");
        w.format("}\n");
    }

    @Override
    public void clear(PrintWriter w) {
        w.format("%s = null;\n", pluralName);
        w.format("_%sCount = 0;\n", pluralName);
    }

    @Override
    protected String typeTag() {
        return "ProtoCodecUtils.WIRETYPE_LENGTH_DELIMITED";
    }
}
