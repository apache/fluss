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

/** Describes a repeated string field in a protobuf message. */
public class ProtobufRepeatedStringField extends ProtobufAbstractRepeated<Field.String> {

    protected final String pluralName;
    protected final String singularName;

    public ProtobufRepeatedStringField(Field.String field, int index) {
        super(field, index);
        this.pluralName = ProtoGenUtil.plural(ccName);
        this.singularName = ProtoGenUtil.singular(ccName);
    }

    @Override
    public void declaration(PrintWriter w) {
        w.format("private java.util.List<ProtoCodecUtils.StringHolder> %s = null;\n", pluralName);
        w.format("private int _%sCount = 0;\n", pluralName);
    }

    @Override
    public void parse(PrintWriter w) {
        w.format(
                "ProtoCodecUtils.StringHolder _%sSh = _%sStringHolder();\n",
                ccName, ProtoGenUtil.camelCase("new", singularName));
        w.format("_%sSh.len = ProtoCodecUtils.readVarInt(_buffer);\n", ccName);
        w.format(
                "_%sSh.s = ProtoCodecUtils.readString(_buffer, _buffer.readerIndex(), _%sSh.len);\n",
                ccName, ccName);
        w.format("_buffer.skipBytes(_%sSh.len);\n", ccName);
    }

    @Override
    public void getter(PrintWriter w) {
        // get count
        w.format("public int %s() {\n", ProtoGenUtil.camelCase("get", pluralName, "count"));
        w.format("    return _%sCount;\n", pluralName);
        w.format("}\n");
        w.println();

        // get string at idx
        w.format(
                "public %s %s(int idx) {\n",
                field.getJavaType(), ProtoGenUtil.camelCase("get", singularName, "at"));
        w.format("    if (idx < 0 || idx >= _%sCount) {\n", pluralName);
        w.format(
                "        throw new IndexOutOfBoundsException(\"Index \" + idx + \" is out of the list size (\" + _%sCount + \") for field '%s'\");\n",
                pluralName, field.getName());
        w.format("    }\n");
        w.format("    ProtoCodecUtils.StringHolder _sh = %s.get(idx);\n", pluralName);
        w.format("    return _sh.s;\n");
        w.format("}\n");
        w.println();

        // get list
        w.format(
                "public java.util.List<String> %s() {\n",
                ProtoGenUtil.camelCase("get", pluralName, "list"));
        w.format("    if (_%sCount == 0) {\n", pluralName);
        w.format("        return java.util.Collections.emptyList();\n");
        w.format("    } else {\n");
        w.format(
                "        java.util.List<String> _l = new java.util.ArrayList<>(_%sCount);\n",
                pluralName);
        w.format("        for (int i = 0; i < _%sCount; i++) {\n", pluralName);
        w.format("            _l.add(%s(i));\n", ProtoGenUtil.camelCase("get", singularName, "at"));
        w.format("        }\n");
        w.format("        return _l;\n");
        w.format("    }\n");
        w.format("}\n");
    }

    @Override
    public void serialize(PrintWriter w) {
        w.format("for (int i = 0; i < _%sCount; i++) {\n", pluralName);
        w.format("    ProtoCodecUtils.StringHolder _sh = %s.get(i);\n", pluralName);
        w.format("    _w.writeVarInt(%s);\n", tagName());
        w.format("    _w.writeVarInt(_sh.len);\n");
        w.format("    _w.writeString(_sh.s, _sh.len);\n");
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
    public void setter(PrintWriter w, String enclosingType) {
        // add
        w.format(
                "public void %s(String %s) {\n",
                ProtoGenUtil.camelCase("add", singularName), singularName);
        w.format(
                "    ProtoCodecUtils.StringHolder _sh = _%sStringHolder();\n",
                ProtoGenUtil.camelCase("new", singularName));
        w.format("    _cachedSize = -1;\n");
        w.format("    _sh.s = %s;\n", singularName);
        w.format("    _sh.len = ProtoCodecUtils.computeStringUTF8Size(_sh.s);\n");
        w.format("}\n");
        w.println();

        // addAll
        w.format(
                "public %s %s(java.util.Collection<String> %s) {\n",
                enclosingType, ProtoGenUtil.camelCase("addAll", pluralName), pluralName);
        w.format("    if (this.%s == null) {\n", pluralName);
        // set initial capacity to avoid resizing
        w.format(
                "        this.%s = new java.util.ArrayList<ProtoCodecUtils.StringHolder>(%s.size());\n",
                pluralName, pluralName);
        w.format("    }\n");
        w.format("    for (String _s : %s) {\n", pluralName);
        w.format("        %s(_s);\n", ProtoGenUtil.camelCase("add", singularName));
        w.format("    }\n");
        w.format("    return this;\n");
        w.format("}\n");
        w.println();

        // new StringHolder
        w.format(
                "private ProtoCodecUtils.StringHolder _%sStringHolder() {\n",
                ProtoGenUtil.camelCase("new", singularName));
        w.format("    if (%s == null) {\n", pluralName);
        w.format(
                "         %s = new java.util.ArrayList<ProtoCodecUtils.StringHolder>();\n",
                pluralName);
        w.format("    }\n");
        w.format("    ProtoCodecUtils.StringHolder _sh = new ProtoCodecUtils.StringHolder();\n");
        w.format("    %s.add(_sh);\n", pluralName);
        w.format("    _%sCount++;\n", pluralName);
        w.format("    return _sh;\n");
        w.format("}\n");
        w.println();
    }

    @Override
    public void totalSize(PrintWriter w) {
        w.format("for (int i = 0; i < _%sCount; i++) {\n", pluralName);
        w.format("    ProtoCodecUtils.StringHolder _sh = %s.get(i);\n", pluralName);
        w.format("    _size += %s_SIZE;\n", tagName());
        w.format("    _size += ProtoCodecUtils.computeVarIntSize(_sh.len) + _sh.len;\n");
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
