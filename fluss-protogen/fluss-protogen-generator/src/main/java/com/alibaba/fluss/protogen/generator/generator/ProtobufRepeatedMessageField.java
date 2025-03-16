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

package com.alibaba.fluss.protogen.generator.generator;

import io.protostuff.parser.MessageField;

import java.io.PrintWriter;

/** Describes a repeated message field in a protobuf message. */
public class ProtobufRepeatedMessageField extends ProtobufAbstractRepeated<MessageField> {

    protected final String pluralName;
    protected final String singularName;

    public ProtobufRepeatedMessageField(MessageField field, int index) {
        super(field, index);
        this.pluralName = ProtoGenUtil.plural(ccName);
        this.singularName = ProtoGenUtil.singular(ccName);
    }

    @Override
    public void declaration(PrintWriter w) {
        w.format("private java.util.List<%s> %s = null;\n", field.getJavaType(), pluralName);
        w.format("private int _%sCount = 0;\n", pluralName);
    }

    @Override
    public void getter(PrintWriter w) {
        w.format("public int %s() {\n", ProtoGenUtil.camelCase("get", pluralName, "count"));
        w.format("    return _%sCount;\n", pluralName);
        w.format("}\n");
        w.format(
                "public %s %s(int idx) {\n",
                field.getJavaType(), ProtoGenUtil.camelCase("get", singularName, "at"));
        w.format("    if (idx < 0 || idx >= _%sCount) {\n", pluralName);
        w.format(
                "        throw new IndexOutOfBoundsException(\"Index \" + idx + \" is out of the list size (\" + _%sCount + \") for field '%s'\");\n",
                pluralName, field.getName());
        w.format("    }\n");
        w.format("    return %s.get(idx);\n", pluralName);
        w.format("}\n");

        w.format(
                "public java.util.List<%s> %s() {\n",
                field.getJavaType(), ProtoGenUtil.camelCase("get", pluralName, "list"));
        w.format("    if (_%sCount == 0) {\n", pluralName);
        w.format("        return java.util.Collections.emptyList();\n");
        w.format("    } else {\n");
        w.format("        return %s.subList(0, _%sCount);\n", pluralName, pluralName);
        w.format("    }\n");
        w.format("}\n");
    }

    @Override
    public void parse(PrintWriter w) {
        w.format("int _%sSize = ProtoCodecUtils.readVarInt(_buffer);\n", ccName);
        w.format(
                "%s().parseFrom(_buffer, _%sSize);\n",
                ProtoGenUtil.camelCase("add", singularName), ccName);
    }

    @Override
    public void serialize(PrintWriter w) {
        w.format("for (int i = 0; i < _%sCount; i++) {\n", pluralName);
        w.format("    %s _item = %s.get(i);\n", field.getJavaType(), pluralName);
        w.format("    _w.writeVarInt(%s);\n", tagName());
        w.format("    _w.writeVarInt(_item.totalSize());\n");
        w.format("    _item.writeTo(_w);\n");
        w.format("}\n");
    }

    @Override
    public void copy(PrintWriter w) {
        w.format(
                "for (int i = 0; i < _other.%s(); i++) {\n",
                ProtoGenUtil.camelCase("get", pluralName, "count"));
        w.format(
                "    %s().copyFrom(_other.%s(i));\n",
                ProtoGenUtil.camelCase("add", singularName),
                ProtoGenUtil.camelCase("get", singularName, "at"));
        w.format("}\n");
    }

    @Override
    public void setter(PrintWriter w, String enclosingType) {
        // add and new
        w.format(
                "public %s %s() {\n",
                field.getJavaType(), ProtoGenUtil.camelCase("add", singularName));
        w.format(
                "    return %s(new %s());\n",
                ProtoGenUtil.camelCase("add", singularName), field.getJavaType());
        w.format("}\n");
        w.println();

        // add
        w.format(
                "private %s %s(%s %s) {\n",
                field.getJavaType(),
                ProtoGenUtil.camelCase("add", singularName),
                field.getJavaType(),
                singularName);
        w.format("    if (%s == null) {\n", pluralName);
        w.format("        %s = new java.util.ArrayList<%s>();\n", pluralName, field.getJavaType());
        w.format("    }\n");
        w.format("    %s.add(%s);\n", pluralName, singularName);
        w.format("    _cachedSize = -1;\n");
        w.format("    return %s.get(_%sCount++);\n", pluralName, pluralName);
        w.format("}\n");
        w.println();

        // add Collection
        w.format(
                "public %s %s(java.util.Collection<%s> %s) {\n",
                enclosingType,
                ProtoGenUtil.camelCase("addAll", pluralName),
                field.getJavaType(),
                pluralName);
        w.format("    if (this.%s == null) {\n", pluralName);
        w.format("        if (%s instanceof java.util.ArrayList) {\n", pluralName);
        w.format("            this.%s = (java.util.ArrayList) %s;\n", pluralName, pluralName);
        w.format("            this._%sCount = %s.size();\n", pluralName, pluralName);
        w.format("            _cachedSize = -1;\n");
        w.format("            return this;\n");
        w.format("        } else {\n");
        // set initial capacity to avoid resizing
        w.format(
                "             this.%s = new java.util.ArrayList<%s>(%s.size());\n",
                pluralName, field.getJavaType(), pluralName);
        w.format("        }\n");
        w.format("    }\n");
        w.format("    for (%s _o : %s) {\n", field.getJavaType(), pluralName);
        w.format("        %s(_o);\n", ProtoGenUtil.camelCase("add", singularName));
        w.format("    }\n");
        w.format("    return this;\n");
        w.format("}\n");
    }

    @Override
    public void totalSize(PrintWriter w) {
        String tmpName = ProtoGenUtil.camelCase("_msgSize", field.getName());

        w.format("for (int i = 0; i < _%sCount; i++) {\n", pluralName);
        w.format("     %s _item = %s.get(i);\n", field.getJavaType(), pluralName);
        w.format("     _size += %s_SIZE;\n", tagName());
        w.format("     int %s = _item.totalSize();\n", tmpName);
        w.format("     _size += ProtoCodecUtils.computeVarIntSize(%s) + %s;\n", tmpName, tmpName);
        w.format("}\n");
    }

    @Override
    public void zeroCopySize(PrintWriter w) {
        w.format("for (int i = 0; i < _%sCount; i++) {\n", pluralName);
        w.format("     %s _item = %s.get(i);\n", field.getJavaType(), pluralName);
        w.format("     _size += _item.zeroCopySize();\n");
        w.format("}\n");
    }

    @Override
    public void clear(PrintWriter w) {
        w.format("for (int i = 0; i < _%sCount; i++) {\n", pluralName);
        w.format("    %s.get(i).clear();\n", pluralName);
        w.format("}\n");
        w.format("_%sCount = 0;\n", pluralName);
    }

    @Override
    protected String typeTag() {
        return "ProtoCodecUtils.WIRETYPE_LENGTH_DELIMITED";
    }
}
