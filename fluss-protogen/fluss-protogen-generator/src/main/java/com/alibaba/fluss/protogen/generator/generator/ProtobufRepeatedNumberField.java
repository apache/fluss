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

import io.protostuff.parser.Field;

import java.io.PrintWriter;

/** Describes a repeated number field in a protobuf message. */
public class ProtobufRepeatedNumberField extends ProtobufAbstractRepeated<Field<?>> {

    protected final String pluralName;
    protected final String singularName;

    public ProtobufRepeatedNumberField(Field<?> field, int index) {
        super(field, index);
        this.pluralName = ProtoGenUtil.plural(ccName);
        this.singularName = ProtoGenUtil.singular(ccName);
    }

    @Override
    public void declaration(PrintWriter w) {
        w.format("private %s[] %s = null;\n", field.getJavaType(), pluralName);
        w.format("private int _%sCount = 0;\n", pluralName);
    }

    @Override
    public void parse(PrintWriter w) {
        w.format(
                "%s(%s);\n",
                ProtoGenUtil.camelCase("add", singularName),
                ProtobufNumberField.parseNumber(field));
    }

    public void parsePacked(PrintWriter w) {
        w.format(
                "int _%s = ProtoCodecUtils.readVarInt(_buffer);\n",
                ProtoGenUtil.camelCase(singularName, "size"));
        w.format(
                "int _%s = _buffer.readerIndex() + _%s;\n",
                ProtoGenUtil.camelCase(singularName, "endIdx"),
                ProtoGenUtil.camelCase(singularName, "size"));
        w.format(
                "while (_buffer.readerIndex() < _%s) {\n",
                ProtoGenUtil.camelCase(singularName, "endIdx"));
        w.format(
                "%s(%s);\n",
                ProtoGenUtil.camelCase("add", singularName),
                ProtobufNumberField.parseNumber(field));
        w.format("}\n");
    }

    @Override
    public void getter(PrintWriter w) {
        w.format(
                "private static final int %s_PACKED = (%s << ProtoCodecUtils.TAG_TYPE_BITS) | ProtoCodecUtils.WIRETYPE_LENGTH_DELIMITED;\n",
                tagName(), fieldNumber());
        // getCount
        w.format("public int %s() {\n", ProtoGenUtil.camelCase("get", pluralName, "count"));
        w.format("    return _%sCount;\n", pluralName);
        w.format("}\n");

        // getAt(index)
        w.format(
                "public %s %s(int idx) {\n",
                field.getJavaType(), ProtoGenUtil.camelCase("get", singularName, "at"));
        w.format("    if (idx < 0 || idx >= _%sCount) {\n", pluralName);
        w.format(
                "        throw new IndexOutOfBoundsException(\"Index \" + idx + \" is out of the list size (\" + _%sCount + \") for field '%s'\");\n",
                pluralName, field.getName());
        w.format("    }\n");
        w.format("    return %s[idx];\n", pluralName);
        w.format("}\n");

        // getAll
        w.format(
                "public %s[] %s() {\n",
                field.getJavaType(), ProtoGenUtil.camelCase("get", pluralName));
        w.format("    if (%s == null) {\n", pluralName);
        w.format("        return new %s[0];\n", field.getJavaType());
        w.format("    } else if (_%sCount == %s.length) {\n", pluralName, pluralName);
        w.format("        return %s;\n", pluralName);
        w.format("    } else {\n");
        w.format("        return java.util.Arrays.copyOf(%s, _%sCount);\n", pluralName, pluralName);
        w.format("    }\n");
        w.format("}\n");
    }

    @Override
    public void serialize(PrintWriter w) {
        if (field.getOption("packed") == Boolean.TRUE) {
            w.format("    _w.writeVarInt(%s_PACKED);\n", tagName());
            w.format("    int _%sSize = 0;\n", pluralName);
            w.format("for (int i = 0; i < _%sCount; i++) {\n", pluralName);
            w.format("    %s _item = %s[i];\n", field.getJavaType(), pluralName);
            w.format(
                    "    _%sSize += %s;\n",
                    pluralName, ProtobufNumberField.serializedSizeOfNumber(field, "_item"));
            w.format("}\n");
            w.format("    _w.writeVarInt(_%sSize);\n", pluralName);
            w.format("for (int i = 0; i < _%sCount; i++) {\n", pluralName);
            w.format("    %s _item = %s[i];\n", field.getJavaType(), pluralName);
            ProtobufNumberField.serializeNumber(w, field, "_item");
            w.format("}\n");
        } else {
            w.format("for (int i = 0; i < _%sCount; i++) {\n", pluralName);
            w.format("    %s _item = %s[i];\n", field.getJavaType(), pluralName);
            w.format("    _w.writeVarInt(%s);\n", tagName());
            ProtobufNumberField.serializeNumber(w, field, "_item");
            w.format("}\n");
        }
    }

    @Override
    public void setter(PrintWriter w, String enclosingType) {
        // add
        w.format(
                "public void %s(%s %s) {\n",
                ProtoGenUtil.camelCase("add", singularName), field.getJavaType(), singularName);
        w.format("    if (%s == null) {\n", pluralName);
        w.format("        %s = new %s[4];\n", pluralName, field.getJavaType());
        w.format("    }\n");
        w.format("    if (%s.length == _%sCount) {\n", pluralName, pluralName);
        w.format(
                "        %s = java.util.Arrays.copyOf(%s, _%sCount * 2);\n",
                pluralName, pluralName, pluralName);
        w.format("    }\n");
        w.format("    _cachedSize = -1;\n");
        w.format("    %s[_%sCount++] = %s;\n", pluralName, pluralName, singularName);
        w.format("}\n");

        // setAll
        w.format(
                "public %s %s(%s[] %s) {\n",
                enclosingType,
                ProtoGenUtil.camelCase("set", pluralName),
                field.getJavaType(),
                pluralName);
        w.format("    _cachedSize = -1;\n");
        w.format("    _%sCount = %s.length;\n", pluralName, pluralName);
        w.format("    this.%s = %s;\n", pluralName, pluralName);
        w.format("    return this;\n");
        w.format("}\n");
        w.println();
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
        if (field.getOption("packed") == Boolean.TRUE) {
            w.format("    _size += %s_SIZE;\n", tagName());
            w.format("    int _%sSize = 0;\n", pluralName);
            w.format("for (int i = 0; i < _%sCount; i++) {\n", pluralName);
            w.format("    %s _item = %s[i];\n", field.getJavaType(), pluralName);
            w.format(
                    "    _%sSize += %s;\n",
                    pluralName, ProtobufNumberField.serializedSizeOfNumber(field, "_item"));
            w.format("}\n");
            w.format("    _size += ProtoCodecUtils.computeVarIntSize(_%sSize);\n", pluralName);
            w.format("    _size += _%sSize;\n", pluralName);
        } else {
            w.format("for (int i = 0; i < _%sCount; i++) {\n", pluralName);
            w.format("    %s _item = %s[i];\n", field.getJavaType(), pluralName);
            w.format("    _size += %s_SIZE;\n", tagName());
            w.format(
                    "    _size += %s;\n",
                    ProtobufNumberField.serializedSizeOfNumber(field, "_item"));
            w.format("}\n");
        }
    }

    @Override
    public void clear(PrintWriter w) {
        w.format("_%sCount = 0;\n", pluralName);
        w.format("%s = null;\n", pluralName);
    }

    @Override
    protected String typeTag() {
        return ProtobufNumberField.typeTag(field);
    }
}
