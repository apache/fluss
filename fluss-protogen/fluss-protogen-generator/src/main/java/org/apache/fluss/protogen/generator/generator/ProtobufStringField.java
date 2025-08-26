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

/** Describes a string field in a protobuf message. */
public class ProtobufStringField extends ProtobufField<Field.String> {

    public ProtobufStringField(Field.String field, int index, boolean isErrorField) {
        super(field, index, isErrorField);
    }

    @Override
    public void declaration(PrintWriter w) {
        if (field.isDefaultValueSet()) {
            w.format("private String %s = \"%s\";\n", ccName, field.getDefaultValue());
        } else {
            w.format("private String %s;\n", ccName);
        }
        w.format("private int _%sLen = -1;\n", ccName);
    }

    @Override
    public void setter(PrintWriter w, String enclosingType) {
        if (isErrorField) {
            w.format("@Override\n");
        }
        w.format(
                "public %s %s(%s %s) {\n",
                enclosingType,
                ProtoGenUtil.camelCase("set", field.getName()),
                field.getJavaType(),
                ProtoGenUtil.camelCase(field.getName()));
        // add a better exception message for null argument.
        w.format("    if (%s == null) {\n", ProtoGenUtil.camelCase(field.getName()));
        w.format(
                "        throw new NullPointerException(\"Field '%s' cannot be null\");\n",
                field.getName());
        w.format("    }\n");
        w.println();
        w.format(
                "    this.%s = %s;\n",
                ProtoGenUtil.camelCase(field.getName()), ProtoGenUtil.camelCase(field.getName()));
        w.format("    _bitField%d |= %s;\n", bitFieldIndex(), fieldMask());
        w.format("    _%sLen = ProtoCodecUtils.computeStringUTF8Size(%s);\n", ccName, ccName);
        w.format("    _cachedSize = -1;\n");
        w.format("    return this;\n");
        w.format("}\n");
    }

    @Override
    public void copy(PrintWriter w) {
        w.format(
                "%s(_other.%s());\n",
                ProtoGenUtil.camelCase("set", ccName), ProtoGenUtil.camelCase("get", ccName));
    }

    @Override
    public void getter(PrintWriter w) {
        if (isErrorField) {
            w.format("@Override\n");
        }
        w.format(
                "public %s %s() {\n",
                field.getJavaType(), ProtoGenUtil.camelCase("get", field.getName()));
        if (!field.isDefaultValueSet()) {
            w.format("    if (!%s()) {\n", ProtoGenUtil.camelCase("has", ccName));
            w.format(
                    "        throw new IllegalStateException(\"Field '%s' is not set\");\n",
                    field.getName());
            w.format("    }\n");
        }
        w.format("    return %s;\n", ProtoGenUtil.camelCase(field.getName()));
        w.format("}\n");
    }

    @Override
    public void clear(PrintWriter w) {
        w.format("%s = %s;\n", ccName, field.getDefaultValue());
        w.format("_%sLen = -1;\n", ccName);
    }

    @Override
    public void totalSize(PrintWriter w) {
        w.format("_size += %s_SIZE;\n", tagName());
        w.format("_size += ProtoCodecUtils.computeVarIntSize(_%sLen);\n", ccName);
        w.format("_size += _%sLen;\n", ccName);
    }

    @Override
    public void serialize(PrintWriter w) {
        w.format("_w.writeVarInt(%s);\n", tagName());
        w.format("_w.writeVarInt(_%sLen);\n", ccName);
        w.format("_w.writeString(%s, _%sLen);\n", ccName, ccName);
    }

    @Override
    public void parse(PrintWriter w) {
        w.format("_%sLen = ProtoCodecUtils.readVarInt(_buffer);\n", ccName);
        w.format("int _%sBufferIdx = _buffer.readerIndex();\n", ccName);
        w.format(
                "%s = ProtoCodecUtils.readString(_buffer, _buffer.readerIndex(), _%sLen);\n",
                ccName, ccName);
        w.format("_buffer.skipBytes(_%sLen);\n", ccName);
    }

    @Override
    protected String typeTag() {
        return "ProtoCodecUtils.WIRETYPE_LENGTH_DELIMITED";
    }
}
