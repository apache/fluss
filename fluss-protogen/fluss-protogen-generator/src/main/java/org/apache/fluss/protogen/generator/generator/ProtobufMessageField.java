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

import io.protostuff.parser.MessageField;

import java.io.PrintWriter;

/** Generator for a message field. */
public class ProtobufMessageField extends ProtobufField<MessageField> {

    public ProtobufMessageField(MessageField field, int index) {
        super(field, index, false);
    }

    @Override
    public void declaration(PrintWriter w) {
        w.format("private %s %s;\n", field.getJavaType(), ccName);
    }

    @Override
    public void setter(PrintWriter w, String enclosingType) {
        w.format("public %s %s() {\n", field.getJavaType(), ProtoGenUtil.camelCase("set", ccName));
        w.format("    if (%s == null) {\n", ccName);
        w.format("        %s = new %s();\n", ccName, field.getJavaType());
        w.format("    }\n");
        w.format("    _bitField%d |= %s;\n", bitFieldIndex(), fieldMask());
        w.format("    _cachedSize = -1;\n");
        w.format("    return %s;\n", ccName);
        w.format("}\n");

        w.format(
                "public %s %s(%s %s) {\n",
                enclosingType, ProtoGenUtil.camelCase("set", ccName), field.getJavaType(), ccName);
        w.format("    this.%s = %s;", ccName, ccName);
        w.format("    _bitField%d |= %s;\n", bitFieldIndex(), fieldMask());
        w.format("    _cachedSize = -1;\n");
        w.format("    return this;\n");
        w.format("}\n");
    }

    @Override
    public void copy(PrintWriter w) {
        w.format("%s().copyFrom(_other.%s);\n", ProtoGenUtil.camelCase("set", ccName), ccName);
    }

    public void getter(PrintWriter w) {
        w.format(
                "public %s %s() {\n",
                field.getJavaType(), ProtoGenUtil.camelCase("get", field.getName()));
        w.format("    if (!%s()) {\n", ProtoGenUtil.camelCase("has", ccName));
        w.format(
                "        throw new IllegalStateException(\"Field '%s' is not set\");\n",
                field.getName());
        w.format("    }\n");
        w.format("    return %s;\n", ccName);
        w.format("}\n");
    }

    @Override
    public void parse(PrintWriter w) {
        w.format("int %sSize = ProtoCodecUtils.readVarInt(_buffer);\n", ccName);
        w.format(
                "%s().parseFrom(_buffer, %sSize);\n",
                ProtoGenUtil.camelCase("set", ccName), ccName);
    }

    @Override
    public void totalSize(PrintWriter w) {
        String tmpName = ProtoGenUtil.camelCase("_msgSize", ccName);
        w.format("_size += ProtoCodecUtils.computeVarIntSize(%s);\n", tagName());
        w.format("int %s = %s.totalSize();\n", tmpName, ccName);
        w.format("_size += ProtoCodecUtils.computeVarIntSize(%s) + %s;\n", tmpName, tmpName);
    }

    @Override
    public void zeroCopySize(PrintWriter w) {
        w.format("if (%s()){\n", ProtoGenUtil.camelCase("has", ccName));
        w.format("    _size += %s.zeroCopySize();\n", ccName);
        w.format("}\n");
    }

    @Override
    public void serialize(PrintWriter w) {
        w.format("_w.writeVarInt(%s);\n", tagName());
        w.format("_w.writeVarInt(%s.totalSize());\n", ccName);
        w.format("%s.writeTo(_w);\n", ccName);
    }

    @Override
    public void clear(PrintWriter w) {
        w.format("if (%s()){\n", ProtoGenUtil.camelCase("has", ccName));
        w.format("    %s.clear();\n", ccName);
        w.format("}\n");
    }

    @Override
    protected String typeTag() {
        return "ProtoCodecUtils.WIRETYPE_LENGTH_DELIMITED";
    }
}
