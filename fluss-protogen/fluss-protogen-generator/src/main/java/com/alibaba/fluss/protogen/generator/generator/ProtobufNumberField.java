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
import org.apache.fluss.shaded.guava32.com.google.common.collect.Maps;

import java.io.PrintWriter;
import java.util.Map;

/* This file is based on source code of LightProto Project (https://github.com/splunk/lightproto/),
 * licensed by Splunk, Inc. under the Apache License, Version 2.0. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership. */

/** Describes a field in a protobuf message. */
public class ProtobufNumberField extends ProtobufField<Field<?>> {

    private static final Map<String, String> typeToTag = Maps.newHashMap();

    static {
        typeToTag.put("double", "ProtoCodecUtils.WIRETYPE_FIXED64");
        typeToTag.put("float", "ProtoCodecUtils.WIRETYPE_FIXED32");
        typeToTag.put("bool", "ProtoCodecUtils.WIRETYPE_VARINT");
        typeToTag.put("int32", "ProtoCodecUtils.WIRETYPE_VARINT");
        typeToTag.put("int64", "ProtoCodecUtils.WIRETYPE_VARINT");
        typeToTag.put("uint32", "ProtoCodecUtils.WIRETYPE_VARINT");
        typeToTag.put("uint64", "ProtoCodecUtils.WIRETYPE_VARINT");
        typeToTag.put("sint32", "ProtoCodecUtils.WIRETYPE_VARINT");
        typeToTag.put("sint64", "ProtoCodecUtils.WIRETYPE_VARINT");
        typeToTag.put("fixed32", "ProtoCodecUtils.WIRETYPE_FIXED32");
        typeToTag.put("fixed64", "ProtoCodecUtils.WIRETYPE_FIXED64");
        typeToTag.put("sfixed32", "ProtoCodecUtils.WIRETYPE_FIXED32");
        typeToTag.put("sfixed64", "ProtoCodecUtils.WIRETYPE_FIXED64");
    }

    public ProtobufNumberField(Field<?> field, int index, boolean isErrorField) {
        super(field, index, isErrorField);
    }

    static void serializeNumber(PrintWriter w, Field<?> field, String name) {
        if (field.isEnumField()) {
            w.format("                _w.writeVarInt(%s.getValue());\n", name);
        } else if (field.getProtoType().equals("bool")) {
            w.format("                _w.writeBoolean(%s);\n", name);
        } else if (field.getProtoType().equals("int32")) {
            w.format("                _w.writeVarInt(%s);\n", name);
        } else if (field.getProtoType().equals("uint32")) {
            w.format("                _w.writeVarInt(%s);\n", name);
        } else if (field.getProtoType().equals("sint32")) {
            w.format("                _w.writeSignedVarInt(%s);\n", name);
        } else if (field.getProtoType().equals("sint64")) {
            w.format("                _w.writeSignedVarInt64(%s);\n", name);
        } else if (field.getProtoType().equals("int64")) {
            w.format("                _w.writeVarInt64(%s);\n", name);
        } else if (field.getProtoType().equals("uint64")) {
            w.format("                _w.writeVarInt64(%s);\n", name);
        } else if (field.getProtoType().equals("fixed32")) {
            w.format("                _w.writeFixedInt32(%s);\n", name);
        } else if (field.getProtoType().equals("fixed64")) {
            w.format("                _w.writeFixedInt64(%s);\n", name);
        } else if (field.getProtoType().equals("sfixed32")) {
            w.format("                _w.writeFixedInt32(%s);\n", name);
        } else if (field.getProtoType().equals("sfixed64")) {
            w.format("                _w.writeFixedInt64(%s);\n", name);
        } else if (field.getProtoType().equals("double")) {
            w.format("                _w.writeDouble(%s);\n", name);
        } else if (field.getProtoType().equals("float")) {
            w.format("                _w.writeFloat(%s);\n", name);
        } else {
            throw new IllegalArgumentException(
                    "Failed to write serializer for field: " + field.getProtoType());
        }
    }

    static String parseNumber(Field<?> field) {
        if (field.isEnumField()) {
            return String.format(
                    "%s.valueOf(ProtoCodecUtils.readVarInt(_buffer))", field.getJavaType());
        } else if (field.getProtoType().equals("bool")) {
            return "ProtoCodecUtils.readVarInt(_buffer) == 1";
        } else if (field.getProtoType().equals("int32")) {
            return "ProtoCodecUtils.readVarInt(_buffer)";
        } else if (field.getProtoType().equals("uint32")) {
            return "ProtoCodecUtils.readVarInt(_buffer)";
        } else if (field.getProtoType().equals("sint32")) {
            return "ProtoCodecUtils.readSignedVarInt(_buffer)";
        } else if (field.getProtoType().equals("sint64")) {
            return "ProtoCodecUtils.readSignedVarInt64(_buffer)";
        } else if (field.getProtoType().equals("int64")) {
            return "ProtoCodecUtils.readVarInt64(_buffer)";
        } else if (field.getProtoType().equals("uint64")) {
            return "ProtoCodecUtils.readVarInt64(_buffer)";
        } else if (field.getProtoType().equals("fixed32")) {
            return "ProtoCodecUtils.readFixedInt32(_buffer)";
        } else if (field.getProtoType().equals("fixed64")) {
            return "ProtoCodecUtils.readFixedInt64(_buffer)";
        } else if (field.getProtoType().equals("sfixed32")) {
            return "ProtoCodecUtils.readFixedInt32(_buffer)";
        } else if (field.getProtoType().equals("sfixed64")) {
            return "ProtoCodecUtils.readFixedInt64(_buffer)";
        } else if (field.getProtoType().equals("double")) {
            return "ProtoCodecUtils.readDouble(_buffer)";
        } else if (field.getProtoType().equals("float")) {
            return "ProtoCodecUtils.readFloat(_buffer)";
        } else {
            throw new IllegalArgumentException(
                    "Failed to write parser for field: " + field.getProtoType());
        }
    }

    static String serializedSizeOfNumber(Field<?> field, String name) {
        if (field.isEnumField()) {
            return String.format("ProtoCodecUtils.computeVarIntSize(%s.getValue())", name);
        } else if (field.getProtoType().equals("sint32")) {
            return String.format("ProtoCodecUtils.computeSignedVarIntSize(%s)", name);
        } else if (field.getProtoType().equals("sint64")) {
            return String.format("ProtoCodecUtils.computeSignedVarInt64Size(%s)", name);
        } else if (field.getProtoType().equals("int32")) {
            return String.format("ProtoCodecUtils.computeVarIntSize(%s)", name);
        } else if (field.getProtoType().equals("uint32")) {
            return String.format("ProtoCodecUtils.computeVarIntSize(%s)", name);
        } else if (field.getProtoType().equals("int64")) {
            return String.format("ProtoCodecUtils.computeVarInt64Size(%s)", name);
        } else if (field.getProtoType().equals("uint64")) {
            return String.format("ProtoCodecUtils.computeVarInt64Size(%s)", name);
        } else if (field.getProtoType().equals("fixed32")) {
            return "4";
        } else if (field.getProtoType().equals("fixed64")) {
            return "8";
        } else if (field.getProtoType().equals("sfixed32")) {
            return "4";
        } else if (field.getProtoType().equals("sfixed64")) {
            return "8";
        } else if (field.getProtoType().equals("bool")) {
            return "1";
        } else if (field.getProtoType().equals("double")) {
            return "8";
        } else if (field.getProtoType().equals("float")) {
            return "4";
        } else {
            throw new IllegalArgumentException(
                    "Failed to write serializer for field: " + field.getProtoType());
        }
    }

    static String typeTag(Field<?> field) {
        if (field.isEnumField()) {
            return "ProtoCodecUtils.WIRETYPE_VARINT";
        } else {
            return typeToTag.get(field.getProtoType());
        }
    }

    public void getter(PrintWriter w) {
        if (isErrorField) {
            w.format("       @Override\n");
        }
        w.format(
                "        public %s %s() {\n",
                field.getJavaType(), ProtoGenUtil.camelCase("get", field.getName()));
        if (!field.isDefaultValueSet()) {
            w.format("            if (!%s()) {\n", ProtoGenUtil.camelCase("has", ccName));
            w.format(
                    "                throw new IllegalStateException(\"Field '%s' is not set\");\n",
                    field.getName());
            w.format("            }\n");
        }
        w.format("            return %s;\n", ccName);
        w.format("        }\n");
    }

    @Override
    public void parse(PrintWriter w) {
        w.format("%s = %s;\n", ccName, parseNumber(field));
    }

    @Override
    public void serialize(PrintWriter w) {
        w.format("_w.writeVarInt(%s);\n", tagName());
        serializeNumber(w, field, ccName);
    }

    @Override
    public void setter(PrintWriter w, String enclosingType) {
        if (isErrorField) {
            w.format("       @Override\n");
        }
        w.format(
                "public %s %s(%s %s) {\n",
                enclosingType,
                ProtoGenUtil.camelCase("set", field.getName()),
                field.getJavaType(),
                ProtoGenUtil.camelCase(field.getName()));
        w.format(
                "    this.%s = %s;\n",
                ProtoGenUtil.camelCase(field.getName()), ProtoGenUtil.camelCase(field.getName()));
        w.format("    _bitField%d |= %s;\n", bitFieldIndex(), fieldMask());
        w.format("    _cachedSize = -1;\n");
        w.format("    return this;\n");
        w.format("}\n");
    }

    @Override
    public void declaration(PrintWriter w) {
        if (field.isDefaultValueSet()) {
            w.format(
                    "private %s %s = %s;\n",
                    field.getJavaType(), ccName, field.getDefaultValueAsString());
        } else {
            w.format("private %s %s;\n", field.getJavaType(), ccName);
        }
    }

    @Override
    public void copy(PrintWriter w) {
        w.format("%s(_other.%s);\n", ProtoGenUtil.camelCase("set", ccName), ccName);
    }

    @Override
    public void clear(PrintWriter w) {
        if (field.isDefaultValueSet()) {
            w.format("%s = %s;\n", ccName, field.getDefaultValueAsString());
        }
    }

    @Override
    public void totalSize(PrintWriter w) {
        w.format("_size += %s_SIZE;\n", tagName());
        w.format("_size += %s;\n", serializedSizeOfNumber(field, ccName));
    }

    @Override
    protected String typeTag() {
        return typeTag(field);
    }
}
