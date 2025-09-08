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

/** Describes a repeated enum field in a protobuf message. */
public class ProtobufRepeatedEnumField extends ProtobufRepeatedNumberField {

    public ProtobufRepeatedEnumField(Field<?> field, int index) {
        super(field, index);
    }

    @Override
    public void parse(PrintWriter w) {
        w.format(
                "%s _%s = %s;\n",
                field.getJavaType(), ccName, ProtobufNumberField.parseNumber(field));
        w.format("if (_%s != null) {\n", ccName);
        w.format("   %s(_%s);\n", ProtoGenUtil.camelCase("add", singularName), ccName);
        w.format("}\n");
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
                "    %s _%sPacked = %s;\n",
                field.getJavaType(), ccName, ProtobufNumberField.parseNumber(field));
        w.format("    if (_%sPacked != null) {\n", ccName);
        w.format("        %s(_%sPacked);\n", ProtoGenUtil.camelCase("add", singularName), ccName);
        w.format("    }\n");
        w.format("}\n");
    }
}
