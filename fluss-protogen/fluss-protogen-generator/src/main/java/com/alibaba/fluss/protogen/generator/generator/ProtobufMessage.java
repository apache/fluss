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

import com.alibaba.fluss.record.send.ByteBufWritableOutput;

import io.protostuff.parser.Field;
import io.protostuff.parser.Message;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/* This file is based on source code of LightProto Project (https://github.com/splunk/lightproto/),
 * licensed by Splunk, Inc. under the Apache License, Version 2.0. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership. */

/** Describes a protobuf message. */
public class ProtobufMessage {

    private final Message message;
    private final boolean isNested;
    private final List<ProtobufEnum> enums;
    private final List<ProtobufField> fields;
    private final List<ProtobufMessage> nestedMessages;
    private final boolean hasErrorFields;

    public ProtobufMessage(Message message, boolean isNested) {
        this.message = message;
        this.isNested = isNested;
        this.enums =
                message.getNestedEnumGroups().stream()
                        .map(ProtobufEnum::new)
                        .collect(Collectors.toList());
        this.nestedMessages =
                message.getNestedMessages().stream()
                        .map(m -> new ProtobufMessage(m, true))
                        .collect(Collectors.toList());

        this.fields = new ArrayList<>();
        this.hasErrorFields = hasErrorFields(message);
        for (int i = 0; i < message.getFields().size(); i++) {
            Field<?> field = message.getFields().get(i);
            boolean isErrorField =
                    hasErrorFields && (isErrorCodeField(field) || isErrorMessageField(field));
            fields.add(ProtobufField.create(message.getFields().get(i), i, isErrorField));
        }
    }

    public String getName() {
        return message.getName();
    }

    public void generate(PrintWriter w) {
        String baseInterface = hasErrorFields ? "ApiMessage, ErrorMessage" : "ApiMessage";
        w.println();
        w.format(
                "    public %s final class %s implements %s {\n",
                isNested ? "static" : "", message.getName(), baseInterface);

        enums.forEach(e -> e.generate(w));
        nestedMessages.forEach(nm -> nm.generate(w));
        fields.forEach(
                field -> {
                    field.docs(w);
                    field.declaration(w);
                    field.tags(w);
                    w.println();
                    field.has(w);
                    w.println();
                    field.getter(w);
                    w.println();
                    field.setter(w, message.getName());
                    w.println();
                    field.fieldClear(w, message.getName());
                    w.println();
                });

        generateBitFields(w);
        generateSerialize(w);
        generateZeroCopySerialize(w);
        generateTotalSize(w);
        generateZeroCopySize(w);
        generateParseFrom(w);
        generateIsLazilyParsed(w);
        generateCheckRequiredFields(w);
        generateClear(w);
        generateCopyFrom(w);

        w.println();
        w.println("        @Override");
        w.println("        public byte[] toByteArray() {");
        w.println("            byte[] a = new byte[this.totalSize()];");
        w.println("            ByteBuf b = Unpooled.wrappedBuffer(a).writerIndex(0);");
        w.println("            this.writeTo(b);");
        w.println("            return a;");
        w.println("        }");

        w.println();
        w.println("        @Override");
        w.println("        public void parseFrom(byte[] a) {");
        w.println("            ByteBuf b = Unpooled.wrappedBuffer(a);");
        w.println("            this.parseFrom(b, b.readableBytes());");
        w.println("        }");

        w.println("        private int _cachedSize = -1;\n");
        w.println("        private ByteBuf _parsedBuffer;\n");
        w.println("    }");
        w.println();
    }

    private void generateParseFrom(PrintWriter w) {
        w.println();
        w.println("       @Override");
        w.format("        public void parseFrom(ByteBuf _buffer, int _size) {\n");
        w.format("            clear();\n");
        w.format("            int _endIdx = _buffer.readerIndex() + _size;\n");
        w.format("            while (_buffer.readerIndex() < _endIdx) {\n");
        w.format("                int _tag = ProtoCodecUtils.readVarInt(_buffer);\n");
        w.format("                switch (_tag) {\n");

        for (ProtobufField field : fields) {
            w.format("                case %s:\n", field.tagName());
            if (!field.isRepeated() && !field.isEnum()) {
                w.format(
                        "                    _bitField%d |= %s;\n",
                        field.bitFieldIndex(), field.fieldMask());
            }
            field.parse(w);
            w.format("                    break;\n");
        }

        for (ProtobufField field : fields) {
            if (field.isPackable()) {
                w.format("                case %s_PACKED:\n", field.tagName());
                field.parsePacked(w);
                w.format("                    break;\n");
            }
        }

        w.format("                default:\n");
        w.format("                    ProtoCodecUtils.skipUnknownField(_tag, _buffer);\n");
        w.format("                }\n");
        w.format("            }\n");
        if (hasRequiredFields()) {
            w.format("            checkRequiredFields();\n");
        }
        w.format("            _parsedBuffer = _buffer;\n");
        w.format("        }\n");
    }

    private void generateIsLazilyParsed(PrintWriter w) {
        w.println();
        w.println("       @Override");
        w.format("        public boolean isLazilyParsed() {\n");
        w.format(
                "            return %s;\n",
                RecordsFieldFinder.hasRecordsField(message) ? "true" : "false");
        w.format("        }\n");
    }

    private void generateClear(PrintWriter w) {
        w.println();
        w.format("        public %s clear() {\n", message.getName());
        for (ProtobufField f : fields) {
            f.clear(w);
        }

        w.format("            _parsedBuffer = null;\n");
        w.format("            _cachedSize = -1;\n");
        for (int i = 0; i < bitFieldsCount(); i++) {
            w.format("            _bitField%d = 0;\n", i);
        }

        w.format("            return this;\n");
        w.format("        }\n");
    }

    private void generateCopyFrom(PrintWriter w) {
        w.println();
        w.format("public %s copyFrom(%s _other) {\n", message.getName(), message.getName());
        w.format("            _cachedSize = -1;\n");
        for (ProtobufField f : fields) {
            if (f.isRepeated()) {
                f.copy(w);
            } else {
                w.format("    if (_other.%s()) {\n", ProtoGenUtil.camelCase("has", f.ccName));
                f.copy(w);
                w.format("    }\n");
            }
        }

        w.format("            return this;\n");
        w.format("        }\n");
    }

    private void generateSerialize(PrintWriter w) {
        String writableClass = ByteBufWritableOutput.class.getSimpleName();
        w.println();
        w.println("       @Override");
        w.format("        public int writeTo(ByteBuf _b) {\n");
        w.format("            int _writeIdx = _b.writerIndex();\n");
        w.format("            %s _w = new %s(_b);\n", writableClass, writableClass);
        w.format("            this.writeTo(_w);\n");
        w.format("            return (_b.writerIndex() - _writeIdx);\n");
        w.format("        }\n");
    }

    private void generateZeroCopySerialize(PrintWriter w) {
        w.println();
        w.println("       @Override");
        w.format("        public void writeTo(WritableOutput _w) {\n");
        if (hasRequiredFields()) {
            w.format("            checkRequiredFields();\n");
        }
        for (ProtobufField f : fields) {
            if (f.isRequired() || f.isRepeated()) {
                // If required, skip the has() check
                f.serialize(w);
            } else {
                w.format(
                        "            if (%s()) {\n",
                        ProtoGenUtil.camelCase("has", f.field.getName()));
                f.serialize(w);
                w.format("            }\n");
            }
        }
        w.format("        }\n");
    }

    private void generateTotalSize(PrintWriter w) {
        w.println();
        w.println("@Override");
        w.format("public int totalSize() {\n");
        if (hasRequiredFields()) {
            w.format("            checkRequiredFields();\n");
        }
        w.format("    if (_cachedSize > -1) {\n");
        w.format("        return _cachedSize;\n");
        w.format("    }\n");
        w.format("\n");

        w.format("    int _size = 0;\n");
        fields.forEach(
                field -> {
                    if (field.isRequired() || field.isRepeated()) {
                        field.totalSize(w);
                    } else {
                        w.format(
                                "        if (%s()) {\n",
                                ProtoGenUtil.camelCase("has", field.field.getName()));
                        field.totalSize(w);
                        w.format("        }\n");
                    }
                });

        w.format("            _cachedSize = _size;\n");
        w.format("            return _size;\n");
        w.format("        }\n");
    }

    private void generateZeroCopySize(PrintWriter w) {
        w.println();
        w.println("@Override");
        w.format("public int zeroCopySize() {\n");
        w.format("    int _size = 0;\n");
        fields.forEach(field -> field.zeroCopySize(w));
        w.format("    return _size;\n");
        w.format("}\n");
    }

    private void generateBitFields(PrintWriter w) {
        for (int i = 0; i < bitFieldsCount(); i++) {
            w.format("private int _bitField%d;\n", i);
            w.format("private static final int _REQUIRED_FIELDS_MASK%d = 0", i);
            int idx = i;
            fields.forEach(
                    f -> {
                        if (f.isRequired() && f.index() / 32 == idx) {
                            w.format(" | %s", f.fieldMask());
                        }
                    });
            w.println(";");
        }
    }

    private void generateCheckRequiredFields(PrintWriter w) {
        if (!hasRequiredFields()) {
            return;
        }

        w.println();
        w.format("        private void checkRequiredFields() {\n");
        w.format("            if (");
        for (int i = 0; i < bitFieldsCount(); i++) {
            if (i != 0) {
                w.print("\n             || ");
            }

            w.format("(_bitField%d & _REQUIRED_FIELDS_MASK%d) != _REQUIRED_FIELDS_MASK%d", i, i, i);
        }

        w.format(")   {\n");
        w.format("      throw new IllegalStateException(\"Some required fields are missing\");\n");
        w.format("    }\n");
        w.format("}\n");
    }

    private int bitFieldsCount() {
        if (message.getFieldCount() == 0) {
            return 0;
        }

        return (int) Math.ceil(message.getFields().size() / 32.0);
    }

    private boolean hasRequiredFields() {
        for (ProtobufField field : fields) {
            if (field.isRequired()) {
                return true;
            }
        }

        return false;
    }

    private static boolean hasErrorFields(Message message) {
        boolean findErrorCode = false;
        boolean findErrorMessage = false;
        for (Field<?> field : message.getFields()) {
            if (!findErrorCode && isErrorCodeField(field)) {
                findErrorCode = true;
            } else if (!findErrorMessage && isErrorMessageField(field)) {
                findErrorMessage = true;
            }
        }
        return findErrorCode && findErrorMessage;
    }

    private static boolean isErrorCodeField(Field<?> field) {
        return field.getName().equals("error_code")
                && field.getJavaType().equals("int")
                && !field.isRepeated();
    }

    private static boolean isErrorMessageField(Field<?> field) {
        return field.getName().equals("error_message")
                && field.getJavaType().equals("String")
                && !field.isRepeated();
    }
}
