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
import io.protostuff.parser.MessageField;

import java.io.PrintWriter;

/** Describes a field in a protobuf message. */
public abstract class ProtobufField<FieldType extends Field<?>> {

    protected final FieldType field;
    protected final int index;
    protected final String ccName;
    protected final boolean isErrorField;

    protected ProtobufField(FieldType field, int index, boolean isErrorField) {
        this.field = field;
        this.index = index;
        this.ccName = ProtoGenUtil.camelCase(field.getName());
        this.isErrorField = isErrorField;
    }

    public static ProtobufField create(Field field, int index, boolean isErrorField) {
        if (field.isRepeated()) {
            if (field.isMessageField()) {
                return new ProtobufRepeatedMessageField((MessageField) field, index);
            } else if (field.isStringField()) {
                return new ProtobufRepeatedStringField((Field.String) field, index);
            } else if (field.isEnumField()) {
                return new ProtobufRepeatedEnumField(field, index);
            } else if (field.isNumberField() || field.isBoolField()) {
                return new ProtobufRepeatedNumberField(field, index);
            } else if (field.isBytesField()) {
                return new ProtobufRepeatedBytesField((Field.Bytes) field, index);
            }
        } else if (field.isMessageField()) {
            return new ProtobufMessageField((MessageField) field, index);
        } else if (field.isBytesField()) {
            if (field.getName().equals(RecordsFieldFinder.RECORDS_FIELD_NAME)) {
                // add zero-copy and lazy deserialization support for "bytes records"
                return new ProtobufRecordsField((Field.Bytes) field, index);
            } else {
                return new ProtobufBytesField((Field.Bytes) field, index);
            }
        } else if (field.isStringField()) {
            return new ProtobufStringField((Field.String) field, index, isErrorField);
        } else if (field.isEnumField()) {
            return new ProtobufEnumField(field, index);
        } else if (field.isNumberField()) {
            return new ProtobufNumberField(field, index, isErrorField);
        } else if (field.isBoolField()) {
            return new ProtobufBooleanField(field, index);
        }

        throw new IllegalArgumentException("Unknown field: " + field);
    }

    public int index() {
        return index;
    }

    public boolean isRepeated() {
        return field.isRepeated();
    }

    public boolean isEnum() {
        return field.isEnumField();
    }

    public boolean isRequired() {
        return field.isRequired();
    }

    public void docs(PrintWriter w) {
        field.getDocs().forEach(d -> w.format("        // %s\n", d));
    }

    public abstract void declaration(PrintWriter w);

    public void tags(PrintWriter w) {
        w.format("        private static final int %s = %d;\n", fieldNumber(), field.getNumber());
        w.format(
                "        private static final int %s = (%s << ProtoCodecUtils.TAG_TYPE_BITS) | %s;\n",
                tagName(), fieldNumber(), typeTag());
        w.format(
                "        private static final int %s_SIZE = ProtoCodecUtils.computeVarIntSize(%s);\n",
                tagName(), tagName());
        if (!field.isRepeated()) {
            w.format(
                    "        private static final int %s = 1 << (%d %% 32);\n", fieldMask(), index);
        }
    }

    public void has(PrintWriter w) {
        if (isErrorField) {
            w.format("        @Override\n");
        }
        w.format("        public boolean %s() {\n", ProtoGenUtil.camelCase("has", field.getName()));
        w.format("            return (_bitField%d & %s) != 0;\n", bitFieldIndex(), fieldMask());
        w.format("        }\n");
    }

    public abstract void clear(PrintWriter w);

    public void fieldClear(PrintWriter w, String enclosingType) {
        if (isErrorField) {
            w.format("       @Override\n");
        }
        w.format(
                "        public %s %s() {\n",
                enclosingType, ProtoGenUtil.camelCase("clear", field.getName()));
        w.format("            _bitField%d &= ~%s;\n", bitFieldIndex(), fieldMask());
        clear(w);
        w.format("            return this;\n");
        w.format("        }\n");
    }

    public abstract void setter(PrintWriter w, String enclosingType);

    public abstract void getter(PrintWriter w);

    public abstract void totalSize(PrintWriter w);

    public void zeroCopySize(PrintWriter w) {
        // size += 0 by default
    }

    public abstract void serialize(PrintWriter w);

    public abstract void parse(PrintWriter w);

    public abstract void copy(PrintWriter w);

    public boolean isPackable() {
        return field.isRepeated() && field.isPackable();
    }

    public void parsePacked(PrintWriter w) {}

    protected abstract String typeTag();

    protected String tagName() {
        return "_" + ProtoGenUtil.upperCase(field.getName(), "tag");
    }

    protected String fieldNumber() {
        return "_" + ProtoGenUtil.upperCase(field.getName(), "fieldNumber");
    }

    protected String fieldMask() {
        return "_" + ProtoGenUtil.upperCase(field.getName(), "mask");
    }

    protected int bitFieldIndex() {
        return index / 32;
    }
}
