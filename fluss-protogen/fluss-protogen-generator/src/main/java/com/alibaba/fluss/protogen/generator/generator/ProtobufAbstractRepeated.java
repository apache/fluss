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

/** Abstract class for repeated fields. */
public abstract class ProtobufAbstractRepeated<FieldType extends Field<?>>
        extends ProtobufField<FieldType> {

    protected final String pluralName;
    protected final String singularName;

    public ProtobufAbstractRepeated(FieldType field, int index) {
        super(field, index, false);
        this.pluralName = ProtoGenUtil.plural(ccName);
        this.singularName = ProtoGenUtil.singular(ccName);
    }

    public void has(PrintWriter w) {}

    public void fieldClear(PrintWriter w, String enclosingType) {
        w.format(
                "        public %s %s() {\n",
                enclosingType, ProtoGenUtil.camelCase("clear", field.getName()));
        clear(w);
        w.format("            return this;\n");
        w.format("        }\n");
    }
}
