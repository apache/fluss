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

import java.io.PrintWriter;

/** Generator for a boolean field. */
public class ProtobufBooleanField extends ProtobufNumberField {

    public ProtobufBooleanField(Field<?> field, int index) {
        super(field, index, false);
    }

    @Override
    public void getter(PrintWriter w) {
        w.format(
                "        public %s %s() {\n",
                field.getJavaType(), ProtoGenUtil.camelCase("is", ccName));
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
}
