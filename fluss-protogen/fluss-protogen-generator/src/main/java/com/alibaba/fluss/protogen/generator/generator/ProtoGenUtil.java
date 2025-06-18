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

import com.alibaba.fluss.record.bytesview.ByteBufBytesView;
import com.alibaba.fluss.record.bytesview.BytesView;
import com.alibaba.fluss.record.bytesview.FileRegionBytesView;
import com.alibaba.fluss.record.bytesview.MemorySegmentBytesView;
import com.alibaba.fluss.record.send.ByteBufWritableOutput;
import com.alibaba.fluss.record.send.WritableOutput;
import com.alibaba.fluss.rpc.messages.ApiMessage;
import com.alibaba.fluss.rpc.messages.ErrorMessage;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import com.alibaba.fluss.utils.ProtoCodecUtils;

import java.io.PrintWriter;

import static com.alibaba.fluss.shaded.guava32.com.google.common.base.CaseFormat.LOWER_CAMEL;
import static com.alibaba.fluss.shaded.guava32.com.google.common.base.CaseFormat.LOWER_UNDERSCORE;

/** Utility class for String manipulation. */
public class ProtoGenUtil {
    private static final String[] HEADER =
            new String[] {
                "/*",
                " * Licensed to the Apache Software Foundation (ASF) under one or more",
                " * contributor license agreements.  See the NOTICE file distributed with",
                " * this work for additional information regarding copyright ownership.",
                " * The ASF licenses this file to You under the Apache License, Version 2.0",
                " * (the \"License\"); you may not use this file except in compliance with",
                " * the License.  You may obtain a copy of the License at",
                " *",
                " *    http://www.apache.org/licenses/LICENSE-2.0",
                " *",
                " * Unless required by applicable law or agreed to in writing, software",
                " * distributed under the License is distributed on an \"AS IS\" BASIS,",
                " * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.",
                " * See the License for the specific language governing permissions and",
                " * limitations under the License.",
                " */",
                "",
                "// THIS CODE IS AUTOMATICALLY GENERATED. DO NOT EDIT.",
                ""
            };

    private static final Class<?>[] IMPORT_CLASSES =
            new Class[] {
                ApiMessage.class,
                ByteBufBytesView.class,
                ByteBufWritableOutput.class,
                BytesView.class,
                ErrorMessage.class,
                FileRegionBytesView.class,
                MemorySegmentBytesView.class,
                ProtoCodecUtils.class,
                WritableOutput.class,
                // netty dependencies
                ByteBuf.class,
                Unpooled.class
            };

    public static void printHeader(PrintWriter pw) {
        for (String line : HEADER) {
            pw.println(line);
        }
    }

    public static void printPackageAndImports(PrintWriter pw, String packageName) {
        pw.format("package %s;\n", packageName);
        pw.println();
        for (Class<?> clazz : IMPORT_CLASSES) {
            // skip importing classes from the same package
            if (!clazz.getPackage().getName().equals(packageName)) {
                pw.format("import %s;\n", clazz.getName());
            }
        }
        pw.println();
    }

    public static String camelCase(String... parts) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            String s = parts[i];
            if (s == null || s.isEmpty()) {
                continue;
            }
            if (s.contains("_")) {
                s = LOWER_UNDERSCORE.to(LOWER_CAMEL, s);
            }

            if (i != 0) {
                sb.append(Character.toUpperCase(s.charAt(0)));
                sb.append(s.substring(1));
            } else {
                sb.append(s);
            }
        }

        return sb.toString();
    }

    public static String camelCaseFirstUpper(String... parts) {
        String s = camelCase(parts);
        return Character.toUpperCase(s.charAt(0)) + s.substring(1);
    }

    public static String upperCase(String... parts) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            String s = LOWER_CAMEL.to(LOWER_UNDERSCORE, parts[i]);
            if (i != 0) {
                sb.append('_');
            }

            sb.append(s);
        }

        return sb.toString().toUpperCase();
    }

    public static String plural(String s) {
        return NameUtilities.pluralize(s);
    }

    public static String singular(String s) {
        return NameUtilities.depluralize(s);
    }
}
