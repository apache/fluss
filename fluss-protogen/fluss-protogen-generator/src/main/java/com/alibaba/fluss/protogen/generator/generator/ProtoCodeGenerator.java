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

import com.alibaba.fluss.shaded.guava32.com.google.common.base.Joiner;
import com.alibaba.fluss.shaded.guava32.com.google.common.base.Splitter;

import io.protostuff.parser.Proto;
import io.protostuff.parser.ProtoUtil;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/** Utilities for generating Java classes from protobufs. */
public class ProtoCodeGenerator {

    public static void generate(
            List<File> inputs, File outputDirectory, String classPrefix, boolean useOuterClass)
            throws Exception {
        for (File input : inputs) {
            Proto proto = new Proto();
            ProtoUtil.loadFrom(input, proto);

            String fileWithoutExtension = Splitter.on(".").splitToList(input.getName()).get(0);
            String outerClassName =
                    ProtoGenUtil.camelCaseFirstUpper(classPrefix, fileWithoutExtension);

            String javaPackageName = proto.getJavaPackageName();
            String javaDir = Joiner.on('/').join(javaPackageName.split("\\."));
            Path targetDir = Paths.get(String.format("%s/%s", outputDirectory, javaDir));

            ProtoGen protogen = new ProtoGen(proto, outerClassName, useOuterClass);
            protogen.generate(targetDir.toFile());
        }
    }
}
