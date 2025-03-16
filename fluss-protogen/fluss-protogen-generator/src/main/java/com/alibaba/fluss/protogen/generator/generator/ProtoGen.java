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

import io.protostuff.parser.Proto;
import org.jboss.forge.roaster.Roaster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/* This file is based on source code of LightProto Project (https://github.com/splunk/lightproto/),
 * licensed by Splunk, Inc. under the Apache License, Version 2.0. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership. */

/** Entrance to generate a Java class from a protobuf file. */
public class ProtoGen {

    private static final Logger log = LoggerFactory.getLogger(ProtoGen.class);
    private final Proto proto;
    private final String outerClassName;
    private final boolean useOuterClass;
    private final List<ProtobufEnum> enums;
    private final List<ProtobufMessage> messages;

    public ProtoGen(Proto proto, String outerClassName, boolean useOuterClass) {
        this.proto = proto;
        this.outerClassName = outerClassName;
        this.useOuterClass = useOuterClass;
        this.enums =
                proto.getEnumGroups().stream().map(ProtobufEnum::new).collect(Collectors.toList());
        this.messages =
                proto.getMessages().stream()
                        .map(m -> new ProtobufMessage(m, useOuterClass))
                        .collect(Collectors.toList());
    }

    public List<File> generate(File directory) throws IOException {
        directory.mkdirs();

        if (useOuterClass) {
            return generateWithSingleOuterClass(directory);
        } else {
            return generateIndividualClasses(directory);
        }
    }

    public List<File> generateIndividualClasses(File outDirectory) throws IOException {
        List<File> generatedFiles = new ArrayList<>();
        for (ProtobufEnum e : enums) {
            File file = new File(outDirectory, e.getName() + ".java");
            StringWriter sw = new StringWriter();
            try (PrintWriter pw = new PrintWriter(sw)) {
                ProtoGenUtil.printHeader(pw);
                ProtoGenUtil.printPackageAndImports(pw, proto.getJavaPackageName());
                e.generate(pw);
            }

            formatAndWrite(file, sw.toString());
            log.info("ProtoGen generated enum {}", file);
            generatedFiles.add(file);
        }

        for (ProtobufMessage m : messages) {
            File file = new File(outDirectory, m.getName() + ".java");
            StringWriter sw = new StringWriter();
            try (PrintWriter pw = new PrintWriter(sw)) {
                ProtoGenUtil.printHeader(pw);
                ProtoGenUtil.printPackageAndImports(pw, proto.getJavaPackageName());
                m.generate(pw);
            }

            formatAndWrite(file, sw.toString());
            log.info("ProtoGen generated class {}", file);
            generatedFiles.add(file);
        }

        return generatedFiles;
    }

    public List<File> generateWithSingleOuterClass(File outDirectory) throws IOException {
        File outFile = new File(outDirectory, outerClassName + ".java");

        StringWriter sw = new StringWriter();
        try (PrintWriter pw = new PrintWriter(sw)) {
            ProtoGenUtil.printHeader(pw);
            ProtoGenUtil.printPackageAndImports(pw, proto.getJavaPackageName());
            pw.format("public final class %s {\n", outerClassName);
            pw.format("   private %s() {}\n", outerClassName);

            enums.forEach(e -> e.generate(pw));
            messages.forEach(m -> m.generate(pw));

            pw.println("}");
        }

        formatAndWrite(outFile, sw.toString());

        log.info("ProtoGen generated {}", outFile);
        return Collections.singletonList(outFile);
    }

    private void formatAndWrite(File file, String content) throws IOException {
        String formattedCode = Roaster.format(content);
        try (Writer w = Files.newBufferedWriter(file.toPath())) {
            w.write(formattedCode);
        }
    }
}
