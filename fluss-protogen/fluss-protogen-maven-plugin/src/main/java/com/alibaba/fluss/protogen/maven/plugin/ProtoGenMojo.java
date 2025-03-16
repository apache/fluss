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

package com.alibaba.fluss.protogen.maven.plugin;

import com.alibaba.fluss.protogen.generator.generator.ProtoCodeGenerator;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.apache.maven.project.MavenProject;

import java.io.File;
import java.util.Arrays;
import java.util.List;

/* This file is based on source code of LightProto Project (https://github.com/splunk/lightproto/),
 * licensed by Splunk, Inc. under the Apache License, Version 2.0. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership. */

/** Goal which generates Java code from the proto definition. */
@Mojo(
        name = "generate",
        defaultPhase = LifecyclePhase.GENERATE_SOURCES,
        requiresDependencyResolution = ResolutionScope.COMPILE,
        threadSafe = true)
public class ProtoGenMojo extends AbstractMojo {

    @Parameter(property = "classPrefix", defaultValue = "", required = false)
    private String classPrefix;

    @Parameter(property = "singleOuterClass", defaultValue = "false", required = false)
    private boolean singleOuterClass;

    @Parameter(property = "sources", required = false)
    private List<File> sources;

    @Parameter(defaultValue = "${project}", required = true, readonly = true)
    private MavenProject project;

    @Parameter(defaultValue = "generated-sources/protobuf/java", required = false)
    private String targetSourcesSubDir;

    @Parameter(defaultValue = "generated-test-sources/protobuf/java", required = false)
    private String targetTestSourcesSubDir;

    private void generate(List<File> protoFiles, File outputDirectory)
            throws MojoExecutionException {
        try {
            ProtoCodeGenerator.generate(protoFiles, outputDirectory, classPrefix, singleOuterClass);
        } catch (Exception e) {
            getLog().error(
                            "Failed to generate protogen code for "
                                    + protoFiles
                                    + ": "
                                    + e.getMessage(),
                            e);
            throw new MojoExecutionException(
                    "Failed to generate protogen code for " + protoFiles, e);
        }
    }

    public void execute() throws MojoExecutionException {
        File baseDir = project.getBasedir();
        File targetDir = new File(project.getBuild().getDirectory());

        if (sources == null || sources.isEmpty()) {
            File[] mainFilesArray =
                    new File(baseDir, "src/main/proto")
                            .listFiles((dir, name) -> name.endsWith(".proto"));
            if (mainFilesArray != null && mainFilesArray.length > 0) {
                List<File> mainFiles = Arrays.asList(mainFilesArray);
                File generatedSourcesDir = new File(targetDir, targetSourcesSubDir);
                generate(mainFiles, new File(targetDir, targetSourcesSubDir));

                project.addCompileSourceRoot(generatedSourcesDir.toString());
            }

            File[] testFilesArray =
                    new File(baseDir, "src/test/proto")
                            .listFiles((dir, name) -> name.endsWith(".proto"));
            if (testFilesArray != null && testFilesArray.length > 0) {
                List<File> testFiles = Arrays.asList(testFilesArray);
                File generatedTestSourcesDir = new File(targetDir, targetTestSourcesSubDir);
                generate(testFiles, generatedTestSourcesDir);

                project.addTestCompileSourceRoot(generatedTestSourcesDir.toString());
            }
        } else {
            File generatedSourcesDir = new File(targetDir, targetSourcesSubDir);
            generate(sources, generatedSourcesDir);
            project.addCompileSourceRoot(generatedSourcesDir.toString());
        }
    }
}
