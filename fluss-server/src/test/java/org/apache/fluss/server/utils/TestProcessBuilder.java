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

package org.apache.fluss.server.utils;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.testutils.common.CommonTestUtils;
import org.apache.fluss.testutils.common.CommonTestUtils.PipeForwarder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Map;

import static org.apache.fluss.testutils.common.CommonTestUtils.getCurrentClasspath;
import static org.apache.fluss.testutils.common.CommonTestUtils.getJavaCommandPath;

/** Utility class wrapping {@link ProcessBuilder} and pre-configuring it with common options. */
public class TestProcessBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(TestProcessBuilder.class);

    private final String javaCommand = getJavaCommandPath();

    private final ArrayList<String> jvmArgs = new ArrayList<>();
    private final ArrayList<String> mainClassArgs = new ArrayList<>();

    private final String mainClass;

    private MemorySize jvmMemory = MemorySize.parse("80mb");

    private boolean withCleanEnvironment = false;

    public TestProcessBuilder(String mainClass) throws IOException {
        File tempLogFile =
                File.createTempFile(getClass().getSimpleName() + "-", "-log4j.properties");
        tempLogFile.deleteOnExit();
        CommonTestUtils.printLog4jDebugConfig(tempLogFile);

        jvmArgs.add("-Dlog.level=DEBUG");
        jvmArgs.add("-Dlog4j.configurationFile=file:" + tempLogFile.getAbsolutePath());
        jvmArgs.add("-classpath");
        jvmArgs.add(getCurrentClasspath());
        jvmArgs.add("-XX:+IgnoreUnrecognizedVMOptions");

        final String moduleConfig = System.getProperty("surefire.module.config");
        if (moduleConfig != null) {
            for (String moduleArg : moduleConfig.split(" ")) {
                addJvmArg(moduleArg);
            }
        }

        this.mainClass = mainClass;
    }

    public TestProcess start() throws IOException {
        final ArrayList<String> commands = new ArrayList<>();

        commands.add(javaCommand);
        commands.add(String.format("-Xms%dm", jvmMemory.getMebiBytes()));
        commands.add(String.format("-Xmx%dm", jvmMemory.getMebiBytes()));
        commands.addAll(jvmArgs);
        commands.add(mainClass);
        commands.addAll(mainClassArgs);

        StringWriter processOutput = new StringWriter();
        StringWriter errorOutput = new StringWriter();
        LOG.info("Starting process with commands {}", commands);
        final ProcessBuilder processBuilder = new ProcessBuilder(commands);
        if (withCleanEnvironment) {
            processBuilder.environment().clear();
        }
        Process process = processBuilder.start();
        new PipeForwarder(process.getInputStream(), processOutput);
        new PipeForwarder(process.getErrorStream(), errorOutput);

        return new TestProcess(process, processOutput, errorOutput);
    }

    public TestProcessBuilder setJvmMemory(MemorySize jvmMemory) {
        this.jvmMemory = jvmMemory;
        return this;
    }

    public TestProcessBuilder addJvmArg(String arg) {
        jvmArgs.add(arg);
        return this;
    }

    public TestProcessBuilder addMainClassArg(String arg) {
        mainClassArgs.add(arg);
        return this;
    }

    public TestProcessBuilder addConfigAsMainClassArgs(Configuration config) {
        for (Map.Entry<String, String> keyValue : config.toMap().entrySet()) {
            addMainClassArg("--" + keyValue.getKey());
            addMainClassArg(keyValue.getValue());
        }
        return this;
    }

    public TestProcessBuilder withCleanEnvironment() {
        withCleanEnvironment = true;
        return this;
    }

    /** {@link Process} with it's {@code processOutput}. */
    public static class TestProcess {
        private final Process process;
        private final StringWriter processOutput;
        private final StringWriter errorOutput;

        public TestProcess(Process process, StringWriter processOutput, StringWriter errorOutput) {
            this.process = process;
            this.processOutput = processOutput;
            this.errorOutput = errorOutput;
        }

        public Process getProcess() {
            return process;
        }

        public StringWriter getProcessOutput() {
            return processOutput;
        }

        public StringWriter getErrorOutput() {
            return errorOutput;
        }

        public void destroy() {
            process.destroy();
        }

        public static void printProcessLog(String processName, TestProcess process) {
            if (process == null) {
                System.out.println("-----------------------------------------");
                System.out.println(" PROCESS " + processName + " WAS NOT STARTED.");
                System.out.println("-----------------------------------------");
            } else {
                System.out.println("-----------------------------------------");
                System.out.println(" BEGIN SPAWNED PROCESS LOG FOR " + processName);
                System.out.println("-----------------------------------------");
                System.out.println(process.getProcessOutput().toString());
                System.out.println("-----------------------------------------");
                System.out.println("		END SPAWNED PROCESS LOG");
                System.out.println("-----------------------------------------");
            }
        }
    }
}
