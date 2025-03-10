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

package com.alibaba.fluss.lakehouse.cli;

import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.cli.CliFrontend;
import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.execution.DefaultExecutorServiceLoader;

import java.util.ArrayList;
import java.util.List;

/**
 * Extends from {@link CliFrontend} but override method {@link #executeProgram(Configuration,
 * PackagedProgram)} to respect the configuration key {@link PipelineOptions#JARS} so that the
 * additional jar needed to run lakehouse tiering service can be submitted to fluss cluster as well.
 */
public class FlussCliFrontend extends CliFrontend {

    // the origin flink configuration before method #executeProgram
    private final Configuration originConfiguration;

    public FlussCliFrontend(
            Configuration configuration, List<CustomCommandLine> customCommandLines) {
        super(configuration, customCommandLines);
        this.originConfiguration = configuration;
    }

    @Override
    protected void executeProgram(final Configuration configuration, final PackagedProgram program)
            throws ProgramInvocationException {
        List<String> originPipelineJars = originConfiguration.get(PipelineOptions.JARS);
        if (originPipelineJars != null) {
            List<String> pipelineJars = new ArrayList<>(configuration.get(PipelineOptions.JARS));
            pipelineJars.addAll(originPipelineJars);
            configuration.set(PipelineOptions.JARS, pipelineJars);
        }
        ClientUtils.executeProgram(
                new DefaultExecutorServiceLoader(), configuration, program, false, false);
    }
}
