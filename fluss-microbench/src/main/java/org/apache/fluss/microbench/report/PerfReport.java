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

package org.apache.fluss.microbench.report;

import org.apache.fluss.microbench.config.ScenarioConfig;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Result of one benchmark scenario. */
public class PerfReport {

    @JsonProperty private final String scenarioName;
    @JsonProperty private final String configSnapshot;
    @JsonProperty private final List<PhaseResult> phaseResults;
    @JsonProperty private final EnvironmentSnapshot environment;
    @JsonProperty private final String status;
    @JsonProperty private final String error;

    private PerfReport(
            String scenarioName,
            String configSnapshot,
            List<PhaseResult> phaseResults,
            EnvironmentSnapshot environment,
            String status,
            String error) {
        this.scenarioName = scenarioName;
        this.configSnapshot = configSnapshot;
        this.phaseResults = Collections.unmodifiableList(new ArrayList<>(phaseResults));
        this.environment = environment;
        this.status = status;
        this.error = error;
    }

    public static PerfReport build(
            ScenarioConfig config,
            List<PhaseResult> phaseResults,
            EnvironmentSnapshot environment,
            String status,
            String error) {
        String name = config.meta() != null ? config.meta().name() : "unnamed";
        return new PerfReport(name, config.rawYaml(), phaseResults, environment, status, error);
    }

    /** Returns the scenario YAML for the separate snapshot file. */
    public String configSnapshot() {
        return configSnapshot;
    }

    public String status() {
        return status;
    }
}
