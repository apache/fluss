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

import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/** Runtime details needed to interpret a benchmark result. */
public final class EnvironmentSnapshot {

    @JsonProperty private final String jvmVersion;
    @JsonProperty private final String osName;
    @JsonProperty private final int cpuCores;

    private EnvironmentSnapshot(String jvmVersion, String osName, int cpuCores) {
        this.jvmVersion = jvmVersion;
        this.osName = osName;
        this.cpuCores = cpuCores;
    }

    public static EnvironmentSnapshot capture() {
        return new EnvironmentSnapshot(
                System.getProperty("java.version"),
                System.getProperty("os.name"),
                Runtime.getRuntime().availableProcessors());
    }
}
