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

package org.apache.fluss.microbench.config;

import java.util.List;
import java.util.Map;

/** Cluster configuration (tablet servers, JVM args, Fluss config overrides). */
public class ClusterConfig {

    private Integer tabletServers;
    private List<String> jvmArgs;
    private Map<String, String> configOverrides;

    public Integer tabletServers() {
        return tabletServers;
    }

    public void setTabletServers(Integer tabletServers) {
        this.tabletServers = tabletServers;
    }

    public List<String> jvmArgs() {
        return jvmArgs;
    }

    public void setJvmArgs(List<String> jvmArgs) {
        this.jvmArgs = jvmArgs;
    }

    public Map<String, String> configOverrides() {
        return configOverrides;
    }

    public void setConfig(Map<String, String> configOverrides) {
        this.configOverrides = configOverrides;
    }
}
