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

/** Sampling configuration for resource monitoring. */
public class SamplingConfig {

    /** The base sampling interval (e.g. "1s", "500ms"). */
    private String interval;

    /** Optional: lower frequency interval for expensive NMT sampling. */
    private String nmtInterval;

    /** Whether to enable JVM Native Memory Tracking (NMT) sampling. */
    private Boolean nmt;

    /** Whether to enable JDK Flight Recorder (JFR) recording. */
    private Boolean jfr;

    /** JFR configuration name or path (e.g. "profile", "default"). */
    private String jfrConfig;

    public String interval() {
        return interval;
    }

    public void setInterval(String interval) {
        this.interval = interval;
    }

    /**
     * Returns the NMT sampling interval. Falls back to {@link #interval()} if not explicitly
     * configured.
     */
    public String nmtInterval() {
        return nmtInterval != null ? nmtInterval : interval;
    }

    public void setNmtInterval(String nmtInterval) {
        this.nmtInterval = nmtInterval;
    }

    public Boolean isNmtEnabled() {
        return nmt;
    }

    public void setNmt(Boolean nmt) {
        this.nmt = nmt;
    }

    public Boolean isJfrEnabled() {
        return jfr;
    }

    public void setJfr(Boolean jfr) {
        this.jfr = jfr;
    }

    public String jfrConfig() {
        return jfrConfig;
    }

    public void setJfrConfig(String jfrConfig) {
        this.jfrConfig = jfrConfig;
    }
}
