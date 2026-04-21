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

import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.net.URL;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

/** Immutable snapshot of the runtime environment captured at benchmark start. */
public class EnvironmentSnapshot {

    private final String jvmVersion;
    private final List<String> jvmArgs;
    private final String osName;
    private final String osVersion;
    private final int cpuCores;
    private final long totalMemoryBytes;
    private final String flussVersion;
    private final String flussCommit;

    @JsonCreator
    public EnvironmentSnapshot(
            @JsonProperty("jvmVersion") String jvmVersion,
            @JsonProperty("jvmArgs") List<String> jvmArgs,
            @JsonProperty("osName") String osName,
            @JsonProperty("osVersion") String osVersion,
            @JsonProperty("cpuCores") int cpuCores,
            @JsonProperty("totalMemoryBytes") long totalMemoryBytes,
            @JsonProperty("flussVersion") String flussVersion,
            @JsonProperty("flussCommit") String flussCommit) {
        this.jvmVersion = jvmVersion;
        this.jvmArgs =
                jvmArgs == null ? Collections.emptyList() : Collections.unmodifiableList(jvmArgs);
        this.osName = osName;
        this.osVersion = osVersion;
        this.cpuCores = cpuCores;
        this.totalMemoryBytes = totalMemoryBytes;
        this.flussVersion = flussVersion;
        this.flussCommit = flussCommit;
    }

    /** Captures the current runtime environment. */
    public static EnvironmentSnapshot capture() {
        RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
        String jvmVersion = System.getProperty("java.version", "unknown");
        List<String> jvmArgs = runtime.getInputArguments();
        String osName = System.getProperty("os.name", "unknown");
        String osVersion = System.getProperty("os.version", "unknown");
        int cpuCores = Runtime.getRuntime().availableProcessors();
        long totalMemoryBytes = getTotalPhysicalMemory();
        String[] versionInfo = readFlussVersionFromManifest();
        return new EnvironmentSnapshot(
                jvmVersion,
                jvmArgs,
                osName,
                osVersion,
                cpuCores,
                totalMemoryBytes,
                versionInfo[0],
                versionInfo[1]);
    }

    private static long getTotalPhysicalMemory() {
        try {
            OperatingSystemMXBean osMxBean = ManagementFactory.getOperatingSystemMXBean();
            if (osMxBean instanceof com.sun.management.OperatingSystemMXBean) {
                return ((com.sun.management.OperatingSystemMXBean) osMxBean)
                        .getTotalPhysicalMemorySize();
            }
        } catch (Exception ignored) {
            // Not a HotSpot JVM or method not available.
        }
        return Runtime.getRuntime().maxMemory();
    }

    private static String[] readFlussVersionFromManifest() {
        String version = "unknown";
        String commit = "unknown";
        try {
            Enumeration<URL> resources =
                    EnvironmentSnapshot.class.getClassLoader().getResources("META-INF/MANIFEST.MF");
            while (resources.hasMoreElements()) {
                try (InputStream is = resources.nextElement().openStream()) {
                    Manifest manifest = new Manifest(is);
                    Attributes attrs = manifest.getMainAttributes();
                    String title = attrs.getValue("Implementation-Title");
                    if (title != null && title.toLowerCase().contains("fluss")) {
                        String v = attrs.getValue("Implementation-Version");
                        if (v != null) {
                            version = v;
                        }
                        String c = attrs.getValue("Git-Commit");
                        if (c == null) {
                            c = attrs.getValue("Implementation-Build");
                        }
                        if (c != null) {
                            commit = c;
                        }
                        break;
                    }
                }
            }
        } catch (Exception ignored) {
            // Jar manifest not available (e.g. running from IDE).
        }
        return new String[] {version, commit};
    }

    @JsonProperty("jvmVersion")
    public String jvmVersion() {
        return jvmVersion;
    }

    @JsonProperty("jvmArgs")
    public List<String> jvmArgs() {
        return jvmArgs;
    }

    @JsonProperty("osName")
    public String osName() {
        return osName;
    }

    @JsonProperty("osVersion")
    public String osVersion() {
        return osVersion;
    }

    @JsonProperty("cpuCores")
    public int cpuCores() {
        return cpuCores;
    }

    @JsonProperty("totalMemoryBytes")
    public long totalMemoryBytes() {
        return totalMemoryBytes;
    }

    @JsonProperty("flussVersion")
    public String flussVersion() {
        return flussVersion;
    }

    @JsonProperty("flussCommit")
    public String flussCommit() {
        return flussCommit;
    }
}
