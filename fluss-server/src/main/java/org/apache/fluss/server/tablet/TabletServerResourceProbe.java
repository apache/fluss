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

package org.apache.fluss.server.tablet;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.server.metadata.TabletServerResource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

/** Probes CPU and memory resource information for tablet server registration. */
public class TabletServerResourceProbe {

    private static final Logger LOG = LoggerFactory.getLogger(TabletServerResourceProbe.class);

    private static final long UNLIMITED_MEMORY_THRESHOLD = Long.MAX_VALUE / 2;

    private final Configuration conf;
    private final Path cgroupRoot;

    public TabletServerResourceProbe(Configuration conf) {
        this(conf, Paths.get("/sys/fs/cgroup"));
    }

    TabletServerResourceProbe(Configuration conf, Path cgroupRoot) {
        this.conf = conf;
        this.cgroupRoot = cgroupRoot;
    }

    public TabletServerResource probe() {
        return new TabletServerResource(
                probeCpuCores().orElse(null), probeMemoryBytes().orElse(null));
    }

    private Optional<Double> probeCpuCores() {
        Optional<Double> configuredCpuCores =
                conf.getOptional(ConfigOptions.TABLET_SERVER_ADVERTISED_RESOURCE_CPU_CORES);
        if (configuredCpuCores.isPresent()) {
            Optional<Double> validConfiguredCpuCores = positiveDouble(configuredCpuCores.get());
            if (validConfiguredCpuCores.isPresent()) {
                return validConfiguredCpuCores;
            }
        }

        Optional<Double> cgroupCpuCores = probeCgroupCpuCores();
        if (cgroupCpuCores.isPresent()) {
            return cgroupCpuCores;
        }

        return positiveDouble(Runtime.getRuntime().availableProcessors());
    }

    private Optional<Long> probeMemoryBytes() {
        Optional<MemorySize> configuredMemory =
                conf.getOptional(ConfigOptions.TABLET_SERVER_ADVERTISED_RESOURCE_MEMORY_SIZE);
        if (configuredMemory.isPresent()) {
            Optional<Long> validConfiguredMemory = positiveLong(configuredMemory.get().getBytes());
            if (validConfiguredMemory.isPresent()) {
                return validConfiguredMemory;
            }
        }

        Optional<Long> cgroupMemory = probeCgroupMemoryBytes();
        if (cgroupMemory.isPresent()) {
            return cgroupMemory;
        }

        return probeOperatingSystemMemoryBytes();
    }

    private Optional<Long> probeCgroupMemoryBytes() {
        Optional<Long> cgroupV2Memory = readMemoryValue(cgroupRoot.resolve("memory.max"));
        if (cgroupV2Memory.isPresent()) {
            return cgroupV2Memory;
        }

        return readMemoryValue(cgroupRoot.resolve("memory").resolve("memory.limit_in_bytes"));
    }

    private Optional<Long> readMemoryValue(Path path) {
        Optional<String> value = readFirstLine(path);
        if (!value.isPresent()) {
            return Optional.empty();
        }

        String trimmedValue = value.get().trim();
        if ("max".equals(trimmedValue)) {
            return Optional.empty();
        }

        try {
            long memoryBytes = Long.parseLong(trimmedValue);
            if (memoryBytes <= 0 || memoryBytes >= UNLIMITED_MEMORY_THRESHOLD) {
                return Optional.empty();
            }
            return Optional.of(memoryBytes);
        } catch (NumberFormatException e) {
            LOG.debug("Failed to parse memory value from {}: {}", path, trimmedValue, e);
            return Optional.empty();
        }
    }

    private Optional<Double> probeCgroupCpuCores() {
        Optional<String> cpuMax = readFirstLine(cgroupRoot.resolve("cpu.max"));
        if (cpuMax.isPresent()) {
            String[] parts = cpuMax.get().trim().split("\\s+");
            if (parts.length >= 2 && !"max".equals(parts[0])) {
                Optional<Long> quota = parseLong(parts[0]);
                Optional<Long> period = parseLong(parts[1]);
                if (quota.isPresent()
                        && period.isPresent()
                        && quota.get() > 0
                        && period.get() > 0) {
                    return positiveDouble(((double) quota.get()) / period.get());
                }
            }
        }

        Optional<Long> quota = readLong(cgroupRoot.resolve("cpu").resolve("cpu.cfs_quota_us"));
        Optional<Long> period = readLong(cgroupRoot.resolve("cpu").resolve("cpu.cfs_period_us"));
        if (quota.isPresent() && period.isPresent() && quota.get() > 0 && period.get() > 0) {
            return positiveDouble(((double) quota.get()) / period.get());
        }
        return Optional.empty();
    }

    private Optional<Long> probeOperatingSystemMemoryBytes() {
        java.lang.management.OperatingSystemMXBean operatingSystemMXBean =
                ManagementFactory.getOperatingSystemMXBean();
        if (operatingSystemMXBean instanceof com.sun.management.OperatingSystemMXBean) {
            long memoryBytes =
                    ((com.sun.management.OperatingSystemMXBean) operatingSystemMXBean)
                            .getTotalPhysicalMemorySize();
            return positiveLong(memoryBytes);
        }
        return Optional.empty();
    }

    private Optional<Long> readLong(Path path) {
        Optional<String> value = readFirstLine(path);
        return value.isPresent() ? parseLong(value.get().trim()) : Optional.empty();
    }

    private Optional<String> readFirstLine(Path path) {
        if (!Files.isRegularFile(path)) {
            return Optional.empty();
        }

        try {
            byte[] bytes = Files.readAllBytes(path);
            String content = new String(bytes, StandardCharsets.UTF_8).trim();
            return content.isEmpty() ? Optional.empty() : Optional.of(content);
        } catch (IOException e) {
            LOG.debug("Failed to read resource file {}", path, e);
            return Optional.empty();
        }
    }

    private Optional<Long> parseLong(String value) {
        try {
            return Optional.of(Long.parseLong(value));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    private Optional<Long> positiveLong(long value) {
        return value > 0 ? Optional.of(value) : Optional.empty();
    }

    private Optional<Double> positiveDouble(double value) {
        return value > 0 ? Optional.of(value) : Optional.empty();
    }
}
