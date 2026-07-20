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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TabletServerResourceProbe}. */
class TabletServerResourceProbeTest {

    @TempDir private Path tempDir;

    @Test
    void testProbeFromExplicitConfig() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.TABLET_SERVER_ADVERTISED_RESOURCE_CPU_CORES, 8.0);
        conf.set(ConfigOptions.TABLET_SERVER_ADVERTISED_RESOURCE_MEMORY_SIZE, new MemorySize(1024));

        TabletServerResource resource = new TabletServerResourceProbe(conf, tempDir).probe();

        assertThat(resource.getCpuCores()).isEqualTo(8.0);
        assertThat(resource.getMemoryBytes()).isEqualTo(1024);
    }

    @Test
    void testProbeFromCgroupV2Files() throws Exception {
        write(tempDir.resolve("cpu.max"), "200000 100000");
        write(tempDir.resolve("memory.max"), "1024");

        TabletServerResource resource =
                new TabletServerResourceProbe(new Configuration(), tempDir).probe();

        assertThat(resource.getCpuCores()).isEqualTo(2.0);
        assertThat(resource.getMemoryBytes()).isEqualTo(1024);
    }

    @Test
    void testProbeFromCgroupV1Files() throws Exception {
        Path cpuDir = Files.createDirectory(tempDir.resolve("cpu"));
        Path memoryDir = Files.createDirectory(tempDir.resolve("memory"));
        write(cpuDir.resolve("cpu.cfs_quota_us"), "250000");
        write(cpuDir.resolve("cpu.cfs_period_us"), "100000");
        write(memoryDir.resolve("memory.limit_in_bytes"), "2048");

        TabletServerResource resource =
                new TabletServerResourceProbe(new Configuration(), tempDir).probe();

        assertThat(resource.getCpuCores()).isEqualTo(2.5);
        assertThat(resource.getMemoryBytes()).isEqualTo(2048);
    }

    @Test
    void testProbeFallsBackToCgroupV1WhenCgroupV2IsUnlimited() throws Exception {
        write(tempDir.resolve("cpu.max"), "max 100000");
        write(tempDir.resolve("memory.max"), "max");
        Path cpuDir = Files.createDirectory(tempDir.resolve("cpu"));
        Path memoryDir = Files.createDirectory(tempDir.resolve("memory"));
        write(cpuDir.resolve("cpu.cfs_quota_us"), "300000");
        write(cpuDir.resolve("cpu.cfs_period_us"), "100000");
        write(memoryDir.resolve("memory.limit_in_bytes"), "4096");

        TabletServerResource resource =
                new TabletServerResourceProbe(new Configuration(), tempDir).probe();

        assertThat(resource.getCpuCores()).isEqualTo(3.0);
        assertThat(resource.getMemoryBytes()).isEqualTo(4096);
    }

    @Test
    void testProbeFallsBackToCgroupWhenExplicitConfigIsNonPositive() throws Exception {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.TABLET_SERVER_ADVERTISED_RESOURCE_CPU_CORES, 0.0);
        conf.set(ConfigOptions.TABLET_SERVER_ADVERTISED_RESOURCE_MEMORY_SIZE, new MemorySize(0));
        write(tempDir.resolve("cpu.max"), "400000 100000");
        write(tempDir.resolve("memory.max"), "8192");

        TabletServerResource resource = new TabletServerResourceProbe(conf, tempDir).probe();

        assertThat(resource.getCpuCores()).isEqualTo(4.0);
        assertThat(resource.getMemoryBytes()).isEqualTo(8192);
    }

    @Test
    void testProbeFallsBackToJavaRuntimeAndOperatingSystem() {
        TabletServerResource resource =
                new TabletServerResourceProbe(new Configuration(), tempDir).probe();

        assertThat(resource.getCpuCores()).isNotNull().isGreaterThan(0.0);

        java.lang.management.OperatingSystemMXBean operatingSystemMXBean =
                java.lang.management.ManagementFactory.getOperatingSystemMXBean();
        if (operatingSystemMXBean instanceof com.sun.management.OperatingSystemMXBean) {
            long expectedMemoryBytes =
                    ((com.sun.management.OperatingSystemMXBean) operatingSystemMXBean)
                            .getTotalPhysicalMemorySize();
            if (expectedMemoryBytes > 0) {
                assertThat(resource.getMemoryBytes()).isEqualTo(expectedMemoryBytes);
            } else {
                assertThat(resource.getMemoryBytes()).isNull();
            }
        } else {
            assertThat(resource.getMemoryBytes()).isNull();
        }
    }

    private static void write(Path path, String value) throws Exception {
        Files.write(path, Collections.singletonList(value), StandardCharsets.UTF_8);
    }
}
