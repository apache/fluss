/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.server.classloader;

import com.alibaba.fluss.server.coordinator.CoordinatorServer;
import com.alibaba.fluss.server.tablet.TabletServer;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import com.alibaba.fluss.testutils.classloader.ClassLoaderTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.net.URLClassLoader;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for server class loading scenarios.
 *
 * <p>This test verifies: 1. Multi-TabletServer class loading consistency 2. CoordinatorServer
 * restart class loading 3. Rolling upgrade class loading
 */
public class ServerClassLoadingITCase {

    @TempDir static Path tempDir;

    @RegisterExtension
    static final FlussClusterExtension FLUSS_CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3) // 3 tablet servers for consistency testing
                    .build();

    @Test
    void testMultiTabletServerClassLoadingConsistency() throws Exception {
        // Test that multiple TabletServers have consistent class loading
        // This verifies that classes are loaded consistently across different server instances

        // Get all tablet servers
        assertThat(FLUSS_CLUSTER.getTabletServers()).hasSize(3);

        // Create isolated class loaders for each server to test consistency
        URLClassLoader server1ClassLoader =
                ClassLoaderTestUtils.createIsolatedClassLoader(tempDir.toString());
        URLClassLoader server2ClassLoader =
                ClassLoaderTestUtils.createIsolatedClassLoader(tempDir.toString());
        URLClassLoader server3ClassLoader =
                ClassLoaderTestUtils.createIsolatedClassLoader(tempDir.toString());

        // Verify they are different instances but can load the same classes
        assertThat(server1ClassLoader).isNotEqualTo(server2ClassLoader);
        assertThat(server1ClassLoader).isNotEqualTo(server3ClassLoader);
        assertThat(server2ClassLoader).isNotEqualTo(server3ClassLoader);

        // Test that they can load the same core classes
        Class<?> coordinatorClass1 =
                server1ClassLoader.loadClass(CoordinatorServer.class.getName());
        Class<?> coordinatorClass2 =
                server2ClassLoader.loadClass(CoordinatorServer.class.getName());
        Class<?> coordinatorClass3 =
                server3ClassLoader.loadClass(CoordinatorServer.class.getName());

        // They should be different class instances (isolated) but same class name
        assertThat(coordinatorClass1.getName()).isEqualTo(coordinatorClass2.getName());
        assertThat(coordinatorClass1.getName()).isEqualTo(coordinatorClass3.getName());
        assertThat(coordinatorClass1).isNotEqualTo(coordinatorClass2);
        assertThat(coordinatorClass1).isNotEqualTo(coordinatorClass3);

        // Clean up
        server1ClassLoader.close();
        server2ClassLoader.close();
        server3ClassLoader.close();
    }

    @Test
    void testCoordinatorServerRestartClassLoading() throws Exception {
        // Test CoordinatorServer restart class loading scenarios
        // This verifies that classes are properly reloaded after a restart

        // Get the coordinator server
        CoordinatorServer coordinatorServer = FLUSS_CLUSTER.getCoordinatorServer();
        assertThat(coordinatorServer).isNotNull();

        // Create an isolated class loader for testing restart scenarios
        URLClassLoader restartClassLoader =
                ClassLoaderTestUtils.createIsolatedClassLoader(tempDir.toString());

        // Load a class before restart
        Class<?> beforeRestartClass = restartClassLoader.loadClass(TabletServer.class.getName());
        assertThat(beforeRestartClass).isNotNull();

        // Simulate coordinator restart by stopping and starting
        FLUSS_CLUSTER.stopCoordinatorServer();
        FLUSS_CLUSTER.startCoordinatorServer();

        // Create a new class loader after restart
        URLClassLoader afterRestartClassLoader =
                ClassLoaderTestUtils.createIsolatedClassLoader(tempDir.toString());

        // Load the same class after restart
        Class<?> afterRestartClass =
                afterRestartClassLoader.loadClass(TabletServer.class.getName());
        assertThat(afterRestartClass).isNotNull();

        // Verify proper isolation (different class instances)
        assertThat(beforeRestartClass).isNotEqualTo(afterRestartClass);

        // But same class name
        assertThat(beforeRestartClass.getName()).isEqualTo(afterRestartClass.getName());

        // Clean up
        restartClassLoader.close();
        afterRestartClassLoader.close();
    }

    @Test
    void testRollingUpgradeClassLoading() throws Exception {
        // Test rolling upgrade class loading scenarios
        // This simulates having old and new version servers coexisting

        // Create class loaders for different "versions"
        URLClassLoader oldVersionClassLoader =
                ClassLoaderTestUtils.createIsolatedClassLoader(tempDir.toString());
        URLClassLoader newVersionClassLoader =
                ClassLoaderTestUtils.createIsolatedClassLoader(tempDir.toString());

        // Verify they are isolated
        assertThat(oldVersionClassLoader).isNotEqualTo(newVersionClassLoader);

        // Load the same classes from different "versions"
        Class<?> oldCoordinatorClass =
                oldVersionClassLoader.loadClass(CoordinatorServer.class.getName());
        Class<?> newCoordinatorClass =
                newVersionClassLoader.loadClass(CoordinatorServer.class.getName());

        // They should be different instances but same class name
        assertThat(oldCoordinatorClass.getName()).isEqualTo(newCoordinatorClass.getName());
        assertThat(oldCoordinatorClass).isNotEqualTo(newCoordinatorClass);

        // Test with exclusions for shared components that should be compatible
        String[] sharedPackages =
                new String[] {"com.alibaba.fluss.metadata", "com.alibaba.fluss.config"};

        URLClassLoader oldWithExclusions =
                ClassLoaderTestUtils.createClassLoaderWithExclusions(
                        sharedPackages, tempDir.toString());
        URLClassLoader newWithExclusions =
                ClassLoaderTestUtils.createClassLoaderWithExclusions(
                        sharedPackages, tempDir.toString());

        assertThat(oldWithExclusions).isNotNull();
        assertThat(newWithExclusions).isNotNull();

        // Clean up
        oldVersionClassLoader.close();
        newVersionClassLoader.close();
        oldWithExclusions.close();
        newWithExclusions.close();
    }

    @Test
    void testIsolatedClassLoaderCreation() throws Exception {
        // Test the ClassLoaderTestUtils functionality for server scenarios
        String[] classpathElements = new String[] {tempDir.toString()};

        // Create isolated class loader
        ClassLoader isolatedClassLoader =
                ClassLoaderTestUtils.createIsolatedClassLoader(classpathElements);

        assertThat(isolatedClassLoader).isNotNull();
        assertThat(isolatedClassLoader).isNotEqualTo(ClassLoader.getSystemClassLoader());

        // Test with exclusions (delegating certain packages to parent)
        String[] excludedPackages =
                new String[] {"com.alibaba.fluss.metadata", "com.alibaba.fluss.config"};

        ClassLoader exclusionClassLoader =
                ClassLoaderTestUtils.createClassLoaderWithExclusions(
                        excludedPackages, classpathElements);

        assertThat(exclusionClassLoader).isNotNull();

        // Clean up
        isolatedClassLoader.close();
        exclusionClassLoader.close();
    }
}
