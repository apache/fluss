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

package com.alibaba.fluss.testutils.classloader;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URLClassLoader;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for concurrent class loading scenarios.
 *
 * <p>This test verifies: 1. Multiple threads loading plugins simultaneously 2. Concurrent
 * ClassLoader initialization 3. Race condition detection
 */
public class ConcurrentClassLoaderITCase {

    @TempDir static Path tempDir;

    @Test
    void testMultipleThreadsLoadingPluginsSimultaneously() throws InterruptedException {
        // Test multiple threads loading plugins simultaneously
        URLClassLoader[] classLoaders = new URLClassLoader[5];
        for (int i = 0; i < classLoaders.length; i++) {
            classLoaders[i] = ClassLoaderTestUtils.createIsolatedClassLoader(tempDir.toString());
        }

        // Test concurrent class loading
        Class<?>[] loadedClasses =
                ClassLoaderTestUtils.testConcurrentClassLoading("java.lang.String", classLoaders);

        // Verify all classes were loaded
        assertThat(loadedClasses).hasSize(classLoaders.length);
        for (Class<?> loadedClass : loadedClasses) {
            assertThat(loadedClass).isNotNull();
            assertThat(loadedClass.getName()).isEqualTo("java.lang.String");
        }

        // Clean up
        for (URLClassLoader classLoader : classLoaders) {
            ClassLoaderTestUtils.verifyClassLoaderCleanup(classLoader);
        }
    }

    @Test
    void testConcurrentClassLoaderInitialization() throws InterruptedException {
        // Test concurrent ClassLoader initialization
        URLClassLoader[] classLoaders = new URLClassLoader[3];

        // Create class loaders concurrently
        Thread[] threads = new Thread[classLoaders.length];
        Exception[] exceptions = new Exception[classLoaders.length];

        for (int i = 0; i < classLoaders.length; i++) {
            final int index = i;
            threads[i] =
                    new Thread(
                            () -> {
                                try {
                                    classLoaders[index] =
                                            ClassLoaderTestUtils.createIsolatedClassLoader(
                                                    tempDir.toString());
                                } catch (Exception e) {
                                    exceptions[index] = e;
                                }
                            });
            threads[i].start();
        }

        // Wait for all threads to complete
        for (Thread thread : threads) {
            thread.join();
        }

        // Check for exceptions
        for (Exception exception : exceptions) {
            assertThat(exception).isNull();
        }

        // Verify all class loaders were created
        for (URLClassLoader classLoader : classLoaders) {
            assertThat(classLoader).isNotNull();
        }

        // Clean up
        for (URLClassLoader classLoader : classLoaders) {
            ClassLoaderTestUtils.verifyClassLoaderCleanup(classLoader);
        }
    }

    @Test
    void testRaceConditionDetection() throws InterruptedException {
        // Test race condition detection in class loading
        URLClassLoader classLoader1 =
                ClassLoaderTestUtils.createIsolatedClassLoader(tempDir.toString());
        URLClassLoader classLoader2 =
                ClassLoaderTestUtils.createIsolatedClassLoader(tempDir.toString());

        // Load classes concurrently to test for race conditions
        Class<?>[] loadedClasses =
                ClassLoaderTestUtils.testConcurrentClassLoading(
                        "java.util.List", classLoader1, classLoader2);

        // Verify classes were loaded correctly
        assertThat(loadedClasses).hasSize(2);
        assertThat(loadedClasses[0]).isEqualTo(loadedClasses[1]);
        assertThat(loadedClasses[0].getName()).isEqualTo("java.util.List");

        // Clean up
        ClassLoaderTestUtils.verifyClassLoaderCleanup(classLoader1);
        ClassLoaderTestUtils.verifyClassLoaderCleanup(classLoader2);
    }
}
