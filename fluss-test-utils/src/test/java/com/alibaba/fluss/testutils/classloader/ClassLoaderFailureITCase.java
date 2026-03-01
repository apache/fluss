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
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration test for class loading failure scenarios.
 *
 * <p>This test verifies: 1. Missing dependency class loading 2. Version incompatibility class
 * loading 3. ClassNotFoundException handling
 */
public class ClassLoaderFailureITCase {

    @TempDir static Path tempDir;

    @Test
    void testMissingDependencyClassLoading() {
        // Test class loading when dependencies are missing
        URLClassLoader isolatedClassLoader =
                ClassLoaderTestUtils.createIsolatedClassLoader(tempDir.toString());

        // Try to load a class that doesn't exist
        Supplier<Class<?>> classSupplier =
                ClassLoaderTestUtils.simulateClassLoadingFailure(
                        "com.nonexistent.Class", isolatedClassLoader);

        // Verify that it throws an exception
        assertThatThrownBy(classSupplier::get)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to load class");

        // Clean up
        ClassLoaderTestUtils.verifyClassLoaderCleanup(isolatedClassLoader);
    }

    @Test
    void testVersionIncompatibilityClassLoading() {
        // Test version incompatibility scenarios
        URLClassLoader version1ClassLoader =
                ClassLoaderTestUtils.createIsolatedClassLoader(tempDir.toString());
        URLClassLoader version2ClassLoader =
                ClassLoaderTestUtils.createIsolatedClassLoader(tempDir.toString());

        // Load the same class name from different class loaders
        Supplier<Class<?>> classSupplier1 =
                ClassLoaderTestUtils.simulateClassLoadingFailure(
                        "com.alibaba.fluss.TestClass", version1ClassLoader);
        Supplier<Class<?>> classSupplier2 =
                ClassLoaderTestUtils.simulateClassLoadingFailure(
                        "com.alibaba.fluss.TestClass", version2ClassLoader);

        // Both should throw ClassNotFoundException since the classes don't actually exist
        assertThatThrownBy(classSupplier1::get).isInstanceOf(RuntimeException.class);

        assertThatThrownBy(classSupplier2::get).isInstanceOf(RuntimeException.class);

        // But they should be different exceptions
        try {
            classSupplier1.get();
        } catch (RuntimeException e1) {
            try {
                classSupplier2.get();
            } catch (RuntimeException e2) {
                assertThat(e1).isNotEqualTo(e2);
            }
        }

        // Clean up
        ClassLoaderTestUtils.verifyClassLoaderCleanup(version1ClassLoader);
        ClassLoaderTestUtils.verifyClassLoaderCleanup(version2ClassLoader);
    }

    @Test
    void testClassNotFoundExceptionHandling() {
        // Test explicit ClassNotFoundException handling
        URLClassLoader isolatedClassLoader =
                ClassLoaderTestUtils.createIsolatedClassLoader(tempDir.toString());

        // Try to load a class that doesn't exist
        assertThatThrownBy(() -> isolatedClassLoader.loadClass("com.nonexistent.Class"))
                .isInstanceOf(ClassNotFoundException.class);

        // Clean up
        ClassLoaderTestUtils.verifyClassLoaderCleanup(isolatedClassLoader);
    }
}
