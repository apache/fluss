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
 * Integration test for resource cleanup scenarios.
 *
 * <p>This test verifies: 1. ClassLoader unload resource release 2. Plugin unload class reference
 * cleanup
 */
public class ResourceCleanupITCase {

    @TempDir static Path tempDir;

    @Test
    void testClassLoaderUnloadResourceRelease() {
        // Test that ClassLoader properly releases resources when unloaded
        URLClassLoader classLoader =
                ClassLoaderTestUtils.createIsolatedClassLoader(tempDir.toString());

        // Load some classes
        try {
            Class<?> stringClass = classLoader.loadClass("java.lang.String");
            assertThat(stringClass).isNotNull();
        } catch (ClassNotFoundException e) {
            // This shouldn't happen for java.lang.String
            throw new RuntimeException(e);
        }

        // Verify cleanup
        boolean cleanupSuccessful = ClassLoaderTestUtils.verifyClassLoaderCleanup(classLoader);
        assertThat(cleanupSuccessful).isTrue();
    }

    @Test
    void testPluginUnloadClassReferenceCleanup() {
        // Test that plugin unload properly cleans up class references
        URLClassLoader pluginClassLoader1 =
                ClassLoaderTestUtils.createIsolatedClassLoader(tempDir.toString());
        URLClassLoader pluginClassLoader2 =
                ClassLoaderTestUtils.createIsolatedClassLoader(tempDir.toString());

        // Load classes from both class loaders
        try {
            Class<?> class1 = pluginClassLoader1.loadClass("java.lang.Object");
            Class<?> class2 = pluginClassLoader2.loadClass("java.lang.Object");

            // Verify they are different instances
            assertThat(class1).isNotEqualTo(class2);
            assertThat(class1.getName()).isEqualTo(class2.getName());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        // Clean up both class loaders
        boolean cleanup1 = ClassLoaderTestUtils.verifyClassLoaderCleanup(pluginClassLoader1);
        boolean cleanup2 = ClassLoaderTestUtils.verifyClassLoaderCleanup(pluginClassLoader2);

        assertThat(cleanup1).isTrue();
        assertThat(cleanup2).isTrue();
    }
}
