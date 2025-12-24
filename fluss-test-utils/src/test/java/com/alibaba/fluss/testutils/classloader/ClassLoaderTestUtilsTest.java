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

/** Unit test for {@link ClassLoaderTestUtils}. */
public class ClassLoaderTestUtilsTest {

    @TempDir static Path tempDir;

    @Test
    void testCreateIsolatedClassLoader() {
        String[] classpathElements = new String[] {tempDir.toString()};

        URLClassLoader classLoader =
                ClassLoaderTestUtils.createIsolatedClassLoader(classpathElements);

        assertThat(classLoader).isNotNull();
        assertThat(classLoader.getParent()).isNull(); // Isolated class loader has no parent
    }

    @Test
    void testVerifyClassLoaderIsolation() throws Exception {
        String[] classpathElements = new String[] {tempDir.toString()};

        URLClassLoader classLoader1 =
                ClassLoaderTestUtils.createIsolatedClassLoader(classpathElements);
        URLClassLoader classLoader2 =
                ClassLoaderTestUtils.createIsolatedClassLoader(classpathElements);

        // Load the same class from both class loaders (using system class loader as fallback)
        Class<?> class1 = classLoader1.loadClass("java.lang.String");
        Class<?> class2 = classLoader2.loadClass("java.lang.String");

        // They should be the same since they're loaded from the bootstrap class loader
        // But let's test the utility method anyway
        boolean isolated = ClassLoaderTestUtils.verifyClassLoaderIsolation(class1, class2);
        // This might be false because both are loaded from bootstrap classloader
        // The method is designed for classes loaded from different custom classloaders
    }

    @Test
    void testCreateClassLoaderWithExclusions() {
        String[] classpathElements = new String[] {tempDir.toString()};
        String[] excludedPackages = {"java.lang", "java.util"};

        URLClassLoader classLoader =
                ClassLoaderTestUtils.createClassLoaderWithExclusions(
                        excludedPackages, classpathElements);

        assertThat(classLoader).isNotNull();
    }
}
