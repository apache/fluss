/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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

package org.apache.fluss.tools.ci;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.jar.JarFile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Opt-in JDK 11+ check: pass packaged connector paths in -Dfluss.sketch.jars. Direct memory
 * requires the JVM access flags documented by DataSketches Memory.
 */
class SketchJarCompatibilityTest {
    @Test
    void testPackagedSketchJars() throws Exception {
        String paths = System.getProperty("fluss.sketch.jars");
        assumeTrue(paths != null && !paths.isEmpty());
        assumeTrue(!"1.8".equals(System.getProperty("java.specification.version")));
        for (String path : paths.split(File.pathSeparator)) {
            File file = new File(path);
            try (JarFile jar = new JarFile(file)) {
                assertThat(jar.getManifest().getMainAttributes().getValue("Multi-Release"))
                        .as("Multi-Release manifest in %s", file)
                        .isEqualToIgnoringCase("true");
            }
            // Keep application-classpath dependencies out of this packaged-artifact check.
            try (URLClassLoader loader =
                    new URLClassLoader(
                            new URL[] {file.toURI().toURL()},
                            ClassLoader.getSystemClassLoader().getParent())) {
                String prefix = "org.apache.fluss.shaded.org.apache.datasketches.memory.";
                Class<?> cleaner = loader.loadClass(prefix + "internal.MemoryCleaner");
                assertThat(cleaner.getResource("MemoryCleaner.class").toString())
                        .contains("META-INF/versions/11/");
                for (String name :
                        new String[] {
                            "com.fasterxml.jackson.core.io.doubleparser.BigSignificand",
                            "org.yaml.snakeyaml.internal.Logger"
                        }) {
                    assertThatThrownBy(() -> loader.loadClass(name))
                            .isInstanceOf(ClassNotFoundException.class);
                    assertThat(
                                    loader.loadClass("org.apache.fluss.shaded.jackson2." + name)
                                            .getName())
                            .isEqualTo("org.apache.fluss.shaded.jackson2." + name);
                }
                Class<?> writable = loader.loadClass(prefix + "WritableMemory");
                Object memory = writable.getMethod("allocateDirect", long.class).invoke(null, 64L);
                try (AutoCloseable resource = (AutoCloseable) memory) {
                    writable.getMethod("putLong", long.class, long.class).invoke(memory, 0L, 42L);
                    assertThat(writable.getMethod("getLong", long.class).invoke(memory, 0L))
                            .isEqualTo(42L);
                }
                assertThat(writable.getMethod("isAlive").invoke(memory)).isEqualTo(false);
                System.out.println(
                        file.getName()
                                + ": "
                                + cleaner.getResource("MemoryCleaner.class")
                                + "; direct-memory and relocated-class checks passed");
            }
        }
    }
}
