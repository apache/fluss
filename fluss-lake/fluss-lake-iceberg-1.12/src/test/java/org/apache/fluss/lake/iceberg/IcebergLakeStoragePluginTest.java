/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.iceberg;

import org.apache.fluss.config.Configuration;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for the Iceberg 1.12 {@link IcebergLakeStoragePlugin}. */
class IcebergLakeStoragePluginTest {

    private static final String JAVA_VERSION_PROPERTY = "java.specification.version";
    private static final String FORMAT_MODEL_REGISTRY =
            "org.apache.iceberg.formats.FormatModelRegistry";
    private static final String GENERIC_RECORD = "org.apache.iceberg.data.Record";

    @Test
    void testRejectsJavaOlderThan17() {
        String javaVersion = System.getProperty(JAVA_VERSION_PROPERTY);
        System.setProperty(JAVA_VERSION_PROPERTY, "11");
        try {
            assertThatThrownBy(
                            () ->
                                    new IcebergLakeStoragePlugin()
                                            .createLakeStorage(new Configuration()))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("requires Java 17 or later");
        } finally {
            System.setProperty(JAVA_VERSION_PROPERTY, javaVersion);
        }
    }

    @Test
    void testRegistersFormatModelsWithPluginClassLoader() throws Exception {
        // a context classloader that cannot see Iceberg, like the one of a plugin caller
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(new URLClassLoader(new URL[0], null));
        try {
            // without the plugin, the registry finds no generic format models
            try (URLClassLoader pluginClassLoader = isolatedClassLoader()) {
                assertThat(registersGenericRecords(pluginClassLoader)).isFalse();
            }

            try (URLClassLoader pluginClassLoader = isolatedClassLoader()) {
                Object plugin =
                        pluginClassLoader
                                .loadClass(IcebergLakeStoragePlugin.class.getName())
                                .getDeclaredConstructor()
                                .newInstance();
                Class<?> configurationClass =
                        pluginClassLoader.loadClass(Configuration.class.getName());
                plugin.getClass()
                        .getMethod("createLakeStorage", configurationClass)
                        .invoke(plugin, configurationClass.getDeclaredConstructor().newInstance());

                assertThat(registersGenericRecords(pluginClassLoader)).isTrue();
            }
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    /** Initializes the registry of the classloader if needed and checks its generic models. */
    private static boolean registersGenericRecords(ClassLoader classLoader) throws Exception {
        Method models =
                Class.forName(FORMAT_MODEL_REGISTRY, true, classLoader).getDeclaredMethod("models");
        models.setAccessible(true);
        for (Object key : ((Map<?, ?>) models.invoke(null)).keySet()) {
            Object type = key.getClass().getMethod("second").invoke(key);
            if (((Class<?>) type).getName().equals(GENERIC_RECORD)) {
                return true;
            }
        }
        return false;
    }

    /** Loads the test classpath again, so that the Iceberg classes are initialized again. */
    private static URLClassLoader isolatedClassLoader() throws MalformedURLException {
        List<URL> urls = new ArrayList<>();
        for (String path : System.getProperty("java.class.path").split(File.pathSeparator)) {
            urls.add(new File(path).toURI().toURL());
        }
        return new URLClassLoader(urls.toArray(new URL[0]), ClassLoader.getPlatformClassLoader());
    }
}
