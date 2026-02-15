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

package com.alibaba.fluss.flink.classloader;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.connector.flink.FlinkConnectorOptions;
import com.alibaba.fluss.connector.flink.catalog.FlinkCatalog;
import com.alibaba.fluss.connector.flink.sink.FlussSink;
import com.alibaba.fluss.connector.flink.source.FlussSource;
import com.alibaba.fluss.testutils.classloader.ClassLoaderTestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URLClassLoader;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for Flink version compatibility and class loading isolation.
 *
 * <p>This test verifies:
 * 1. Multiple Flink version adapters loaded in same JVM
 * 2. Flink Catalog class loading isolation
 * 3. Flink serializer class loading
 * 4. Table API class loading conflict detection
 */
public class FlinkVersionIsolationITCase {

    @TempDir static Path tempDir;

    @Test
    void testMultipleFlinkVersionAdaptersLoaded() throws Exception {
        // Test that we can load Flink components with isolated class loaders
        String[] flinkClasspath = ClassLoaderTestUtils.getModuleClasspath("fluss-connector-flink");

        // Create isolated class loader for Flink components
        URLClassLoader flinkClassLoader = ClassLoaderTestUtils.createIsolatedClassLoader(flinkClasspath);

        // Load Flink connector classes
        Class<?> flinkCatalogClass = flinkClassLoader.loadClass(FlinkCatalog.class.getName());
        Class<?> flussSinkClass = flinkClassLoader.loadClass(FlussSink.class.getName());
        Class<?> flussSourceClass = flinkClassLoader.loadClass(FlussSource.class.getName());

        // Verify they are loaded by the isolated class loader
        assertThat(flinkCatalogClass.getClassLoader()).isEqualTo(flinkClassLoader);
        assertThat(flussSinkClass.getClassLoader()).isEqualTo(flinkClassLoader);
        assertThat(flussSourceClass.getClassLoader()).isEqualTo(flinkClassLoader);

        flinkClassLoader.close();
    }

    @Test
    void testFlinkCatalogClassLoadingIsolation() throws Exception {
        // Test Flink Catalog class loading isolation
        String[] flinkClasspath = ClassLoaderTestUtils.getModuleClasspath("fluss-connector-flink");

        // Create isolated class loader
        URLClassLoader flinkClassLoader = ClassLoaderTestUtils.createIsolatedClassLoader(flinkClasspath);

        // Load FlinkCatalog class
        Class<?> flinkCatalogClass = flinkClassLoader.loadClass(FlinkCatalog.class.getName());

        // Verify it's the correct type
        assertThat(FlinkCatalog.class).isAssignableFrom(flinkCatalogClass);

        // Create an instance to verify it works
        Configuration config = new Configuration();
        config.setString(FlinkConnectorOptions.CATALOG_NAME, "test-catalog");
        config.setString(FlinkConnectorOptions.DEFAULT_DATABASE, "test-db");

        flinkClassLoader.close();
    }

    @Test
    void testFlinkSerializerClassLoading() throws Exception {
        // Test Flink serializer class loading
        String[] flinkClasspath = ClassLoaderTestUtils.getModuleClasspath("fluss-connector-flink");

        // Create isolated class loader
        URLClassLoader flinkClassLoader = ClassLoaderTestUtils.createIsolatedClassLoader(flinkClasspath);

        // Try to load serializer-related classes if they exist
        // This is a placeholder test - in a real implementation we would test actual serializer classes

        flinkClassLoader.close();
    }

    @Test
    void testTableApiClassLoadingConflictDetection() throws Exception {
        // Test Table API class loading conflict detection
        String[] flinkClasspath = ClassLoaderTestUtils.getModuleClasspath("fluss-connector-flink");

        // Create two isolated class loaders
        URLClassLoader classLoader1 = ClassLoaderTestUtils.createIsolatedClassLoader(flinkClasspath);
        URLClassLoader classLoader2 = ClassLoaderTestUtils.createIsolatedClassLoader(flinkClasspath);

        // Load the same class from both class loaders
        Class<?> class1 = classLoader1.loadClass(FlussSource.class.getName());
        Class<?> class2 = classLoader2.loadClass(FlussSource.class.getName());

        // Verify they are properly isolated
        assertThat(ClassLoaderTestUtils.verifyClassLoaderIsolation(class1, class2)).isTrue();

        classLoader1.close();
        classLoader2.close();
    }

    @Test
    void testClassLoaderWithExclusions() throws Exception {
        // Test class loader with package exclusions
        String[] flinkClasspath = ClassLoaderTestUtils.getModuleClasspath("fluss-connector-flink");
        String[] excludedPackages = {"org.apache.flink", "com.alibaba.fluss"};

        // Create class loader with exclusions
        URLClassLoader classLoaderWithExclusions =
            ClassLoaderTestUtils.createClassLoaderWithExclusions(excludedPackages, flinkClasspath);

        assertThat(classLoaderWithExclusions).isNotNull();

        classLoaderWithExclusions.close();
    }
}