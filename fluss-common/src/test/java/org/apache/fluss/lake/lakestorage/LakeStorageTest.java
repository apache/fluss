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

package org.apache.fluss.lake.lakestorage;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.TableAlreadyExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.plugin.PluginManager;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link LakeStorage} base class. */
class LakeStorageTest {
    private static final String TEST_LAKE_PLUGIN_FORMAT = "test-plugin";

    @Test
    void testInvalidPlugin() throws Exception {
        final Map<Class<?>, Iterator<?>> lakeStoragePlugins = new HashMap<>();
        lakeStoragePlugins.put(
                LakeStoragePlugin.class,
                Collections.singletonList(new LakeStorageTest.TestPluginLakeStoragePlugin())
                        .iterator());

        assertThatThrownBy(
                        () ->
                                LakeStoragePluginSetUp.fromDataLakeFormat(
                                        TEST_LAKE_PLUGIN_FORMAT + "1",
                                        new TestingPluginManager(lakeStoragePlugins)))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("No LakeStoragePlugin can be found for datalake format: test-plugin1");
    }

    @Test
    void testWithPluginManager() throws Exception {
        final Map<Class<?>, Iterator<?>> lakeStoragePlugins = new HashMap<>();

        lakeStoragePlugins.put(
                LakeStoragePlugin.class,
                Collections.singletonList(new LakeStorageTest.TestPluginLakeStoragePlugin())
                        .iterator());

        LakeStoragePlugin lakeStoragePlugin =
                LakeStoragePluginSetUp.fromDataLakeFormat(
                        TEST_LAKE_PLUGIN_FORMAT, new TestingPluginManager(lakeStoragePlugins));

        assertThat(lakeStoragePlugin).isInstanceOf(PluginLakeStorageWrapper.class);
        LakeStorage lakeStorage = lakeStoragePlugin.createLakeStorage(new Configuration());

        // the LakeStorage should wrap TestPaimonLakeStorage
        assertThat(lakeStorage)
                .isInstanceOf(PluginLakeStorageWrapper.ClassLoaderFixingLakeStorage.class);
        assertThat(
                        ((PluginLakeStorageWrapper.ClassLoaderFixingLakeStorage) lakeStorage)
                                .getWrappedDelegate())
                .isInstanceOf(TestPaimonLakeStorage.class);

        // the LakeCatalog should wrap TestPaimonLakeCatalog
        LakeCatalog lakeCatalog = lakeStorage.createLakeCatalog();
        assertThat(lakeCatalog)
                .isInstanceOf(PluginLakeStorageWrapper.ClassLoaderFixingLakeCatalog.class);
        assertThat(
                        ((PluginLakeStorageWrapper.ClassLoaderFixingLakeCatalog) lakeCatalog)
                                .getWrappedDelegate())
                .isInstanceOf(TestPaimonLakeCatalog.class);
    }

    private static class TestingPluginManager implements PluginManager {

        private final Map<Class<?>, Iterator<?>> plugins;

        private TestingPluginManager(Map<Class<?>, Iterator<?>> plugins) {
            this.plugins = plugins;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <P> Iterator<P> load(Class<P> service) {
            return (Iterator<P>) plugins.get(service);
        }
    }

    private static class TestPluginLakeStoragePlugin implements LakeStoragePlugin {

        private static final String IDENTIFIER = TEST_LAKE_PLUGIN_FORMAT;

        @Override
        public String identifier() {
            return IDENTIFIER;
        }

        @Override
        public LakeStorage createLakeStorage(Configuration configuration) {
            return new TestPaimonLakeStorage();
        }
    }

    private static class TestPaimonLakeStorage implements LakeStorage {

        public TestPaimonLakeStorage() {}

        @Override
        public LakeTieringFactory<?, ?> createLakeTieringFactory() {
            return null;
        }

        @Override
        public TestPaimonLakeCatalog createLakeCatalog() {
            return new TestPaimonLakeCatalog();
        }

        @Override
        public LakeSource<?> createLakeSource(TablePath tablePath) {
            throw new UnsupportedOperationException("Not implemented");
        }
    }

    private static class TestPaimonLakeCatalog implements LakeCatalog {

        @Override
        public void createTable(TablePath tablePath, TableDescriptor tableDescriptor)
                throws TableAlreadyExistException {}

        @Override
        public void alterTable(TablePath tablePath, List<TableChange> tableChanges)
                throws TableNotExistException {}
    }
}
