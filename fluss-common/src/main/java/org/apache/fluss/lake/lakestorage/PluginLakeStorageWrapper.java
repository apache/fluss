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
import org.apache.fluss.utils.TemporaryClassLoaderContext;
import org.apache.fluss.utils.WrappingProxy;

import java.util.List;

/**
 * A wrapper around {@link LakeStoragePlugin} that ensures the plugin classloader is used for all
 * {@link LakeCatalog} operations.
 */
public class PluginLakeStorageWrapper implements LakeStoragePlugin {
    private final LakeStoragePlugin inner;
    private final ClassLoader loader;

    private PluginLakeStorageWrapper(final LakeStoragePlugin inner, final ClassLoader loader) {
        this.inner = inner;
        this.loader = loader;
    }

    public static PluginLakeStorageWrapper of(final LakeStoragePlugin inner) {
        return new PluginLakeStorageWrapper(inner, inner.getClass().getClassLoader());
    }

    @Override
    public ClassLoader getClassLoader() {
        return inner.getClassLoader();
    }

    @Override
    public String identifier() {
        return inner.identifier();
    }

    @Override
    public LakeStorage createLakeStorage(Configuration configuration) {
        try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(loader)) {
            return new ClassLoaderFixingLakeStorage(inner.createLakeStorage(configuration), loader);
        }
    }

    static class ClassLoaderFixingLakeCatalog implements LakeCatalog, WrappingProxy<LakeCatalog> {

        private final LakeCatalog inner;
        private final ClassLoader loader;

        private ClassLoaderFixingLakeCatalog(final LakeCatalog inner, final ClassLoader loader) {
            this.inner = inner;
            this.loader = loader;
        }

        @Override
        public void createTable(TablePath tablePath, TableDescriptor tableDescriptor)
                throws TableAlreadyExistException {
            try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(loader)) {
                inner.createTable(tablePath, tableDescriptor);
            }
        }

        @Override
        public void alterTable(TablePath tablePath, List<TableChange> tableChanges)
                throws TableNotExistException {
            try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(loader)) {
                inner.alterTable(tablePath, tableChanges);
            }
        }

        @Override
        public void close() throws Exception {
            try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(loader)) {
                inner.close();
            }
        }

        @Override
        public LakeCatalog getWrappedDelegate() {
            return inner;
        }
    }

    static class ClassLoaderFixingLakeStorage implements LakeStorage, WrappingProxy<LakeStorage> {

        private final LakeStorage inner;
        private final ClassLoader loader;

        private ClassLoaderFixingLakeStorage(final LakeStorage inner, final ClassLoader loader) {
            this.inner = inner;
            this.loader = loader;
        }

        @Override
        public LakeStorage getWrappedDelegate() {
            return inner;
        }

        @Override
        public LakeTieringFactory<?, ?> createLakeTieringFactory() {
            return inner.createLakeTieringFactory();
        }

        @Override
        public LakeCatalog createLakeCatalog() {
            try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(loader)) {
                return new ClassLoaderFixingLakeCatalog(inner.createLakeCatalog(), loader);
            }
        }

        @Override
        public LakeSource<?> createLakeSource(TablePath tablePath) {
            return inner.createLakeSource(tablePath);
        }
    }
}
