/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.fs;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.fs.token.ObtainedSecurityToken;
import com.alibaba.fluss.utils.TemporaryClassLoaderContext;
import com.alibaba.fluss.utils.WrappingProxy;

import java.io.IOException;
import java.net.URI;

/**
 * A wrapper around {@link FileSystemPlugin} that ensures the plugin classloader is used for all
 * {@link FileSystem} operations.
 */
public class PluginFileSystemWrapper implements FileSystemPlugin {

    private final FileSystemPlugin inner;
    private final ClassLoader loader;

    private PluginFileSystemWrapper(final FileSystemPlugin inner, final ClassLoader loader) {
        this.inner = inner;
        this.loader = loader;
    }

    public static PluginFileSystemWrapper of(final FileSystemPlugin inner) {
        return new PluginFileSystemWrapper(inner, inner.getClass().getClassLoader());
    }

    @Override
    public String getScheme() {
        return inner.getScheme();
    }

    @Override
    public ClassLoader getClassLoader() {
        return inner.getClassLoader();
    }

    @Override
    public FileSystem create(final URI fsUri, Configuration configuration) throws IOException {
        try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(loader)) {
            return new ClassLoaderFixingFileSystem(inner.create(fsUri, configuration), loader);
        }
    }

    static class ClassLoaderFixingFileSystem extends FileSystem
            implements WrappingProxy<FileSystem> {
        private final FileSystem inner;
        private final ClassLoader loader;

        private ClassLoaderFixingFileSystem(final FileSystem inner, final ClassLoader loader) {
            this.inner = inner;
            this.loader = loader;
        }

        @Override
        public ObtainedSecurityToken obtainSecurityToken() throws IOException {
            return inner.obtainSecurityToken();
        }

        @Override
        public URI getUri() {
            try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(loader)) {
                return inner.getUri();
            }
        }

        @Override
        public FileStatus getFileStatus(final FsPath f) throws IOException {
            try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(loader)) {
                return inner.getFileStatus(f);
            }
        }

        @Override
        public FSDataInputStream open(final FsPath f) throws IOException {
            try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(loader)) {
                return inner.open(f);
            }
        }

        @Override
        public FileStatus[] listStatus(final FsPath f) throws IOException {
            try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(loader)) {
                return inner.listStatus(f);
            }
        }

        @Override
        public boolean exists(final FsPath f) throws IOException {
            try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(loader)) {
                return inner.exists(f);
            }
        }

        @Override
        public boolean delete(final FsPath f, final boolean recursive) throws IOException {
            try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(loader)) {
                return inner.delete(f, recursive);
            }
        }

        @Override
        public boolean mkdirs(final FsPath f) throws IOException {
            try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(loader)) {
                return inner.mkdirs(f);
            }
        }

        @Override
        public FSDataOutputStream create(final FsPath f, final WriteMode overwriteMode)
                throws IOException {
            try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(loader)) {
                return inner.create(f, overwriteMode);
            }
        }

        @Override
        public boolean rename(final FsPath src, final FsPath dst) throws IOException {
            try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(loader)) {
                return inner.rename(src, dst);
            }
        }

        @Override
        public FileSystem getWrappedDelegate() {
            return inner;
        }
    }
}
