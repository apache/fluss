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

package com.alibaba.fluss.fs;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.fs.local.LocalFileSystem;
import com.alibaba.fluss.plugin.PluginManager;
import com.alibaba.fluss.utils.WrappingProxy;
import com.alibaba.fluss.utils.WrappingProxyUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link FileSystem} base class. */
class FileSystemTest {

    @Test
    void testGet() throws URISyntaxException, IOException {
        String scheme = "file";

        assertThat(getFileSystemWithoutSafetyNet(scheme + ":///test/test"))
                .isInstanceOf(LocalFileSystem.class);

        try {
            getFileSystemWithoutSafetyNet(scheme + "://test/test");
        } catch (IOException ioe) {
            assertThat(ioe.getMessage()).startsWith("Found local file path with authority '");
        }

        assertThat(getFileSystemWithoutSafetyNet(scheme + ":/test/test"))
                .isInstanceOf(LocalFileSystem.class);

        assertThat(getFileSystemWithoutSafetyNet(scheme + ":test/test"))
                .isInstanceOf(LocalFileSystem.class);

        assertThat(getFileSystemWithoutSafetyNet("/test/test")).isInstanceOf(LocalFileSystem.class);

        assertThat(getFileSystemWithoutSafetyNet("test/test")).isInstanceOf(LocalFileSystem.class);
    }

    @Test
    void testWithPluginManager(@TempDir Path tempDir) throws Exception {
        String testPluginSchema = "test-plugin";
        final Map<Class<?>, Iterator<?>> fileSystemPlugins = new HashMap<>();
        // use LocalFileSystem as the filesystem of schema 'testPluginSchema'
        fileSystemPlugins.put(
                FileSystemPlugin.class,
                Collections.singletonList(
                                new TestPluginFileSystemPlugin(
                                        testPluginSchema, LocalFileSystem.getSharedInstance()))
                        .iterator());

        FileSystem.initialize(new Configuration(), new TestingPluginManager(fileSystemPlugins));

        // check the gotten filesystem
        FileSystem fileSystem = FileSystem.get(URI.create(testPluginSchema + ":///a/b/c"));
        assertThat(fileSystem)
                .isInstanceOf(PluginFileSystemWrapper.ClassLoaderFixingFileSystem.class);

        // the filesystem should wrap LocalFileSystem
        PluginFileSystemWrapper.ClassLoaderFixingFileSystem classLoaderFixingFileSystem =
                (PluginFileSystemWrapper.ClassLoaderFixingFileSystem) fileSystem;
        assertThat(classLoaderFixingFileSystem.getWrappedDelegate())
                .isInstanceOf(LocalFileSystem.class);

        // now, try to check the operations of the FileSystem is fine
        FsPath path = new FsPath(tempDir.toString(), "test");
        final FSDataOutputStream outputStream =
                fileSystem.create(path, FileSystem.WriteMode.OVERWRITE);
        final byte[] testbytes = {1, 2, 3, 4, 5};
        outputStream.write(testbytes);
        outputStream.close();

        // check the path
        assertThat(fileSystem.exists(path)).isTrue();
        // try to read the file
        byte[] testbytesRead = new byte[5];
        FSDataInputStream inputStream = fileSystem.open(path);
        assertThat(5).isEqualTo(inputStream.read(testbytesRead));
        inputStream.close();
        assertThat(testbytesRead).isEqualTo(testbytes);

        // try to seek the file
        inputStream = fileSystem.open(path);
        inputStream.seek(4);
        testbytesRead = new byte[1];
        assertThat(1).isEqualTo(inputStream.read(testbytesRead));
        assertThat(testbytesRead).isEqualTo(new byte[] {testbytes[4]});

        // now delete the file
        assertThat(fileSystem.delete(path, true)).isTrue();
        // now try to list the files, it should be empty
        assertThat(fileSystem.listStatus(new FsPath(tempDir.toString()))).isEmpty();
        // get the status of the file should throw exception
        assertThatThrownBy(() -> fileSystem.getFileStatus(path))
                .isInstanceOf(FileNotFoundException.class);
        // rename should return false
        assertThat(fileSystem.rename(path, new FsPath(tempDir.toString(), "test1"))).isFalse();
    }

    @Test
    void testUnsupportedFS() {
        /*
        exception should be:
        com.alibaba.fluss.fs.UnsupportedFileSystemSchemeException: Could not find a file system implementation
        for scheme 'unknownfs'. The scheme is not directly supported by Fluss and no Hadoop file system to support this
        scheme could be loaded. */
        assertThatThrownBy(() -> getFileSystemWithoutSafetyNet("unknownfs://authority/"))
                .isInstanceOf(UnsupportedFileSystemSchemeException.class)
                .hasMessageContaining("not directly supported");
    }

    @Test
    void testKnownFSWithoutPluginsAndException() {
        try {
            /*
            exception should be:
            org.apache.fluss.core.fs.UnsupportedFileSystemSchemeException: Could not find a file
            system implementation for scheme 'oss'. File system schemes are supported by Fluss through the following
            plugin(s): fluss-fs-oss. No file system to support this scheme could be loaded.
            Please ensure that each plugin is configured properly and resides within its own subfolder in the plugins directory. */
            assertThatThrownBy(() -> getFileSystemWithoutSafetyNet("oss://authority/"))
                    .isInstanceOf(UnsupportedFileSystemSchemeException.class)
                    .hasMessageContaining("File system schemes are supported")
                    .hasMessageContaining("fluss-fs-oss")
                    .hasMessageContaining("Please ensure that each plugin is configured properly");
        } finally {
            FileSystem.initialize(new Configuration(), null);
        }
    }

    private static FileSystem getFileSystemWithoutSafetyNet(final String uri)
            throws URISyntaxException, IOException {
        final FileSystem fileSystem = FileSystem.get(new URI(uri));

        if (fileSystem instanceof WrappingProxy) {
            //noinspection unchecked
            return WrappingProxyUtils.stripProxy((WrappingProxy<FileSystem>) fileSystem);
        }

        return fileSystem;
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

    private static class TestPluginFileSystemPlugin implements FileSystemPlugin {

        private final String scheme;
        private final FileSystem fileSystem;

        public TestPluginFileSystemPlugin(String scheme, FileSystem fileSystem) {
            this.scheme = scheme;
            this.fileSystem = fileSystem;
        }

        @Override
        public String getScheme() {
            return scheme;
        }

        @Override
        public FileSystem create(URI fsUri, Configuration configuration) throws IOException {
            return fileSystem;
        }
    }
}
