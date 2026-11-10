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

package org.apache.fluss.fs.oss;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.fs.oss.token.OSSSecurityTokenReceiver;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link OSSFileSystemPlugin}. */
class OSSFileSystemPluginTest {

    @Test
    void testAliasSchemes() {
        assertThat(new OSSFileSystemPlugin().getScheme()).isEqualTo("oss");
        assertThat(new OSSFileSystemPlugin().getAliasSchemes()).containsExactly("ossj");
        assertThat(new OSSSecurityTokenReceiver().getAliasSchemes()).containsExactly("ossj");
    }

    @ParameterizedTest
    @ValueSource(strings = {"oss", "ossj"})
    void testFileSystemPreservesScheme(String scheme) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setString("fs.oss.endpoint", "oss.example.invalid");
        configuration.setString("fs.oss.region", "cn-hangzhou");
        configuration.setString("fs.oss.accessKeyId", "test-access-key");
        configuration.setString("fs.oss.accessKeySecret", "test-secret-key");
        AtomicReference<AliyunOSSFileSystem> hadoopFileSystemRef = new AtomicReference<>();
        OSSFileSystemPlugin plugin =
                new OSSFileSystemPlugin() {
                    @Override
                    protected org.apache.hadoop.fs.FileSystem initFileSystem(
                            URI fsUri, org.apache.hadoop.conf.Configuration hadoopConfig)
                            throws IOException {
                        AliyunOSSFileSystem fileSystem =
                                (AliyunOSSFileSystem) super.initFileSystem(fsUri, hadoopConfig);
                        hadoopFileSystemRef.set(fileSystem);
                        return fileSystem;
                    }
                };

        URI uri = URI.create(scheme + "://bucket/path%20with%20spaces");
        FileSystem fileSystem = plugin.create(uri, configuration);
        AliyunOSSFileSystem hadoopFileSystem = hadoopFileSystemRef.get();
        try {
            assertThat(fileSystem.getUri()).isEqualTo(URI.create(scheme + "://bucket"));
            // Hadoop can return the bucket root status without contacting OSS.
            FsPath root = new FsPath(scheme + "://bucket/");
            assertThat(fileSystem.getFileStatus(root).getPath()).isEqualTo(root);
            assertThat(hadoopFileSystem.getWorkingDirectory().toUri().getScheme())
                    .isEqualTo(scheme);
            assertThat(
                            new Path(uri)
                                    .makeQualified(
                                            hadoopFileSystem.getUri(),
                                            hadoopFileSystem.getWorkingDirectory())
                                    .toUri())
                    .isEqualTo(uri);
        } finally {
            hadoopFileSystem.close();
        }
    }
}
