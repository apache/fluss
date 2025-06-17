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

package com.alibaba.fluss.fs.hdfs;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.fs.FileSystem;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests that validate the behavior of the Hadoop File System Plugin. */
class HadoopFsPluginTest {

    @Test
    void testCreateHadoopFsWithoutConfig() throws Exception {
        final URI uri = URI.create("hdfs://localhost:12345/");

        HadoopFsPlugin plugin = new HadoopFsPlugin();
        FileSystem fs = plugin.create(uri, new Configuration());

        assertThat(fs.getUri().getScheme()).isEqualTo(uri.getScheme());
        assertThat(fs.getUri().getAuthority()).isEqualTo(uri.getAuthority());
        assertThat(fs.getUri().getPort()).isEqualTo(uri.getPort());
    }

    @Test
    void testCreateHadoopFsWithMissingAuthority() {
        final URI uri = URI.create("hdfs:///my/path");

        HadoopFsPlugin plugin = new HadoopFsPlugin();

        assertThatThrownBy(() -> plugin.create(uri, new Configuration()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("authority");
    }
}
