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

package org.apache.fluss.fs.juicefs;

import org.apache.fluss.config.Configuration;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests that validate the behavior of the JuiceFS File System Plugin. */
class JuiceFsPluginTest {

    @Test
    void testScheme() {
        assertThat(new JuiceFsPlugin().getScheme()).isEqualTo("jfs");
    }

    @Test
    void testHadoopConfigPropagation() {
        Configuration flussConfig = new Configuration();
        flussConfig.setString("fs.jfs.access-key", "ak-value");
        flussConfig.setString("juicefs.meta", "redis://meta-host:6379/1");
        flussConfig.setString("juicefs.cache-dir", "/var/jfsCache");
        // unrelated key — should NOT be forwarded
        flussConfig.setString("unrelated.key", "x");

        org.apache.hadoop.conf.Configuration hadoopConfig =
                new JuiceFsPlugin().getHadoopConfiguration(flussConfig);

        assertThat(hadoopConfig.get("fs.jfs.access-key")).isEqualTo("ak-value");
        assertThat(hadoopConfig.get("juicefs.meta")).isEqualTo("redis://meta-host:6379/1");
        assertThat(hadoopConfig.get("juicefs.cache-dir")).isEqualTo("/var/jfsCache");
        assertThat(hadoopConfig.get("unrelated.key")).isNull();
    }

    @Test
    void testApplyJuiceFsDefaultsInjectsImpl() {
        org.apache.hadoop.conf.Configuration hadoopConfig =
                new org.apache.hadoop.conf.Configuration(false);
        JuiceFsPlugin.applyJuiceFsDefaults(hadoopConfig);

        assertThat(hadoopConfig.get(JuiceFsPlugin.FS_JFS_IMPL_KEY))
                .isEqualTo(JuiceFsPlugin.JUICEFS_HADOOP_FS_IMPL);
        assertThat(hadoopConfig.get(JuiceFsPlugin.FS_JFS_IMPL_DISABLE_CACHE_KEY))
                .isEqualTo("false");
    }

    @Test
    void testApplyJuiceFsDefaultsDoesNotOverrideUserValues() {
        org.apache.hadoop.conf.Configuration hadoopConfig =
                new org.apache.hadoop.conf.Configuration(false);
        hadoopConfig.set(JuiceFsPlugin.FS_JFS_IMPL_KEY, "com.foo.MyJuiceFs");
        hadoopConfig.set(JuiceFsPlugin.FS_JFS_IMPL_DISABLE_CACHE_KEY, "true");

        JuiceFsPlugin.applyJuiceFsDefaults(hadoopConfig);

        assertThat(hadoopConfig.get(JuiceFsPlugin.FS_JFS_IMPL_KEY)).isEqualTo("com.foo.MyJuiceFs");
        assertThat(hadoopConfig.get(JuiceFsPlugin.FS_JFS_IMPL_DISABLE_CACHE_KEY)).isEqualTo("true");
    }

    @Test
    void testGetHadoopConfigurationWithNullFlussConfig() {
        org.apache.hadoop.conf.Configuration hadoopConfig =
                new JuiceFsPlugin().getHadoopConfiguration(null);
        assertThat(hadoopConfig).isNotNull();
    }
}
