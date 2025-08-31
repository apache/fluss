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
import com.alibaba.fluss.fs.hdfs.utils.HadoopUtils;
import com.alibaba.fluss.testutils.common.CommonTestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that validate the loading of the Hadoop configuration, relative to entries in the Fluss
 * configuration and the environment variables.
 */
class HadoopConfigLoadingTest {

    private static final String IN_CP_CONFIG_KEY = "cp_conf_key";
    private static final String IN_CP_CONFIG_VALUE = "oompf!";

    @Test
    void loadFromClasspathByDefault() {
        org.apache.hadoop.conf.Configuration hadoopConf =
                HadoopUtils.getHadoopConfiguration(new Configuration());

        assertThat(hadoopConf.get(IN_CP_CONFIG_KEY, null)).isEqualTo(IN_CP_CONFIG_VALUE);
    }

    @Test
    void loadFromEnvVariables(@TempDir File hadoopConfDir, @TempDir File hadoopHome)
            throws Exception {
        final String k1 = "where?";
        final String v1 = "I'm on a boat";
        final String k2 = "when?";
        final String v2 = "midnight";
        final String k3 = "why?";
        final String v3 = "what do you think?";
        final String k4 = "which way?";
        final String v4 = "south, always south...";
        final String k5 = "how long?";
        final String v5 = "an eternity";
        final String k6 = "for real?";
        final String v6 = "quite so...";

        final File hadoopHomeConf = new File(hadoopHome, "conf");
        final File hadoopHomeEtc = new File(hadoopHome, "etc/hadoop");

        assertThat(hadoopHomeConf.mkdirs()).isTrue();
        assertThat(hadoopHomeEtc.mkdirs()).isTrue();

        final File file1 = new File(hadoopConfDir, "core-site.xml");
        final File file2 = new File(hadoopConfDir, "hdfs-site.xml");
        final File file3 = new File(hadoopHomeConf, "core-site.xml");
        final File file4 = new File(hadoopHomeConf, "hdfs-site.xml");
        final File file5 = new File(hadoopHomeEtc, "core-site.xml");
        final File file6 = new File(hadoopHomeEtc, "hdfs-site.xml");

        printConfig(file1, k1, v1);
        printConfig(file2, k2, v2);
        printConfig(file3, k3, v3);
        printConfig(file4, k4, v4);
        printConfig(file5, k5, v5);
        printConfig(file6, k6, v6);

        final org.apache.hadoop.conf.Configuration hadoopConf;

        final Map<String, String> originalEnv = System.getenv();
        final Map<String, String> newEnv = new HashMap<>(originalEnv);
        newEnv.put("HADOOP_CONF_DIR", hadoopConfDir.getAbsolutePath());
        newEnv.put("HADOOP_HOME", hadoopHome.getAbsolutePath());
        try {
            CommonTestUtils.setEnv(newEnv);
            hadoopConf = HadoopUtils.getHadoopConfiguration(new Configuration());
        } finally {
            CommonTestUtils.setEnv(originalEnv);
        }

        // contains extra entries
        assertThat(hadoopConf.get(k1, null)).isEqualTo(v1);
        assertThat(hadoopConf.get(k2, null)).isEqualTo(v2);
        assertThat(hadoopConf.get(k3, null)).isEqualTo(v3);
        assertThat(hadoopConf.get(k4, null)).isEqualTo(v4);
        assertThat(hadoopConf.get(k5, null)).isEqualTo(v5);
        assertThat(hadoopConf.get(k6, null)).isEqualTo(v6);

        // also contains classpath defaults
        assertThat(hadoopConf.get(IN_CP_CONFIG_KEY, null)).isEqualTo(IN_CP_CONFIG_VALUE);
    }

    @Test
    void loadOverlappingConfig(@TempDir File hadoopConfDir, @TempDir File hadoopHome)
            throws Exception {
        final String k1 = "key1";
        final String k2 = "key2";
        final String k3 = "key3";
        final String k4 = "key4";
        final String k5 = "key5";

        final String v1 = "from HADOOP_CONF_DIR";
        final String v2 = "from Fluss config";
        final String v3 = "from Fluss config";
        final String v4 = "from HADOOP_HOME/etc/hadoop";
        final String v5 = "from HADOOP_HOME/conf";

        final File hadoopHomeConf = new File(hadoopHome, "conf");
        final File hadoopHomeEtc = new File(hadoopHome, "etc/hadoop");

        assertThat(hadoopHomeConf.mkdirs()).isTrue();
        assertThat(hadoopHomeEtc.mkdirs()).isTrue();

        final File file1 = new File(hadoopConfDir, "core-site.xml");
        final File file2 = new File(hadoopHomeEtc, "core-site.xml");
        final File file3 = new File(hadoopHomeConf, "core-site.xml");

        // set configs to hadoop conf dir
        printConfig(file1, k1, v1);

        // set configs to hadoop home
        Map<String, String> properties4 = new HashMap<>();
        properties4.put(k1, v4);
        properties4.put(k2, v4);
        properties4.put(k3, v4);
        properties4.put(k4, v4);
        printConfigs(file2, properties4);

        // set configs to hadoop home
        Map<String, String> properties5 = new HashMap<>();
        properties5.put(k1, v5);
        properties5.put(k2, v5);
        properties5.put(k3, v5);
        properties5.put(k4, v5);
        properties5.put(k5, v5);
        printConfigs(file3, properties5);

        final Configuration cfg = new Configuration();
        // set configs to fluss config
        cfg.setString("fluss.hadoop." + k2, v2);
        cfg.setString("fluss.hadoop." + k3, v3);

        final org.apache.hadoop.conf.Configuration hadoopConf;

        final Map<String, String> originalEnv = System.getenv();
        final Map<String, String> newEnv = new HashMap<>(originalEnv);
        newEnv.put("HADOOP_CONF_DIR", hadoopConfDir.getAbsolutePath());
        newEnv.put("HADOOP_HOME", hadoopHome.getAbsolutePath());
        try {
            CommonTestUtils.setEnv(newEnv);
            hadoopConf = HadoopUtils.getHadoopConfiguration(cfg);
        } finally {
            CommonTestUtils.setEnv(originalEnv);
        }

        // contains extra entries
        assertThat(hadoopConf.get(k1, null)).isEqualTo(v1);
        assertThat(hadoopConf.get(k2, null)).isEqualTo(v2);
        assertThat(hadoopConf.get(k3, null)).isEqualTo(v3);
        assertThat(hadoopConf.get(k4, null)).isEqualTo(v4);
        assertThat(hadoopConf.get(k5, null)).isEqualTo(v5);

        // also contains classpath defaults
        assertThat(hadoopConf.get(IN_CP_CONFIG_KEY, null)).isEqualTo(IN_CP_CONFIG_VALUE);
    }

    @Test
    void loadFromFlussConfEntry() {
        final String prefix = "fluss.hadoop.";

        final String k1 = "brooklyn";
        final String v1 = "nets";

        final String k2 = "miami";
        final String v2 = "heat";

        final String k3 = "philadelphia";
        final String v3 = "76ers";

        final String k4 = "golden.state";
        final String v4 = "warriors";

        final String k5 = "oklahoma.city";
        final String v5 = "thunders";

        final Configuration cfg = new Configuration();
        cfg.setString(prefix + k1, v1);
        cfg.setString(prefix + k2, v2);
        cfg.setString(prefix + k3, v3);
        cfg.setString(prefix + k4, v4);
        cfg.setString(k5, v5);

        org.apache.hadoop.conf.Configuration hadoopConf = HadoopUtils.getHadoopConfiguration(cfg);

        // contains extra entries
        assertThat(hadoopConf.get(k1, null)).isEqualTo(v1);
        assertThat(hadoopConf.get(k2, null)).isEqualTo(v2);
        assertThat(hadoopConf.get(k3, null)).isEqualTo(v3);
        assertThat(hadoopConf.get(k4, null)).isEqualTo(v4);
        assertThat(hadoopConf.get(k5) == null).isTrue();

        // also contains classpath defaults
        assertThat(hadoopConf.get(IN_CP_CONFIG_KEY, null)).isEqualTo(IN_CP_CONFIG_VALUE);
    }

    private static void printConfig(File file, String key, String value) throws IOException {
        Map<String, String> map = new HashMap<>(1);
        map.put(key, value);
        printConfigs(file, map);
    }

    private static void printConfigs(File file, Map<String, String> properties) throws IOException {
        try (PrintStream out = new PrintStream(Files.newOutputStream(file.toPath()))) {
            out.println("<?xml version=\"1.0\"?>");
            out.println("<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>");
            out.println("<configuration>");
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                out.println("\t<property>");
                out.println("\t\t<name>" + entry.getKey() + "</name>");
                out.println("\t\t<value>" + entry.getValue() + "</value>");
                out.println("\t</property>");
            }
            out.println("</configuration>");
        }
    }
}
