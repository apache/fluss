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

package org.apache.fluss.client;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** IT case for {@link ConnectionFactory} resolving config provider markers. */
class ConfigProviderConnectionITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder().setNumOfTabletServers(1).build();

    @Test
    void testConnectWithConfigProviderResolvedBootstrapServers(@TempDir Path secretsDir)
            throws Exception {
        Configuration clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        String bootstrapServers = clientConf.get(ConfigOptions.BOOTSTRAP_SERVERS).get(0);
        Files.write(
                secretsDir.resolve("bootstrap-servers"),
                bootstrapServers.getBytes(StandardCharsets.UTF_8));

        Configuration conf = new Configuration(clientConf);
        conf.setString(ConfigOptions.CONFIG_PROVIDERS.key(), "directory");
        conf.setString("config.providers.directory.param.allowed.paths", secretsDir.toString());
        conf.setString(
                ConfigOptions.BOOTSTRAP_SERVERS.key(),
                "${directory:" + secretsDir + ":bootstrap-servers}");

        try (Connection connection = ConnectionFactory.createConnection(conf);
                Admin admin = connection.getAdmin()) {
            assertThat(admin.listDatabases().get()).contains("fluss");
        }

        // caller's configuration is not mutated
        assertThat(conf.toMap().get(ConfigOptions.BOOTSTRAP_SERVERS.key()))
                .startsWith("${directory:");
    }
}
