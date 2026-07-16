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

package org.apache.fluss.lake.hudi.flink;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.lake.hudi.testutils.FlinkHudiTieringTestBase;
import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nullable;

/** Base class for Hudi union read tests. */
public class FlinkUnionReadTestBase extends FlinkHudiTieringTestBase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setClusterConf(initConfig())
                    .setNumOfTabletServers(3)
                    .build();

    protected static final int DEFAULT_BUCKET_NUM = 1;
    StreamTableEnvironment batchTEnv;
    StreamTableEnvironment streamTEnv;

    @BeforeAll
    protected static void beforeAll() {
        FlinkHudiTieringTestBase.beforeAll(FLUSS_CLUSTER_EXTENSION.getClientConfig());
    }

    @BeforeEach
    public void beforeEach() {
        super.beforeEach();
        buildBatchTEnv();
        buildStreamTEnv();
    }

    @Override
    protected FlussClusterExtension getFlussClusterExtension() {
        return FLUSS_CLUSTER_EXTENSION;
    }

    protected StreamTableEnvironment buildStreamTEnv(@Nullable String savepointPath) {
        Configuration conf = new Configuration();
        if (savepointPath != null) {
            conf.setString("execution.savepoint.path", savepointPath);
            execEnv.configure(conf);
        }
        String bootstrapServers = String.join(",", clientConf.get(ConfigOptions.BOOTSTRAP_SERVERS));
        streamTEnv = StreamTableEnvironment.create(execEnv, EnvironmentSettings.inStreamingMode());
        streamTEnv.executeSql(
                String.format(
                        "create catalog %s with ('type' = 'fluss', '%s' = '%s')",
                        CATALOG_NAME, ConfigOptions.BOOTSTRAP_SERVERS.key(), bootstrapServers));
        streamTEnv.executeSql("use catalog " + CATALOG_NAME);
        streamTEnv.executeSql("use " + DEFAULT_DB);
        return streamTEnv;
    }

    private void buildStreamTEnv() {
        buildStreamTEnv(null);
    }

    public void buildBatchTEnv() {
        String bootstrapServers = String.join(",", clientConf.get(ConfigOptions.BOOTSTRAP_SERVERS));
        batchTEnv = StreamTableEnvironment.create(execEnv, EnvironmentSettings.inBatchMode());
        batchTEnv.executeSql(
                String.format(
                        "create catalog %s with ('type' = 'fluss', '%s' = '%s')",
                        CATALOG_NAME, ConfigOptions.BOOTSTRAP_SERVERS.key(), bootstrapServers));
        batchTEnv.executeSql("use catalog " + CATALOG_NAME);
        batchTEnv.executeSql("use " + DEFAULT_DB);
    }
}
