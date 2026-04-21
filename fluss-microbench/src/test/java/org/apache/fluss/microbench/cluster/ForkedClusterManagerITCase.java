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

package org.apache.fluss.microbench.cluster;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.microbench.config.ClusterConfig;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Integration test for {@link ForkedClusterManager}. */
class ForkedClusterManagerITCase {

    @Test
    void startAndStopForkedCluster() throws Exception {
        ClusterConfig config = new ClusterConfig();
        config.setTabletServers(1);

        ForkedClusterManager manager = new ForkedClusterManager(config);
        manager.start(null);

        try {
            // Verify server PID is different from this process
            assertThat(manager.getServerPid()).isNotEqualTo(ProcessHandle.current().pid());

            // Verify client can connect and perform operations
            try (Connection conn = ConnectionFactory.createConnection(manager.getClientConfig())) {
                assertThat(conn.getAdmin().listDatabases().get()).isNotEmpty();
            }

            // Verify metrics file exists
            assertThat(manager.getMetricsFile()).exists();
        } finally {
            manager.stop();
        }
    }
}
