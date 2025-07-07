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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.server.ServerBase;
import org.apache.fluss.server.ServerTestBase;
import org.apache.fluss.server.zk.data.CoordinatorAddress;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link CoordinatorServer} . */
class CoordinatorServerTest extends ServerTestBase {

    private CoordinatorServer coordinatorServer;

    @BeforeEach
    void beforeEach() throws Exception {
        coordinatorServer = startCoordinatorServer(createConfiguration());
    }

    @AfterEach
    void after() throws Exception {
        if (coordinatorServer != null) {
            coordinatorServer.close();
        }
    }

    @Override
    protected ServerBase getServer() {
        return coordinatorServer;
    }

    @Override
    protected ServerBase getStartFailServer() {
        Configuration configuration = createConfiguration();
        configuration.set(ConfigOptions.BIND_LISTENERS, "CLIENT://localhost:-12");
        return new CoordinatorServer(configuration);
    }

    @Override
    protected void checkAfterStartServer() throws Exception {
        assertThat(coordinatorServer.getRpcServer()).isNotNull();
        // check the data put in zk after coordinator server start
        Optional<CoordinatorAddress> optCoordinatorAddr = zookeeperClient.getCoordinatorAddress();
        assertThat(optCoordinatorAddr).isNotEmpty();
        verifyEndpoint(
                optCoordinatorAddr.get().getEndpoints(),
                coordinatorServer.getRpcServer().getBindEndpoints());
    }
}
