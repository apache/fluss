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

package com.alibaba.fluss.server.coordinator;

import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.rpc.RpcGateway;
import com.alibaba.fluss.rpc.gateway.CoordinatorGateway;
import com.alibaba.fluss.server.ServerBase;
import com.alibaba.fluss.server.ServerITCaseBase;
import com.alibaba.fluss.server.utils.AvailablePortExtension;
import com.alibaba.fluss.testutils.common.EachCallbackWrapper;

import org.junit.jupiter.api.extension.RegisterExtension;

import static com.alibaba.fluss.config.ConfigOptions.DEFAULT_LISTENER_NAME;

/** IT Case for {@link CoordinatorServer} . */
class CoordinatorServerITCase extends ServerITCaseBase {

    private static final String HOSTNAME = "localhost";

    @RegisterExtension
    final EachCallbackWrapper<AvailablePortExtension> portExtension =
            new EachCallbackWrapper<>(new AvailablePortExtension());

    @Override
    protected ServerNode getServerNode() {
        return new ServerNode(1, HOSTNAME, getPort(), ServerType.COORDINATOR);
    }

    @Override
    protected Class<? extends RpcGateway> getRpcGatewayClass() {
        return CoordinatorGateway.class;
    }

    @Override
    protected Class<? extends ServerBase> getServerClass() {
        return CoordinatorServer.class;
    }

    @Override
    protected Configuration getServerConfig() {
        Configuration conf = new Configuration();
        conf.set(
                ConfigOptions.BIND_LISTENERS,
                String.format("%s://%s:%d", DEFAULT_LISTENER_NAME, HOSTNAME, getPort()));
        conf.set(ConfigOptions.REMOTE_DATA_DIR, "/tmp/fluss/remote-data");
        return conf;
    }

    private int getPort() {
        return portExtension.getCustomExtension().port();
    }
}
