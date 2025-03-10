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

package com.alibaba.fluss.server.tablet;

import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.rpc.RpcGateway;
import com.alibaba.fluss.rpc.gateway.TabletServerGateway;
import com.alibaba.fluss.server.ServerBase;
import com.alibaba.fluss.server.ServerITCaseBase;
import com.alibaba.fluss.server.utils.AvailablePortExtension;
import com.alibaba.fluss.testutils.common.EachCallbackWrapper;

import org.junit.jupiter.api.extension.RegisterExtension;

/** IT Case for {@link TabletServer} . */
public class TabletServerITCase extends ServerITCaseBase {

    private static final String HOSTNAME = "localhost";

    @RegisterExtension
    final EachCallbackWrapper<AvailablePortExtension> portExtension =
            new EachCallbackWrapper<>(new AvailablePortExtension());

    @Override
    protected ServerNode getServerNode() {
        return new ServerNode(1, HOSTNAME, getPort(), ServerType.TABLET_SERVER);
    }

    @Override
    protected Class<? extends RpcGateway> getRpcGatewayClass() {
        return TabletServerGateway.class;
    }

    @Override
    protected Class<? extends ServerBase> getServerClass() {
        return TabletServer.class;
    }

    @Override
    protected Configuration getServerConfig() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.TABLET_SERVER_PORT, getPort() + "");
        conf.set(ConfigOptions.TABLET_SERVER_HOST, HOSTNAME);
        conf.set(ConfigOptions.TABLET_SERVER_ID, 1);
        conf.set(ConfigOptions.REMOTE_DATA_DIR, "/tmp/fluss/remote-data");
        return conf;
    }

    private int getPort() {
        return portExtension.getCustomExtension().port();
    }
}
