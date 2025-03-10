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

package com.alibaba.fluss.server.zk;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.server.utils.FatalErrorHandler;

import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/** ZooKeeper test utilities. */
public class ZooKeeperTestUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperTestUtils.class);

    /**
     * Creates a new {@link TestingServer}, setting additional configuration properties for
     * stability purposes.
     */
    public static TestingServer createAndStartZookeeperTestingServer() throws Exception {
        return new TestingServer(getZookeeperInstanceSpecWithIncreasedSessionTimeout(), true);
    }

    /** Create a {@link ZooKeeperClient} client using provided connect string. */
    public static ZooKeeperClient createZooKeeperClient(
            String connectString, FatalErrorHandler fatalErrorHandler) {
        final Configuration config = new Configuration();
        config.setString(ConfigOptions.ZOOKEEPER_ADDRESS, connectString);

        return ZooKeeperUtils.startZookeeperClient(config, fatalErrorHandler);
    }

    private static InstanceSpec getZookeeperInstanceSpecWithIncreasedSessionTimeout() {
        // this gives us the default settings
        final InstanceSpec instanceSpec = InstanceSpec.newInstanceSpec();

        final Map<String, Object> properties = new HashMap<>();
        properties.put("maxSessionTimeout", "60000");

        final boolean deleteDataDirectoryOnClose = true;

        return new InstanceSpec(
                instanceSpec.getDataDirectory(),
                instanceSpec.getPort(),
                instanceSpec.getElectionPort(),
                instanceSpec.getQuorumPort(),
                deleteDataDirectoryOnClose,
                instanceSpec.getServerId(),
                instanceSpec.getTickTime(),
                instanceSpec.getMaxClientCnxns(),
                properties,
                instanceSpec.getHostname());
    }
}
