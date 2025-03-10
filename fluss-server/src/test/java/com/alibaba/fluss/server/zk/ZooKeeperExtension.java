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

import com.alibaba.fluss.server.utils.FatalErrorHandler;
import com.alibaba.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import com.alibaba.fluss.testutils.common.CustomExtension;

import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;
import static com.alibaba.fluss.utils.Preconditions.checkState;

/** A Junit {@link Extension} which starts a {@link ZooKeeperServer}. */
public class ZooKeeperExtension implements CustomExtension {

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperExtension.class);

    @Nullable private TestingServer zooKeeperServer;

    @Nullable private ZooKeeperClient zooKeeperClient;

    @Override
    public void before(ExtensionContext context) throws Exception {
        close();
        zooKeeperServer = ZooKeeperTestUtils.createAndStartZookeeperTestingServer();
    }

    @Override
    public void after(ExtensionContext context) {
        try {
            close();
        } catch (IOException e) {
            LOG.warn("Could not properly terminate the {}.", getClass().getSimpleName(), e);
        }
    }

    public void close() throws IOException {
        terminateCuratorFrameworkWrapper();
        terminateZooKeeperServer();
    }

    private void terminateCuratorFrameworkWrapper() {
        if (zooKeeperClient != null) {
            zooKeeperClient.close();
            zooKeeperClient = null;
        }
    }

    private void terminateZooKeeperServer() throws IOException {
        if (zooKeeperServer != null) {
            zooKeeperServer.close();
            zooKeeperServer = null;
        }
    }

    public String getConnectString() {
        return getRunningZookeeperInstanceOrFail().getConnectString();
    }

    private TestingServer getRunningZookeeperInstanceOrFail() {
        checkState(zooKeeperServer != null);
        return zooKeeperServer;
    }

    public ZooKeeperClient getZooKeeperClient(FatalErrorHandler fatalErrorHandler) {
        if (zooKeeperClient == null) {
            zooKeeperClient = createZooKeeperClient(fatalErrorHandler);
        }

        return zooKeeperClient;
    }

    public ZooKeeperClient createZooKeeperClient(FatalErrorHandler fatalErrorHandler) {
        return ZooKeeperTestUtils.createZooKeeperClient(getConnectString(), fatalErrorHandler);
    }

    public void restart() throws Exception {
        getRunningZookeeperInstanceOrFail().restart();
    }

    public void stop() throws IOException {
        getRunningZookeeperInstanceOrFail().stop();
    }

    /** Cleanup the root path of the ZooKeeper server. */
    public void cleanupRoot() {
        cleanupPath("/");
    }

    /** Cleanup the path(include children) of the ZooKeeper server. */
    public void cleanupPath(String path) {
        checkNotNull(zooKeeperClient);
        try {
            zooKeeperClient.getCuratorClient().delete().deletingChildrenIfNeeded().forPath(path);
        } catch (KeeperException.NoNodeException ignored) {
            // ignore, some tests may not create any nodes
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
