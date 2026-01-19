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

package org.apache.fluss.server.coordinator.event.watcher;

import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.server.coordinator.event.EventManager;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.fluss.shaded.curator5.org.apache.curator.utils.ZKPaths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An abstract base server class for {@link CoordinatorChangeWatcher} and {@link
 * TabletServerChangeWatcher}.
 */
public abstract class ServerBaseChangeWatcher {

    private static final Logger LOG = LoggerFactory.getLogger(ServerBaseChangeWatcher.class);

    protected final CuratorCache curatorCache;
    protected final EventManager eventManager;
    protected volatile boolean running;

    public ServerBaseChangeWatcher(
            ZooKeeperClient zooKeeperClient, EventManager eventManager, String zkPath) {
        this.curatorCache = CuratorCache.build(zooKeeperClient.getCuratorClient(), zkPath);
        this.eventManager = eventManager;
        this.curatorCache.listenable().addListener(createListener());
    }

    /** Creates the listener for server change events. */
    protected abstract CuratorCacheListener createListener();

    /** Returns the watcher name for logging. */
    protected abstract String getWatcherName();

    public void start() {
        running = true;
        curatorCache.start();
    }

    public void stop() {
        if (!running) {
            return;
        }
        running = false;
        LOG.info("Stopping {}", getWatcherName());
        curatorCache.close();
    }

    protected int getServerIdFromEvent(ChildData data) {
        try {
            return Integer.parseInt(ZKPaths.getNodeFromPath(data.getPath()));
        } catch (NumberFormatException e) {
            throw new FlussRuntimeException(
                    "Invalid server id in zookeeper path: " + data.getPath(), e);
        }
    }
}
