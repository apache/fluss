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

import org.apache.fluss.server.coordinator.event.DeadCoordinatorEvent;
import org.apache.fluss.server.coordinator.event.EventManager;
import org.apache.fluss.server.coordinator.event.NewCoordinatorEvent;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.recipes.cache.CuratorCacheListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A watcher to watch the coordinator server changes(new/delete) in zookeeper. */
public class CoordinatorChangeWatcher extends ServerBaseChangeWatcher {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorChangeWatcher.class);

    public CoordinatorChangeWatcher(ZooKeeperClient zooKeeperClient, EventManager eventManager) {
        super(zooKeeperClient, eventManager, ZkData.CoordinatorIdsZNode.path());
    }

    @Override
    protected CuratorCacheListener createListener() {
        return new CoordinatorChangeListener();
    }

    @Override
    protected String getWatcherName() {
        return "CoordinatorChangeWatcher";
    }

    private final class CoordinatorChangeListener implements CuratorCacheListener {

        @Override
        public void event(Type type, ChildData oldData, ChildData newData) {
            if (newData != null) {
                LOG.debug("Received {} event (path: {})", type, newData.getPath());
            } else {
                LOG.debug("Received {} event", type);
            }

            switch (type) {
                case NODE_CREATED:
                    {
                        if (newData != null && newData.getData().length > 0) {
                            int serverId = getServerIdFromEvent(newData);
                            LOG.info("Received CHILD_ADDED event for server {}.", serverId);
                            eventManager.put(new NewCoordinatorEvent(serverId));
                        }
                        break;
                    }
                case NODE_DELETED:
                    {
                        if (oldData != null && oldData.getData().length > 0) {
                            int serverId = getServerIdFromEvent(oldData);
                            LOG.info("Received CHILD_REMOVED event for server {}.", serverId);
                            eventManager.put(new DeadCoordinatorEvent(serverId));
                        }
                        break;
                    }
                default:
                    break;
            }
        }
    }
}
