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

import org.apache.fluss.server.coordinator.event.AlterTableOrPartitionBucketEvent;
import org.apache.fluss.server.coordinator.event.EventManager;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.recipes.cache.CuratorCacheListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;

import static org.apache.fluss.server.coordinator.event.watcher.TableAssignmentChangeWatcher.validBucketChange;
import static org.apache.fluss.server.zk.data.ZkData.PartitionIdZNode;
import static org.apache.fluss.server.zk.data.ZkData.PartitionIdsZNode;

/** A watcher to watch the partition assignment change(bucket expansion) in zookeeper. */
public class PartitionAssignmentChangeWatcher {

    private static final Logger LOG =
            LoggerFactory.getLogger(PartitionAssignmentChangeWatcher.class);

    private static final Pattern PARTITION_ASSIGNMENT_PATH_PATTERN =
            Pattern.compile("^/tabletservers/partitions/\\d+$");

    private final CuratorCache curatorCache;

    private volatile boolean running;

    private final EventManager eventManager;

    public PartitionAssignmentChangeWatcher(
            ZooKeeperClient zooKeeperClient, EventManager eventManager) {
        this.curatorCache =
                CuratorCache.build(zooKeeperClient.getCuratorClient(), PartitionIdsZNode.path());
        this.eventManager = eventManager;
        this.curatorCache.listenable().addListener(new PartitionBucketChangeListener());
    }

    public void start() {
        running = true;
        curatorCache.start();
    }

    public void stop() {
        if (!running) {
            return;
        }
        running = false;
        LOG.info("Stopping PartitionAssignmentChangeWatcher");
        curatorCache.close();
    }

    private class PartitionBucketChangeListener implements CuratorCacheListener {

        @Override
        public void event(Type type, ChildData oldData, ChildData newData) {
            if (newData != null) {
                LOG.debug("Received {} event (path: {})", type, newData.getPath());
            } else {
                LOG.debug("Received {} event", type);
            }

            switch (type) {
                case NODE_CHANGED:
                    {
                        // only NODE_CHANGE on /tabletservers/partitions/[partitionId] is valid
                        // partition assignment change
                        if (newData != null
                                && PARTITION_ASSIGNMENT_PATH_PATTERN
                                        .matcher(newData.getPath())
                                        .matches()) {
                            Long partitionId = PartitionIdZNode.parsePath(newData.getPath());
                            if (partitionId == null) {
                                break;
                            }

                            PartitionAssignment oldPartitionAssignment =
                                    PartitionIdZNode.decode(oldData.getData());
                            PartitionAssignment newPartitionAssignment =
                                    PartitionIdZNode.decode(newData.getData());
                            if (validBucketChange(oldPartitionAssignment, newPartitionAssignment)) {
                                eventManager.put(
                                        new AlterTableOrPartitionBucketEvent(
                                                newPartitionAssignment.getTableId(),
                                                partitionId,
                                                newPartitionAssignment));
                            }
                        }
                        break;
                    }
                default:
                    break;
            }
        }
    }
}
