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

package com.alibaba.fluss.server.coordinator.event.watcher;

import com.alibaba.fluss.server.coordinator.event.AlterTableOrPartitionBucketEvent;
import com.alibaba.fluss.server.coordinator.event.EventManager;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.data.BucketAssignment;
import com.alibaba.fluss.server.zk.data.TableAssignment;
import com.alibaba.fluss.server.zk.data.ZkData.TableIdZNode;
import com.alibaba.fluss.server.zk.data.ZkData.TableIdsZNode;
import com.alibaba.fluss.shaded.curator5.org.apache.curator.framework.recipes.cache.ChildData;
import com.alibaba.fluss.shaded.curator5.org.apache.curator.framework.recipes.cache.CuratorCache;
import com.alibaba.fluss.shaded.curator5.org.apache.curator.framework.recipes.cache.CuratorCacheListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/** A watcher to watch the table assignment change(bucket expansion) in zookeeper. */
public class TableAssignmentChangeWatcher {

    private static final Logger LOG = LoggerFactory.getLogger(TableAssignmentChangeWatcher.class);

    private final CuratorCache curatorCache;

    private volatile boolean running;

    private final EventManager eventManager;

    public TableAssignmentChangeWatcher(
            ZooKeeperClient zooKeeperClient, EventManager eventManager) {
        this.curatorCache =
                CuratorCache.build(zooKeeperClient.getCuratorClient(), TableIdsZNode.path());
        this.eventManager = eventManager;
        this.curatorCache.listenable().addListener(new TableBucketChangeListener());
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
        LOG.info("Stopping TableAssignmentChangeWatcher");
        curatorCache.close();
    }

    protected class TableBucketChangeListener implements CuratorCacheListener {

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
                        if (newData != null) {
                            Long tableId = TableIdZNode.parsePath(newData.getPath());
                            if (tableId == null) {
                                break;
                            }
                            TableAssignment oldTableAssignment =
                                    TableIdZNode.decode(oldData.getData());
                            TableAssignment newTableAssignment =
                                    TableIdZNode.decode(newData.getData());
                            if (validBucketChange(oldTableAssignment, newTableAssignment)) {
                                eventManager.put(
                                        new AlterTableOrPartitionBucketEvent(
                                                tableId, newTableAssignment));
                            }
                        }
                        break;
                    }
                default:
                    break;
            }
        }

        private boolean validBucketChange(
                TableAssignment oldTableAssignment, TableAssignment newTableAssignment) {
            Map<Integer, BucketAssignment> oldBucketAssignments =
                    oldTableAssignment.getBucketAssignments();
            Map<Integer, BucketAssignment> newBucketAssignments =
                    newTableAssignment.getBucketAssignments();

            return newBucketAssignments.keySet().containsAll(oldBucketAssignments.keySet())
                    && newBucketAssignments.size() > oldBucketAssignments.size();
        }
    }
}
