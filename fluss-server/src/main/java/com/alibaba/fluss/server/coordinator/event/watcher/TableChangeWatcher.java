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

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.config.TableConfig;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.SchemaInfo;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePartition;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.server.coordinator.event.CreatePartitionEvent;
import com.alibaba.fluss.server.coordinator.event.CreateTableEvent;
import com.alibaba.fluss.server.coordinator.event.DropPartitionEvent;
import com.alibaba.fluss.server.coordinator.event.DropTableEvent;
import com.alibaba.fluss.server.coordinator.event.EventManager;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.data.PartitionAssignment;
import com.alibaba.fluss.server.zk.data.TableAssignment;
import com.alibaba.fluss.server.zk.data.TableRegistration;
import com.alibaba.fluss.server.zk.data.ZkData.DatabasesZNode;
import com.alibaba.fluss.server.zk.data.ZkData.PartitionZNode;
import com.alibaba.fluss.server.zk.data.ZkData.TableZNode;

import org.apache.fluss.shaded.curator5.org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/** A watcher to watch the table changes(create/delete) in zookeeper. */
public class TableChangeWatcher {

    private static final Logger LOG = LoggerFactory.getLogger(TableChangeWatcher.class);
    private final CuratorCache curatorCache;

    private volatile boolean running;

    private final EventManager eventManager;
    private final ZooKeeperClient zooKeeperClient;

    public TableChangeWatcher(ZooKeeperClient zooKeeperClient, EventManager eventManager) {
        this.zooKeeperClient = zooKeeperClient;
        this.curatorCache =
                CuratorCache.build(zooKeeperClient.getCuratorClient(), DatabasesZNode.path());
        this.eventManager = eventManager;
        this.curatorCache.listenable().addListener(new TablePathChangeListener());
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
        LOG.info("Stopping TableChangeWatcher");
        curatorCache.close();
    }

    /** A listener to monitor the changes of table nodes in zookeeper. */
    private final class TablePathChangeListener implements CuratorCacheListener {

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
                        if (newData != null) {
                            // maybe it's for create a partition node
                            // try to parse the path as a table partition node
                            PhysicalTablePath physicalTablePath =
                                    PartitionZNode.parsePath(newData.getPath());
                            if (physicalTablePath != null) {
                                assert physicalTablePath.getPartitionName() != null;
                                processCreatePartition(
                                        physicalTablePath.getTablePath(),
                                        physicalTablePath.getPartitionName(),
                                        newData);
                            }
                        }
                        break;
                    }
                case NODE_CHANGED:
                    {
                        // we will first create the path for the table in zk when create schema for
                        // the table, then put the real table info to the path. so, it'll be a node
                        // changed event
                        if (newData != null) {
                            TablePath tablePath = TableZNode.parsePath(newData.getPath());
                            if (tablePath == null) {
                                break;
                            }
                            processCreateTable(tablePath, newData);
                        }
                        break;
                    }
                case NODE_DELETED:
                    {
                        // maybe it's for deletion of a partition
                        // try to parse the path as a table partition node
                        PhysicalTablePath physicalTablePath =
                                PartitionZNode.parsePath(oldData.getPath());
                        if (physicalTablePath != null) {
                            // it's for deletion of a table partition node
                            TablePartition partition = PartitionZNode.decode(oldData.getData());
                            eventManager.put(
                                    new DropPartitionEvent(
                                            partition.getTableId(),
                                            partition.getPartitionId(),
                                            physicalTablePath.getPartitionName()));
                        } else {
                            // maybe table node is deleted
                            // try to parse the path as a table node
                            TablePath tablePath = TableZNode.parsePath(oldData.getPath());
                            if (tablePath == null) {
                                break;
                            }
                            TableRegistration table = TableZNode.decode(oldData.getData());
                            TableConfig tableConfig =
                                    new TableConfig(Configuration.fromMap(table.properties));
                            eventManager.put(
                                    new DropTableEvent(
                                            table.tableId,
                                            tableConfig
                                                    .getAutoPartitionStrategy()
                                                    .isAutoPartitionEnabled(),
                                            tableConfig.isDataLakeEnabled()));
                        }
                        break;
                    }
                default:
                    break;
            }
        }

        private void processCreateTable(TablePath tablePath, ChildData tableData) {
            TableRegistration table = TableZNode.decode(tableData.getData());
            long tableId = table.tableId;
            TableAssignment assignment;
            SchemaInfo schemaInfo;
            if (!table.partitionKeys.isEmpty()) {
                // for partitioned table, we won't create assignment for the
                // table, we only create assignment for the partitions of the table
                assignment = TableAssignment.builder().build();
            } else {
                try {
                    Optional<TableAssignment> optAssignment =
                            zooKeeperClient.getTableAssignment(tableId);
                    if (optAssignment.isPresent()) {
                        assignment = optAssignment.get();
                    } else {
                        LOG.error("No assignments for table {} in zookeeper.", tablePath);
                        return;
                    }
                } catch (Exception e) {
                    LOG.error("Fail to get assignments for table {}.", tablePath, e);
                    return;
                }
            }
            try {
                int schemaId = zooKeeperClient.getCurrentSchemaId(tablePath);
                Optional<SchemaInfo> optSchema = zooKeeperClient.getSchemaById(tablePath, schemaId);
                if (!optSchema.isPresent()) {
                    LOG.error("No schema for table {} in zookeeper.", tablePath);
                    return;
                } else {
                    schemaInfo = optSchema.get();
                }
            } catch (Exception e) {
                LOG.error("Fail to get schema for table {}.", tablePath, e);
                return;
            }
            TableInfo tableInfo = table.toTableInfo(tablePath, schemaInfo);
            eventManager.put(new CreateTableEvent(tableInfo, assignment));
        }

        private void processCreatePartition(
                TablePath tablePath, String partitionName, ChildData partitionData) {
            TablePartition partition = PartitionZNode.decode(partitionData.getData());
            long partitionId = partition.getPartitionId();
            long tableId = partition.getTableId();
            PartitionAssignment partitionAssignment;
            try {
                Optional<PartitionAssignment> optAssignment =
                        zooKeeperClient.getPartitionAssignment(partitionId);
                if (optAssignment.isPresent()) {
                    partitionAssignment = optAssignment.get();
                } else {
                    LOG.error(
                            "No assignments for partition {} of table {} in zookeeper.",
                            partitionName,
                            tablePath);
                    return;
                }
            } catch (Exception e) {
                LOG.error(
                        "Fail to get assignments for partition {} of table {}.",
                        partitionName,
                        tablePath,
                        e);
                return;
            }
            eventManager.put(
                    new CreatePartitionEvent(
                            tablePath, tableId, partitionId, partitionName, partitionAssignment));
        }
    }
}
