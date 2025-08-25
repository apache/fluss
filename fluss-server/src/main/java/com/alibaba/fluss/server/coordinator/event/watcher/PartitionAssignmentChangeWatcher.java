package com.alibaba.fluss.server.coordinator.event.watcher;

import com.alibaba.fluss.server.coordinator.event.AlterTableOrPartitionBucketEvent;
import com.alibaba.fluss.server.coordinator.event.EventManager;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.data.BucketAssignment;
import com.alibaba.fluss.server.zk.data.PartitionAssignment;
import com.alibaba.fluss.server.zk.data.TableAssignment;
import com.alibaba.fluss.server.zk.data.ZkData;
import com.alibaba.fluss.server.zk.data.ZkData.PartitionIdsZNode;
import com.alibaba.fluss.shaded.curator5.org.apache.curator.framework.recipes.cache.ChildData;
import com.alibaba.fluss.shaded.curator5.org.apache.curator.framework.recipes.cache.CuratorCache;
import com.alibaba.fluss.shaded.curator5.org.apache.curator.framework.recipes.cache.CuratorCacheListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/** A watcher to watch the partition assignment change(bucket expansion) in zookeeper. */
public class PartitionAssignmentChangeWatcher {

    private static final Logger LOG =
            LoggerFactory.getLogger(PartitionAssignmentChangeWatcher.class);

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
                        if (newData != null) {
                            Long partitionId = ZkData.PartitionIdZNode.parsePath(newData.getPath());
                            if (partitionId == null) {
                                break;
                            }
                            PartitionAssignment oldPartitionAssignment =
                                    ZkData.PartitionIdZNode.decode(oldData.getData());
                            PartitionAssignment newPartitionAssignment =
                                    ZkData.PartitionIdZNode.decode(newData.getData());
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
