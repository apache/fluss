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

package com.alibaba.fluss.server.authorizer;

import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.utils.clock.Clock;

import org.apache.fluss.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.fluss.shaded.curator5.org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type.CHILD_ADDED;

/** A watcher to watch the change notification (create/delete/alter) in zookeeper. */
public class ZkNodeChangeNotificationWatcher {
    private static final Logger LOG =
            LoggerFactory.getLogger(ZkNodeChangeNotificationWatcher.class);

    private final CuratorCache curatorCache;
    private final NotificationHandler notificationHandler;
    private volatile boolean running;
    private final ZooKeeperClient zooKeeperClient;
    private final String seqNodeRoot;
    private final String seqNodePrefix;
    private final long changeExpirationMs;
    private final Clock clock;
    private final Object lock = new Object();

    private volatile long lastExecutedChange = -1L;

    public ZkNodeChangeNotificationWatcher(
            ZooKeeperClient zooKeeperClient,
            String seqNodeRoot,
            String seqNodePrefix,
            long changeExpirationMs,
            NotificationHandler notificationHandler,
            Clock clock) {

        this.curatorCache = CuratorCache.build(zooKeeperClient.getCuratorClient(), seqNodeRoot);
        this.notificationHandler = notificationHandler;
        this.zooKeeperClient = zooKeeperClient;
        this.seqNodeRoot = seqNodeRoot;
        this.seqNodePrefix = seqNodePrefix;
        this.changeExpirationMs = changeExpirationMs;
        this.clock = clock;
        this.curatorCache
                .listenable()
                .addListener(
                        CuratorCacheListener.builder()
                                .forPathChildrenCache(
                                        seqNodeRoot,
                                        zooKeeperClient.getCuratorClient(),
                                        new ZkNodeChangeNotificationListener())
                                .build());
    }

    public void start() {
        running = true;
        // Initialize notifications by reading existing notification entries from ZooKeeper.
        // This ensures that any pending notifications are processed when the watcher starts
        processNotifications();
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

    private void processNotifications() {
        synchronized (lock) {
            try {
                List<String> notifications = zooKeeperClient.getChildren(seqNodeRoot);
                Collections.sort(notifications);
                if (!notifications.isEmpty()) {
                    long now = clock.milliseconds();
                    for (String notification : notifications) {
                        long changeId = changeNumber(notification);
                        if (changeId > lastExecutedChange) {
                            processNotifications(notification);
                            lastExecutedChange = changeId;
                        }
                    }
                    purgeObsoleteNotifications(now, notifications);
                }

            } catch (Exception e) {
                LOG.error(
                        "Error while processing notification change for path = {}", seqNodeRoot, e);
            }
        }
    }

    private void processNotifications(String notification) {
        String notificationNode = seqNodeRoot + "/" + notification;
        try {
            Optional<byte[]> data = zooKeeperClient.getOrEmpty(notificationNode);
            if (data.isPresent()) {
                notificationHandler.processNotification(data.get());
            }
        } catch (Exception e) {
            LOG.error(
                    "Error while processing notification change for path = {}",
                    notificationNode,
                    e);
        }
    }

    /** Purges expired notifications. */
    private void purgeObsoleteNotifications(long now, List<String> sortedNotifications) {
        for (String notification : sortedNotifications) {
            String notificationNode = seqNodeRoot + "/" + notification;
            try {
                Optional<Stat> state = zooKeeperClient.getStat(notificationNode);
                if (state.isPresent() && now - state.get().getCtime() >= changeExpirationMs) {
                    LOG.debug("Purging change notification {}", notificationNode);
                    zooKeeperClient.deletePath(notificationNode);
                }
            } catch (Exception e) {
                LOG.error(
                        "Error while purging obsolete notification change for path = {}",
                        notificationNode,
                        e);
            }
        }
    }

    private long changeNumber(String name) {
        return Long.parseLong(name.substring(seqNodePrefix.length()));
    }

    private final class ZkNodeChangeNotificationListener implements PathChildrenCacheListener {

        @Override
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) {
            if (CHILD_ADDED.equals(event.getType())) {
                processNotifications();
            }
        }
    }

    /**
     * NotificationHandler defines the contract for processing notifications received from
     * ZooKeeper.
     *
     * <p>This interface is implemented by classes that handle specific notification events, such as
     * ACL changes or other state updates in ZooKeeper.
     */
    public interface NotificationHandler {
        void processNotification(byte[] notification) throws Exception;
    }
}
