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

package org.apache.fluss.server.authorizer;

import org.apache.fluss.security.acl.Resource;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.utils.clock.ManualClock;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ZkNodeChangeNotificationWatcher }. */
public class ZkNodeChangeNotificationWatcherTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    @Test
    void testZkNodeChangeNotifications() throws Exception {
        String seqNodeRoot = ZkData.AclChangesNode.path();
        String seqNodePrefix = ZkData.AclChangeNotificationNode.prefix();
        ZooKeeperClient zookeeperClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
        TestingNotificationHandler handler = new TestingNotificationHandler();

        long startTime = System.currentTimeMillis();
        ManualClock clock = new ManualClock(startTime);
        // Step 1: Insert initial notifications before starting the watcher
        ArrayList<Resource> expectedRegistrations =
                new ArrayList<>(
                        Arrays.asList(
                                Resource.cluster(),
                                Resource.database("test_database1"),
                                Resource.table("test_database2", "test_table")));
        for (Resource resource : expectedRegistrations) {
            zookeeperClient.insertAclChangeNotification(resource);
        }

        ZkNodeChangeNotificationWatcher aclChangeWatcher =
                new ZkNodeChangeNotificationWatcher(
                        zookeeperClient,
                        seqNodeRoot,
                        seqNodePrefix,
                        Duration.ofMinutes(5).toMillis(),
                        handler,
                        clock);
        aclChangeWatcher.start();
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(handler.resourceNotifications)
                                .containsExactlyInAnyOrderElementsOf(expectedRegistrations));
        // Verify that all initial notifications are processed
        List<String> nodesBeforeStart = zookeeperClient.getChildren(seqNodeRoot);
        assertThat(nodesBeforeStart).hasSize(3);

        // Step 2: Insert a new notification after the watcher has started
        Resource newNoticedResource = Resource.table("test_database3", "test_table");
        zookeeperClient.insertAclChangeNotification(newNoticedResource);
        expectedRegistrations.add(newNoticedResource);
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(handler.resourceNotifications)
                                .containsExactlyInAnyOrderElementsOf(expectedRegistrations));
        // Verify that the new notification is processed
        List<String> nodesAfterStart = zookeeperClient.getChildren(ZkData.AclChangesNode.path());
        assertThat(nodesAfterStart).hasSize(4);

        // Step 3: Test purging of obsolete notifications
        long maxCtimeBeforeStart = 0;
        for (String node : nodesBeforeStart) {
            maxCtimeBeforeStart =
                    Math.max(
                            zookeeperClient.getStat(seqNodeRoot + "/" + node).get().getCtime(),
                            maxCtimeBeforeStart);
        }
        // Advance the clock to make the initial notifications obsolete
        clock.advanceTime(
                maxCtimeBeforeStart - startTime + Duration.ofMinutes(5).toMillis(),
                TimeUnit.MILLISECONDS);
        // Insert a new notification to trigger the purging of obsolete notifications.
        zookeeperClient.insertAclChangeNotification(newNoticedResource);
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(zookeeperClient.getChildren(seqNodeRoot)).hasSize(2));
    }

    @Test
    void testNotificationDeletedByAnotherWatcherDuringPurge() throws Exception {
        String seqNodeRoot = ZkData.AclChangesNode.path();
        String seqNodePrefix = ZkData.AclChangeNotificationNode.prefix();
        ZooKeeperClient zooKeeperClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
        Resource resource = Resource.cluster();
        zooKeeperClient.insertAclChangeNotification(resource);

        TestingNotificationHandler handler =
                new TestingNotificationHandler() {
                    @Override
                    public void processNotification(byte[] notification) {
                        super.processNotification(notification);
                        try {
                            for (String child : zooKeeperClient.getChildren(seqNodeRoot)) {
                                zooKeeperClient.deletePath(seqNodeRoot + "/" + child);
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                };
        ZkNodeChangeNotificationWatcher watcher =
                new ZkNodeChangeNotificationWatcher(
                        zooKeeperClient,
                        seqNodeRoot,
                        seqNodePrefix,
                        Duration.ofMinutes(5).toMillis(),
                        handler,
                        new ManualClock());

        TestingAppender appender = new TestingAppender();
        appender.start();
        LoggerContext loggerContext = (LoggerContext) LogManager.getContext(false);
        Configuration configuration = loggerContext.getConfiguration();
        LoggerConfig loggerConfig =
                new LoggerConfig(
                        ZkNodeChangeNotificationWatcher.class.getName(), Level.DEBUG, false);
        loggerConfig.addAppender(appender, Level.DEBUG, null);
        configuration.addLogger(loggerConfig.getName(), loggerConfig);
        loggerContext.updateLoggers();
        try {
            watcher.start();
            assertThat(handler.resourceNotifications).containsExactly(resource);
            assertThat(appender.messages)
                    .anyMatch(
                            message ->
                                    message.startsWith(
                                                    "Notification "
                                                            + seqNodeRoot
                                                            + "/"
                                                            + seqNodePrefix)
                                            && message.endsWith(
                                                    " has already been purged by another watcher"));
        } finally {
            watcher.stop();
            configuration.removeLogger(loggerConfig.getName());
            loggerContext.updateLoggers();
            appender.stop();
        }
    }

    private static class TestingNotificationHandler
            implements ZkNodeChangeNotificationWatcher.NotificationHandler {
        public BlockingQueue<Resource> resourceNotifications = new LinkedBlockingQueue<>();

        @Override
        public void processNotification(byte[] notification) {
            Resource resource = ZkData.AclChangeNotificationNode.decode(notification);
            resourceNotifications.add(resource);
        }
    }

    private static class TestingAppender extends AbstractAppender {
        private final List<String> messages = new ArrayList<>();

        private TestingAppender() {
            super("testing", null, null, true, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(LogEvent event) {
            messages.add(event.getMessage().getFormattedMessage());
        }
    }
}
