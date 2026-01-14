/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.fluss.server.metrics;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.groups.AbstractMetricGroup;
import org.apache.fluss.metrics.registry.MetricRegistry;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.server.entity.UserContext;
import org.apache.fluss.server.metrics.group.AbstractUserMetricGroup;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.server.metrics.group.UserMetricGroup;
import org.apache.fluss.server.metrics.group.UserPerTableMetricGroup;
import org.apache.fluss.utils.MapUtils;
import org.apache.fluss.utils.concurrent.Scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Designed to manage and cleanup user-level metrics (will be used for future quota management).
 *
 * <p>To be more specific, Fluss server maintains three kind of metrics:
 *
 * <ul>
 *   <li>1. Server-level metrics which is never expired or dropped, such as "tableCount".
 *   <li>2. Table-level metrics which is dropped when the table is deleted, such as
 *       "bytesInPerSecond" for a table.
 *   <li>3. User-level metrics which will be expired and dropped after a period of inactivity, such
 *       as "bytesInRate" for a user.
 * </ul>
 *
 * <p>This class mainly manages the user-level metrics. There are many a lot of users to read/write
 * tables, but most of them are idle after a period of time. To avoid memory leak or GC overhead, we
 * need to clean up those inactive user-level metrics periodically.
 */
public class UserMetrics implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(UserMetrics.class);
    private static final long INACTIVE_METRIC_EXPIRATION_TIME_MS = 3600_000L; // 1 hour
    private static final long METRICS_CLEANUP_INTERVAL_MS = 30_000L; // 30s

    private final long inactiveMetricExpirationTimeMs;
    private final MetricRegistry metricRegistry;
    private final TabletServerMetricGroup parentMetricGroup;
    private final ScheduledFuture<?> schedule;

    private final ConcurrentMap<MetricKey, AbstractUserMetricGroup> metrics =
            MapUtils.newConcurrentHashMap();
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    public UserMetrics(
            Scheduler cleanupScheduler,
            MetricRegistry metricRegistry,
            TabletServerMetricGroup parentMetricGroup) {
        this(
                INACTIVE_METRIC_EXPIRATION_TIME_MS,
                METRICS_CLEANUP_INTERVAL_MS,
                cleanupScheduler,
                metricRegistry,
                parentMetricGroup);
    }

    @VisibleForTesting
    UserMetrics(
            long inactiveMetricExpirationTimeMs,
            long cleanupIntervalMs,
            Scheduler cleanupScheduler,
            MetricRegistry metricRegistry,
            TabletServerMetricGroup parentMetricGroup) {
        this.inactiveMetricExpirationTimeMs = inactiveMetricExpirationTimeMs;
        this.metricRegistry = metricRegistry;
        this.parentMetricGroup = parentMetricGroup;
        this.schedule =
                cleanupScheduler.schedule(
                        "user-metrics-expired-cleanup-task",
                        new ExpiredMetricCleanupTask(),
                        cleanupIntervalMs,
                        cleanupIntervalMs);
    }

    protected AbstractUserMetricGroup getOrCreateMetric(MetricKey metricKey) {
        return metrics.computeIfAbsent(
                metricKey,
                key -> {
                    if (metricKey.tablePath != null) {
                        return new UserPerTableMetricGroup(
                                metricRegistry,
                                key.userName,
                                key.tablePath,
                                inactiveMetricExpirationTimeMs,
                                parentMetricGroup);
                    } else {
                        return new UserMetricGroup(
                                metricRegistry,
                                key.userName,
                                inactiveMetricExpirationTimeMs,
                                parentMetricGroup);
                    }
                });
    }

    /** Increments the number of bytes written by a user on a specific table. */
    public void incBytesIn(@Nullable UserContext userContext, TablePath tablePath, long numBytes) {
        incBytes(userContext, tablePath, numBytes, true);
    }

    /** Increments the number of bytes read by a user on a specific table. */
    public void incBytesOut(@Nullable UserContext userContext, TablePath tablePath, long numBytes) {
        incBytes(userContext, tablePath, numBytes, false);
    }

    private void incBytes(
            @Nullable UserContext userContext,
            TablePath tablePath,
            long numBytes,
            boolean bytesIn) {
        if (userContext == null
                || userContext.getPrincipal() == FlussPrincipal.ANY
                || userContext.getPrincipal() == FlussPrincipal.WILD_CARD_PRINCIPAL
                || userContext.getPrincipal() == FlussPrincipal.ANONYMOUS) {
            // Ignore null or anonymous or wildcard users
            return;
        }
        String userName = userContext.getPrincipal().getName();
        AbstractUserMetricGroup user = getOrCreateMetric(new MetricKey(userName, null));
        AbstractUserMetricGroup perTable = getOrCreateMetric(new MetricKey(userName, tablePath));
        if (bytesIn) {
            user.incBytesIn(numBytes);
            perTable.incBytesIn(numBytes);
        } else {
            user.incBytesOut(numBytes);
            perTable.incBytesOut(numBytes);
        }
    }

    @VisibleForTesting
    int numMetrics() {
        return metrics.size();
    }

    @Override
    public void close() throws Exception {
        if (isClosed.compareAndSet(false, true)) {
            schedule.cancel(true);
            for (AbstractMetricGroup metricGroup : metrics.values()) {
                metricGroup.close();
            }
            metrics.clear();
        }
    }

    /** A periodic task to clean up expired user metrics. */
    private class ExpiredMetricCleanupTask implements Runnable {

        @Override
        public void run() {
            for (Map.Entry<MetricKey, AbstractUserMetricGroup> metricEntry : metrics.entrySet()) {
                MetricKey metricName = metricEntry.getKey();
                AbstractUserMetricGroup userMetric = metricEntry.getValue();
                synchronized (userMetric) {
                    if (userMetric.hasExpired()) {
                        LOG.debug("Removing expired user metric [{}]", metricName);
                        metrics.remove(metricName);
                        userMetric.close();
                    }
                }
            }
        }
    }

    /** The key to identify user metrics. */
    protected static class MetricKey {
        final String userName;
        @Nullable final TablePath tablePath;

        MetricKey(String userName, @Nullable TablePath tablePath) {
            this.userName = userName;
            this.tablePath = tablePath;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MetricKey metricKey = (MetricKey) o;
            return Objects.equals(userName, metricKey.userName)
                    && Objects.equals(tablePath, metricKey.tablePath);
        }

        @Override
        public int hashCode() {
            return Objects.hash(userName, tablePath);
        }

        @Override
        public String toString() {
            if (tablePath == null) {
                return userName;
            } else {
                return userName + ":" + tablePath;
            }
        }
    }
}
