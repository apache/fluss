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

import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.registry.NOPMetricRegistry;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.server.entity.UserContext;
import org.apache.fluss.server.metrics.group.AbstractUserMetricGroup;
import org.apache.fluss.utils.concurrent.FlussScheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.apache.fluss.server.metrics.group.TestingMetricGroups.TABLET_SERVER_METRICS;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link UserMetrics}. */
public class UserMetricsTest {

    private FlussScheduler scheduler;

    @BeforeEach
    void before() {
        scheduler = new FlussScheduler(2, false);
        scheduler.startup();
    }

    @AfterEach
    void after() throws InterruptedException {
        scheduler.shutdown();
    }

    @Test
    void testMetricExpiration() {
        // set expiration time to 2s in test, and 10ms check interval
        UserMetrics userMetrics =
                new UserMetrics(
                        Duration.ofSeconds(2).toMillis(),
                        100L,
                        scheduler,
                        NOPMetricRegistry.INSTANCE,
                        TABLET_SERVER_METRICS);

        String user1 = "user1";
        String user2 = "user2";
        UserContext uc1 = new UserContext(new FlussPrincipal(user1, "USER"));
        UserContext uc2 = new UserContext(new FlussPrincipal(user2, "USER"));

        TablePath t1 = TablePath.of("db1", "table1");
        TablePath t2 = TablePath.of("db1", "table2");

        userMetrics.incBytesIn(null, t1, 100);
        userMetrics.incBytesOut(null, t2, 200);
        userMetrics.incBytesOut(new UserContext(FlussPrincipal.ANY), t1, 200);
        userMetrics.incBytesIn(new UserContext(FlussPrincipal.ANONYMOUS), t1, 300);
        userMetrics.incBytesOut(new UserContext(FlussPrincipal.WILD_CARD_PRINCIPAL), t2, 100);

        // ignore null/anonymous/wildcard user
        assertThat(userMetrics.numMetrics()).isEqualTo(0);

        userMetrics.incBytesIn(uc1, t1, 100);
        userMetrics.incBytesOut(uc1, t2, 100);
        userMetrics.incBytesOut(uc2, t2, 200);

        // u1, u1+t1, u1+t2, u2, u2+t2
        assertThat(userMetrics.numMetrics()).isEqualTo(5);

        scheduler.schedule(
                "updating-u1t1",
                () -> {
                    userMetrics.incBytesIn(uc1, t1, 50);
                },
                0L,
                50L);

        AbstractUserMetricGroup u1t1 =
                userMetrics.getOrCreateMetric(new UserMetrics.MetricKey(user1, t1));
        AbstractUserMetricGroup u1t2 =
                userMetrics.getOrCreateMetric(new UserMetrics.MetricKey(user1, t2));
        AbstractUserMetricGroup u2t2 =
                userMetrics.getOrCreateMetric(new UserMetrics.MetricKey(user2, t2));
        AbstractUserMetricGroup u1 =
                userMetrics.getOrCreateMetric(new UserMetrics.MetricKey(user1, null));
        AbstractUserMetricGroup u2 =
                userMetrics.getOrCreateMetric(new UserMetrics.MetricKey(user2, null));

        // wait enough time for metrics expired
        retry(
                Duration.ofSeconds(10),
                () -> {
                    // only u1 and u1+t1 are retained.
                    assertThat(userMetrics.numMetrics()).isEqualTo(2);
                });

        assertThat(u1t2.isClosed()).isTrue();
        assertThat(u2t2.isClosed()).isTrue();
        assertThat(u2.isClosed()).isTrue();

        assertThat(u1.isClosed()).isFalse();
        assertThat(u1t1.isClosed()).isFalse();
        assertThat(userMetrics.getOrCreateMetric(new UserMetrics.MetricKey(user1, null)))
                .isSameAs(u1);
        assertThat(userMetrics.getOrCreateMetric(new UserMetrics.MetricKey(user1, t1)))
                .isSameAs(u1t1);
    }
}
