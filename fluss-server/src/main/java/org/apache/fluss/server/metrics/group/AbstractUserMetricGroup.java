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

package org.apache.fluss.server.metrics.group;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.metrics.CharacterFilter;
import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.ThreadSafeSimpleCounter;
import org.apache.fluss.metrics.groups.AbstractMetricGroup;
import org.apache.fluss.metrics.registry.MetricRegistry;

import java.util.Map;

import static org.apache.fluss.metrics.utils.MetricGroupUtils.makeScope;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Abstract metric the users in server and tracks the expiration. */
public abstract class AbstractUserMetricGroup extends AbstractMetricGroup {
    private static final String NAME = "user";

    private final String principalName;
    private final long inactiveMetricExpirationTimeMs;
    protected final Counter bytesIn;
    protected final Counter bytesOut;

    /** The last record time for the group. */
    protected volatile long lastRecordTime;

    public AbstractUserMetricGroup(
            MetricRegistry registry,
            String principalName,
            long inactiveMetricExpirationTimeMs,
            TabletServerMetricGroup tabletServerMetricGroup) {
        super(registry, makeScope(tabletServerMetricGroup, principalName), tabletServerMetricGroup);
        this.principalName = checkNotNull(principalName);
        this.inactiveMetricExpirationTimeMs = inactiveMetricExpirationTimeMs;

        this.bytesIn = new ThreadSafeSimpleCounter();
        this.bytesOut = new ThreadSafeSimpleCounter();

        this.lastRecordTime = System.currentTimeMillis();
    }

    @Override
    protected String getGroupName(CharacterFilter filter) {
        return NAME;
    }

    @VisibleForTesting
    public String getPrincipalName() {
        return principalName;
    }

    @Override
    protected void putVariables(Map<String, String> variables) {
        variables.put("user", principalName);
    }

    public void incBytesIn(long numBytes) {
        this.lastRecordTime = System.currentTimeMillis();
        bytesIn.inc(numBytes);
    }

    public void incBytesOut(long numBytes) {
        this.lastRecordTime = System.currentTimeMillis();
        bytesOut.inc(numBytes);
    }

    /** Return true if the metric is eligible for removal due to inactivity. false otherwise. */
    public boolean hasExpired() {
        return (System.currentTimeMillis() - lastRecordTime) > this.inactiveMetricExpirationTimeMs;
    }
}
