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

package org.apache.fluss.server.tablet;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.exception.StaleMetadataException;

/** Process-wide fail-closed access gate with one monotonic invalidation epoch. */
@Internal
public final class TabletServerAccessGate {

    private long accessEpoch;
    private boolean enabled;
    private boolean permanentlyClosed;

    /** Immutable identity of one session-validation epoch. */
    public static final class ValidationToken {
        private final long accessEpoch;

        private ValidationToken(long accessEpoch) {
            this.accessEpoch = accessEpoch;
        }
    }

    /** Starts validation for an initial or reconnected ZooKeeper session. */
    public synchronized ValidationToken beginValidation() {
        if (permanentlyClosed) {
            throw stale("TabletServer access is permanently closed after ZooKeeper session loss.");
        }
        invalidate();
        return new ValidationToken(accessEpoch);
    }

    /** Closes access and invalidates every request captured before suspension. */
    public synchronized void suspended() {
        invalidate();
    }

    /** Permanently closes access after session loss. */
    public synchronized void lost() {
        permanentlyClosed = true;
        invalidate();
    }

    /** Enables access after validation succeeds for the current ZooKeeper session. */
    public synchronized void validationSucceeded(ValidationToken token) {
        ensureCurrent(token);
        enabled = true;
    }

    /** Captures the enabled access epoch at external request ingress. */
    public synchronized long captureAccessEpoch() {
        ensureEnabled(accessEpoch);
        return accessEpoch;
    }

    /** Rechecks the captured access epoch immediately before successful completion. */
    public synchronized void ensureEnabled(long expectedAccessEpoch) {
        if (expectedAccessEpoch != accessEpoch || !enabled) {
            throw stale("TabletServer access is disabled or its access epoch is stale.");
        }
    }

    private void invalidate() {
        accessEpoch++;
        enabled = false;
    }

    private void ensureCurrent(ValidationToken token) {
        if (permanentlyClosed || token.accessEpoch != accessEpoch) {
            throw stale("TabletServer validation token is stale.");
        }
    }

    private static StaleMetadataException stale(String message) {
        return new StaleMetadataException(message);
    }
}
