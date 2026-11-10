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

import org.apache.fluss.exception.StaleMetadataException;
import org.apache.fluss.exception.TimeoutException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Snapshot restoration responses remain bound to their registered ZooKeeper session. */
class SnapshotRestoreAccessGateTest {
    @Test
    void testReconnectPreservesSessionAndInvalidatesPreviousRequests() {
        SnapshotRestoreAccessGate gate = new SnapshotRestoreAccessGate();
        gate.validationSucceeded(gate.beginValidation(), 17L);
        long previousEpoch = gate.captureAccessEpoch();
        assertThat(gate.getServerSessionId(previousEpoch)).isEqualTo(17L);
        gate.suspended();
        assertThatThrownBy(() -> gate.getServerSessionId(previousEpoch))
                .isInstanceOf(TimeoutException.class);
        gate.validationSucceeded(gate.beginValidation(), 17L);
        assertThatThrownBy(() -> gate.getServerSessionId(previousEpoch))
                .isInstanceOf(TimeoutException.class);
        assertThat(gate.getServerSessionId(gate.captureAccessEpoch())).isEqualTo(17L);
    }

    @Test
    void testDifferentSessionCannotRelabelResponses() {
        SnapshotRestoreAccessGate gate = new SnapshotRestoreAccessGate();
        gate.validationSucceeded(gate.beginValidation(), 17L);
        SnapshotRestoreAccessGate.ValidationToken token = gate.beginValidation();
        assertThatThrownBy(() -> gate.validationSucceeded(token, 18L))
                .isInstanceOf(StaleMetadataException.class);
        assertThatThrownBy(gate::captureAccessEpoch).isInstanceOf(TimeoutException.class);
        gate.lost();
        assertThatThrownBy(() -> gate.validationSucceeded(token, 17L))
                .isInstanceOf(StaleMetadataException.class);
        assertThatThrownBy(gate::beginValidation).isInstanceOf(StaleMetadataException.class);
    }
}
