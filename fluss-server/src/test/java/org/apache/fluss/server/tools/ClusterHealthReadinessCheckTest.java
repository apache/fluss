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

package org.apache.fluss.server.tools;

import org.apache.fluss.exception.NotCoordinatorLeaderException;
import org.apache.fluss.exception.UnsupportedVersionException;
import org.apache.fluss.rpc.messages.GetClusterHealthResponse;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ClusterHealthReadinessCheck} coordinator-role evaluation. */
class ClusterHealthReadinessCheckTest {

    @Test
    void testCoordinatorLeaderIsReadyRegardlessOfClusterColor() {
        for (int status = 0; status <= 3; status++) {
            GetClusterHealthResponse resp = emptyResponse(status);
            resp.setIsLeader(true).setLeaderElected(true);
            assertThat(ClusterHealthReadinessCheck.evaluateCoordinator(resp))
                    .as("leader with status %d", status)
                    .isEqualTo(ClusterHealthReadinessCheck.EXIT_READY);
        }
    }

    @Test
    void testCoordinatorStandbyReadyWhenLeaderElected() {
        GetClusterHealthResponse resp = emptyResponse(3 /* UNKNOWN */);
        resp.setIsLeader(false).setLeaderElected(true);
        assertThat(ClusterHealthReadinessCheck.evaluateCoordinator(resp))
                .isEqualTo(ClusterHealthReadinessCheck.EXIT_READY);
    }

    @Test
    void testCoordinatorStandbyNotReadyWithoutElectedLeader() {
        GetClusterHealthResponse resp = emptyResponse(3 /* UNKNOWN */);
        resp.setIsLeader(false).setLeaderElected(false);
        assertThat(ClusterHealthReadinessCheck.evaluateCoordinator(resp))
                .isEqualTo(ClusterHealthReadinessCheck.EXIT_NOT_READY);
    }

    @Test
    void testCoordinatorStandbyWithoutLeaderElectedFieldIsNotReady() {
        GetClusterHealthResponse resp = emptyResponse(3 /* UNKNOWN */);
        resp.setIsLeader(false);
        assertThat(ClusterHealthReadinessCheck.evaluateCoordinator(resp))
                .isEqualTo(ClusterHealthReadinessCheck.EXIT_NOT_READY);
    }

    @Test
    void testCoordinatorResponseWithoutRoleFieldsIsLeaderServed() {
        // An old leader answers without the role fields (an old standby rejects the RPC
        // instead), so a response without is_leader must be treated as leader-served.
        GetClusterHealthResponse resp = emptyResponse(1 /* YELLOW */);
        assertThat(ClusterHealthReadinessCheck.evaluateCoordinator(resp))
                .isEqualTo(ClusterHealthReadinessCheck.EXIT_READY);
    }

    @Test
    void testInvalidRoleIsConfigError() {
        assertThat(ClusterHealthReadinessCheck.run(new String[] {"--role", "bogus"}))
                .isEqualTo(ClusterHealthReadinessCheck.EXIT_ERROR);
    }

    @Test
    void testTabletEvaluationRequiresGreen() {
        assertThat(ClusterHealthReadinessCheck.evaluate(emptyResponse(0 /* GREEN */)))
                .isEqualTo(ClusterHealthReadinessCheck.EXIT_READY);
        for (int status = 1; status <= 3; status++) {
            assertThat(ClusterHealthReadinessCheck.evaluate(emptyResponse(status)))
                    .as("status %d", status)
                    .isEqualTo(ClusterHealthReadinessCheck.EXIT_NOT_READY);
        }
    }

    @Test
    void testMapExecutionFailure() {
        assertThat(
                        ClusterHealthReadinessCheck.mapExecutionFailure(
                                new UnsupportedVersionException("old server"), false, "host:9124"))
                .isEqualTo(ClusterHealthReadinessCheck.EXIT_API_UNSUPPORTED);
        // an old standby coordinator rejects the RPC — coordinator role maps it to the
        // API-unsupported TCP fallback so the caller does not wait on it forever
        assertThat(
                        ClusterHealthReadinessCheck.mapExecutionFailure(
                                new NotCoordinatorLeaderException("standby"), true, "host:9124"))
                .isEqualTo(ClusterHealthReadinessCheck.EXIT_API_UNSUPPORTED);
        // in the tablet role the same rejection is a generic not-ready
        assertThat(
                        ClusterHealthReadinessCheck.mapExecutionFailure(
                                new NotCoordinatorLeaderException("standby"), false, "host:9124"))
                .isEqualTo(ClusterHealthReadinessCheck.EXIT_NOT_READY);
        assertThat(
                        ClusterHealthReadinessCheck.mapExecutionFailure(
                                new RuntimeException("boom"), true, "host:9124"))
                .isEqualTo(ClusterHealthReadinessCheck.EXIT_NOT_READY);
    }

    private static GetClusterHealthResponse emptyResponse(int status) {
        GetClusterHealthResponse resp = new GetClusterHealthResponse();
        resp.setNumReplicas(0);
        resp.setInSyncReplicas(0);
        resp.setNumLeaderReplicas(0);
        resp.setActiveLeaderReplicas(0);
        resp.setStatus(status);
        return resp;
    }
}
