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

import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.exception.NotCoordinatorLeaderException;
import org.apache.fluss.exception.UnsupportedVersionException;
import org.apache.fluss.rpc.messages.DescribeClusterResponse;
import org.apache.fluss.rpc.messages.GetClusterHealthResponse;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ClusterHealthReadinessCheck}. */
class ClusterHealthReadinessCheckTest {

    private static final ServerNode ADVERTISED =
            ClusterHealthReadinessCheck.parseCoordinators("coordinator-1.fluss:9124").get(0);

    @Test
    void testStandbyReadyWhenListedByLeader() {
        DescribeClusterResponse resp = new DescribeClusterResponse();
        resp.addCoordinatorStandby().setNodeId(0).setHost("coordinator-2.fluss").setPort(9124);
        // host names compare case-insensitively
        resp.addCoordinatorStandby().setNodeId(0).setHost("Coordinator-1.FLUSS").setPort(9124);
        assertThat(ClusterHealthReadinessCheck.evaluateStandby(resp, ADVERTISED, "leader:9124"))
                .isEqualTo(ClusterHealthReadinessCheck.EXIT_READY);
    }

    @Test
    void testStandbyNotReadyWhenNotListedByLeader() {
        DescribeClusterResponse resp = new DescribeClusterResponse();
        assertThat(ClusterHealthReadinessCheck.evaluateStandby(resp, ADVERTISED, "leader:9124"))
                .isEqualTo(ClusterHealthReadinessCheck.EXIT_NOT_READY);

        // same host on another port is a different coordinator
        resp.addCoordinatorStandby().setNodeId(0).setHost("coordinator-1.fluss").setPort(9123);
        assertThat(ClusterHealthReadinessCheck.evaluateStandby(resp, ADVERTISED, "leader:9124"))
                .isEqualTo(ClusterHealthReadinessCheck.EXIT_NOT_READY);
    }

    @Test
    void testParseCoordinators() {
        List<ServerNode> nodes =
                ClusterHealthReadinessCheck.parseCoordinators(" a.fluss:9124, ,b.fluss:9125 ");
        assertThat(nodes).extracting(ServerNode::host).containsExactly("a.fluss", "b.fluss");
        assertThat(nodes).extracting(ServerNode::port).containsExactly(9124, 9125);
        assertThat(ClusterHealthReadinessCheck.parseCoordinators(null)).isEmpty();

        assertThatThrownBy(() -> ClusterHealthReadinessCheck.parseCoordinators("a.fluss"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ClusterHealthReadinessCheck.parseCoordinators("a.fluss:x"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testInvalidArgumentsAreConfigErrors() {
        assertThat(ClusterHealthReadinessCheck.run(new String[] {"--role", "bogus"}))
                .isEqualTo(ClusterHealthReadinessCheck.EXIT_ERROR);
        assertThat(
                        ClusterHealthReadinessCheck.run(
                                new String[] {"--role", "coordinator", "--coordinators", "a"}))
                .isEqualTo(ClusterHealthReadinessCheck.EXIT_ERROR);
        assertThat(
                        ClusterHealthReadinessCheck.run(
                                new String[] {"--role", "coordinator", "--advertisedAddress", "a"}))
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
                                new UnsupportedVersionException("old server"), "host:9124"))
                .isEqualTo(ClusterHealthReadinessCheck.EXIT_API_UNSUPPORTED);
        assertThat(
                        ClusterHealthReadinessCheck.mapExecutionFailure(
                                new NotCoordinatorLeaderException("standby"), "host:9124"))
                .isEqualTo(ClusterHealthReadinessCheck.EXIT_NOT_READY);
        assertThat(
                        ClusterHealthReadinessCheck.mapExecutionFailure(
                                new RuntimeException("boom"), "host:9124"))
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
