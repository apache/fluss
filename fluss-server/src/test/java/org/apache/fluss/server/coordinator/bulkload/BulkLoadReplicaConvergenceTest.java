/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.coordinator.bulkload;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.exception.InvalidBulkLoadRequestException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.messages.PbNotifyLeaderAndIsrRespForBucket;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.coordinator.CoordinatorContext;
import org.apache.fluss.server.coordinator.CoordinatorTestUtils;
import org.apache.fluss.server.coordinator.bulkload.BulkLoadMetadataStore.Versioned;
import org.apache.fluss.server.coordinator.bulkload.BulkLoadReplicaConvergence.ReplicaConvergenceContext;
import org.apache.fluss.server.metadata.RegisteredTabletServer;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.metadata.TabletServerResource;
import org.apache.fluss.server.zk.ZkEpoch;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.ServerProtocolVersion;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TabletServerRegistration;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.utils.ExceptionUtils;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for the per-bucket failure translation of {@link BulkLoadReplicaConvergence}. */
class BulkLoadReplicaConvergenceTest {

    @Test
    void testReRegisteredServerRequiresItsOwnGatewayBinding() {
        int serverId = 1;
        CoordinatorContext context = new CoordinatorContext(ZkEpoch.INITIAL_EPOCH);
        TabletServerRegistration registration =
                new TabletServerRegistration(
                        "rack",
                        Collections.singletonList(new Endpoint("localhost", 20001, "INTERNAL")),
                        1L,
                        TabletServerResource.unknown(),
                        ServerProtocolVersion.CURRENT);
        context.setLiveTabletServers(
                Collections.singletonList(
                        new ServerInfo(
                                serverId,
                                "rack",
                                registration.getEndpoints(),
                                ServerType.TABLET_SERVER)));
        RegisteredTabletServer original =
                new RegisteredTabletServer(
                        serverId, registration, ZkData.ServerIdZNode.path(serverId), 0, 11L);
        RegisteredTabletServer replacement =
                new RegisteredTabletServer(
                        serverId, registration, ZkData.ServerIdZNode.path(serverId), 0, 12L);
        CoordinatorTestUtils.bindTabletServerRegistration(context, original);
        Versioned<TableAssignment> assignment =
                new Versioned<>(
                        TableAssignment.builder().add(0, BucketAssignment.of(serverId)).build(),
                        "/assignment",
                        0,
                        0L);
        ReplicaConvergenceContext captured =
                new ReplicaConvergenceContext(
                        assignment, Collections.singletonList(original), Collections.emptyMap());
        assertThat(
                        captured.matchesRoundFacts(
                                assignment, context, Collections.singletonList(original)))
                .isTrue();

        assertThat(
                        BulkLoadReplicaConvergence.liveRecipients(
                                context, Collections.singletonList(replacement)))
                .isEmpty();
        assertThat(
                        captured.matchesRoundFacts(
                                assignment, context, Collections.singletonList(replacement)))
                .isFalse();

        CoordinatorTestUtils.bindTabletServerRegistration(context, replacement);

        assertThat(
                        BulkLoadReplicaConvergence.liveRecipients(
                                context, Collections.singletonList(replacement)))
                .containsExactly(replacement);
        assertThat(
                        captured.matchesRoundFacts(
                                assignment, context, Collections.singletonList(replacement)))
                .isFalse();
    }

    @Test
    void testRejectedBucketKeepsItsErrorType() {
        TableBucket bucket = new TableBucket(1L, 3);
        PbNotifyLeaderAndIsrRespForBucket rejected = new PbNotifyLeaderAndIsrRespForBucket();
        rejected.setTableBucket().setTableId(bucket.getTableId()).setBucketId(bucket.getBucket());
        rejected.setError(
                Errors.INVALID_BULK_LOAD_REQUEST.code(),
                "BulkLoad target contains local data: " + bucket);

        Throwable failure = BulkLoadReplicaConvergence.notifyFailure(bucket, rejected);

        // The Coordinator decides TARGET_NOT_EMPTY by finding this type in the failure chain, so a
        // rejection that arrives as an untyped wrapper would silently become an ordinary failure.
        assertThat(
                        ExceptionUtils.findThrowable(failure, InvalidBulkLoadRequestException.class)
                                .isPresent())
                .as("an empty-target rejection must stay recognisable to the abort decision")
                .isTrue();
        assertThat(failure.getMessage()).contains(bucket.toString());
    }

    @Test
    void testRejectedBucketWithoutErrorCodeStaysUntyped() {
        TableBucket bucket = new TableBucket(1L, 0);
        PbNotifyLeaderAndIsrRespForBucket missing = new PbNotifyLeaderAndIsrRespForBucket();
        missing.setTableBucket().setTableId(bucket.getTableId()).setBucketId(bucket.getBucket());

        Throwable failure = BulkLoadReplicaConvergence.notifyFailure(bucket, missing);

        assertThat(
                        ExceptionUtils.findThrowable(failure, InvalidBulkLoadRequestException.class)
                                .isPresent())
                .isFalse();
        assertThat(failure.getMessage()).contains(bucket.toString());
    }
}
