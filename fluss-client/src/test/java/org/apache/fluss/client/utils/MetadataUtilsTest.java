/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.client.utils;

import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.cluster.rebalance.ServerTag;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.UnsupportedVersionException;
import org.apache.fluss.rpc.messages.DescribeClusterResponse;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link MetadataUtils}. */
class MetadataUtilsTest {

    private final AtomicInteger fallbackCalls = new AtomicInteger();

    private final Supplier<CompletableFuture<String>> fallback =
            () -> {
                fallbackCalls.incrementAndGet();
                return CompletableFuture.completedFuture("fallback");
            };

    @Test
    void testNoFallbackWhenRequestSucceeds() throws Exception {
        CompletableFuture<String> result =
                MetadataUtils.fallbackIfUnsupported(
                        () -> CompletableFuture.completedFuture("request"), fallback);

        assertThat(result.get()).isEqualTo("request");
        assertThat(fallbackCalls).hasValue(0);
    }

    @Test
    void testFallbackWhenServerDoesNotSupportRequest() throws Exception {
        // as reported by the RPC client for a server that does not know the API
        CompletableFuture<String> unsupported = new CompletableFuture<>();
        unsupported.completeExceptionally(
                new CompletionException(
                        new UnsupportedVersionException(
                                "The server does not support DESCRIBE_CLUSTER")));

        assertThat(MetadataUtils.fallbackIfUnsupported(() -> unsupported, fallback).get())
                .isEqualTo("fallback");
        assertThat(fallbackCalls).hasValue(1);
    }

    @Test
    void testFallbackWhenRequestThrowsUnsupportedVersion() throws Exception {
        Supplier<CompletableFuture<String>> throwing =
                () -> {
                    throw new UnsupportedVersionException("unsupported");
                };

        assertThat(MetadataUtils.fallbackIfUnsupported(throwing, fallback).get())
                .isEqualTo("fallback");
        assertThat(fallbackCalls).hasValue(1);
    }

    @Test
    void testOtherFailuresAreNotHiddenByFallback() {
        CompletableFuture<String> failed = new CompletableFuture<>();
        failed.completeExceptionally(
                new CompletionException(new FlussRuntimeException("coordinator unavailable")));

        assertThatThrownBy(() -> MetadataUtils.fallbackIfUnsupported(() -> failed, fallback).get())
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOf(FlussRuntimeException.class)
                .hasMessage("coordinator unavailable");
        assertThat(fallbackCalls).hasValue(0);
    }

    @Test
    void testToServerNodes() {
        DescribeClusterResponse response = new DescribeClusterResponse();
        response.setCoordinatorServer().setNodeId(0).setHost("coordinator").setPort(9123);
        response.addTabletServer().setNodeId(1).setHost("ts-1").setPort(9124).setRack("rack-1");
        response.addTabletServer()
                .setNodeId(2)
                .setHost("ts-2")
                .setPort(9125)
                .setRack("rack-2")
                .setServerTag(ServerTag.PERMANENT_OFFLINE.value);
        // a tag added by a newer server version is reported as no tag
        response.addTabletServer().setNodeId(3).setHost("ts-3").setPort(9126).setServerTag(42);

        List<ServerNode> serverNodes = MetadataUtils.toServerNodes(response);

        assertThat(serverNodes)
                .containsExactly(
                        new ServerNode(0, "coordinator", 9123, ServerType.COORDINATOR),
                        new ServerNode(1, "ts-1", 9124, ServerType.TABLET_SERVER, "rack-1"),
                        new ServerNode(2, "ts-2", 9125, ServerType.TABLET_SERVER, "rack-2"),
                        new ServerNode(3, "ts-3", 9126, ServerType.TABLET_SERVER, null));
        assertThat(serverNodes)
                .extracting(ServerNode::serverTag)
                .containsExactly(null, null, ServerTag.PERMANENT_OFFLINE, null);
    }

    @Test
    void testToServerNodesWithoutCoordinator() {
        DescribeClusterResponse response = new DescribeClusterResponse();
        response.addTabletServer().setNodeId(1).setHost("ts-1").setPort(9124);

        assertThat(MetadataUtils.toServerNodes(response))
                .containsExactly(new ServerNode(1, "ts-1", 9124, ServerType.TABLET_SERVER));
    }
}
