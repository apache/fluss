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

package com.alibaba.fluss.kafka;

import com.alibaba.fluss.rpc.TestingTabletGatewayService;

import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KafkaRequestHandler}. */
public class KafkaRequestHandlerTest {

    @Test
    public void testKafkaApiVersionsNotSupported() {
        KafkaRequestHandler handler = createKafkaRequestHandler();
        short latestVersion = ApiKeys.API_VERSIONS.latestVersion();
        ApiVersionsRequest apiVersionsRequest =
                new ApiVersionsRequest.Builder().build(latestVersion);
        ChannelHandlerContext ctx = new TestingChannelHandlerContext();
        KafkaRequest request =
                new KafkaRequest(
                        ApiKeys.API_VERSIONS,
                        (short) (latestVersion + 1), // unsupported version
                        new RequestHeader(ApiKeys.API_VERSIONS, latestVersion, "client-id", 0),
                        apiVersionsRequest,
                        ByteBufAllocator.DEFAULT.buffer(),
                        ctx,
                        new CompletableFuture<>());
        handler.handleApiVersionsRequest(request);

        ByteBuf responseBuffer = request.responseBuffer();
        ApiVersionsResponse response =
                (ApiVersionsResponse)
                        AbstractResponse.parseResponse(
                                responseBuffer.nioBuffer(), request.header());
        Map<Errors, Integer> errorCounts = response.errorCounts();
        assertThat(1).isEqualTo(errorCounts.size());
        assertThat(1).isEqualTo(errorCounts.get(Errors.UNSUPPORTED_VERSION));
    }

    @Test
    public void testKafkaApiVersionsRequest() {
        KafkaRequestHandler handler = createKafkaRequestHandler();
        short latestVersion = ApiKeys.API_VERSIONS.latestVersion();
        ApiVersionsRequest apiVersionsRequest =
                new ApiVersionsRequest.Builder().build(latestVersion);
        ChannelHandlerContext ctx = new TestingChannelHandlerContext();
        KafkaRequest request =
                new KafkaRequest(
                        ApiKeys.API_VERSIONS,
                        latestVersion,
                        new RequestHeader(ApiKeys.API_VERSIONS, latestVersion, "client-id", 0),
                        apiVersionsRequest,
                        ByteBufAllocator.DEFAULT.buffer(),
                        ctx,
                        new CompletableFuture<>());
        handler.handleApiVersionsRequest(request);

        ByteBuf responseBuffer = request.responseBuffer();
        ApiVersionsResponse response =
                (ApiVersionsResponse)
                        AbstractResponse.parseResponse(
                                responseBuffer.nioBuffer(), request.header());
        Map<Errors, Integer> errorCounts = response.errorCounts();
        assertThat(1).isEqualTo(errorCounts.size());
        assertThat(1).isEqualTo(errorCounts.get(Errors.NONE));
        response.data()
                .apiKeys()
                .forEach(
                        apiVersion -> {
                            if (ApiKeys.METADATA.id == apiVersion.apiKey()) {
                                assertThat((short) 11)
                                        .isGreaterThanOrEqualTo(apiVersion.maxVersion());
                            } else if (ApiKeys.FETCH.id == apiVersion.apiKey()) {
                                assertThat((short) 12)
                                        .isGreaterThanOrEqualTo(apiVersion.maxVersion());
                            } else {
                                ApiKeys apiKeys = ApiKeys.forId(apiVersion.apiKey());
                                assertThat(apiVersion.minVersion())
                                        .isEqualTo(apiKeys.oldestVersion());
                                assertThat(apiVersion.maxVersion())
                                        .isEqualTo(apiKeys.latestVersion());
                            }
                        });
    }

    private static KafkaRequestHandler createKafkaRequestHandler() {
        return new KafkaRequestHandler(new TestingTabletGatewayService());
    }
}
