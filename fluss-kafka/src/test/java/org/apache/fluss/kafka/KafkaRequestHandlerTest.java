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

package org.apache.fluss.kafka;

import org.apache.fluss.rpc.TestingTabletGatewayService;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

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
                newRequest(
                        ApiKeys.API_VERSIONS,
                        (short) (latestVersion + 1), // unsupported version
                        new RequestHeader(ApiKeys.API_VERSIONS, latestVersion, "client-id", 0),
                        apiVersionsRequest,
                        ctx);
        handler.processRequest(request);

        ApiVersionsResponse response = (ApiVersionsResponse) parseResponse(request);
        Map<Errors, Integer> errorCounts = response.errorCounts();
        assertThat(1).isEqualTo(errorCounts.size());
        assertThat(1).isEqualTo(errorCounts.get(Errors.UNSUPPORTED_VERSION));
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1, 2, 3, 4})
    public void testKafkaApiVersionsRequest(short version) {
        KafkaRequestHandler handler = createKafkaRequestHandler();
        ApiVersionsResponse response = requestApiVersions(handler, version);

        assertSuccessfulResponseDefaults(response);
        assertBrokerCapabilities(response);
    }

    private static ApiVersionsResponse requestApiVersions(
            KafkaRequestHandler handler, short version) {
        ApiVersionsRequest apiVersionsRequest = new ApiVersionsRequest.Builder().build(version);
        ChannelHandlerContext ctx = new TestingChannelHandlerContext();
        KafkaRequest request =
                newRequest(
                        ApiKeys.API_VERSIONS,
                        version,
                        new RequestHeader(ApiKeys.API_VERSIONS, version, "client-id", 0),
                        apiVersionsRequest,
                        ctx);
        handler.processRequest(request);

        return parseApiVersionsResponse(request);
    }

    private static ApiVersionsResponse parseApiVersionsResponse(KafkaRequest request) {
        return (ApiVersionsResponse) parseResponse(request);
    }

    private static void assertSuccessfulResponseDefaults(ApiVersionsResponse response) {
        assertThat(response.errorCounts())
                .containsExactlyEntriesOf(Collections.singletonMap(Errors.NONE, 1));
        assertThat(response.data().throttleTimeMs()).isZero();
        assertThat(response.data().supportedFeatures()).isEmpty();
        assertThat(response.data().finalizedFeaturesEpoch()).isEqualTo(-1L);
        assertThat(response.data().finalizedFeatures()).isEmpty();
        assertThat(response.data().zkMigrationReady()).isFalse();
    }

    private static void assertBrokerCapabilities(ApiVersionsResponse response) {
        assertThat(response.data().apiKeys())
                .extracting(ApiVersion::apiKey, ApiVersion::minVersion, ApiVersion::maxVersion)
                .containsExactly(
                        tuple(ApiKeys.METADATA.id, ApiKeys.METADATA.oldestVersion(), (short) 11),
                        tuple(
                                ApiKeys.API_VERSIONS.id,
                                ApiKeys.API_VERSIONS.oldestVersion(),
                                ApiKeys.API_VERSIONS.latestVersion()));
    }

    @Test
    public void testInvalidApiVersionsRequest() {
        KafkaRequestHandler handler = createKafkaRequestHandler();
        short latestVersion = ApiKeys.API_VERSIONS.latestVersion();
        ApiVersionsRequest requestBody =
                new ApiVersionsRequest.Builder(
                                new ApiVersionsRequestData()
                                        .setClientSoftwareName("invalid client name")
                                        .setClientSoftwareVersion("1.0"),
                                latestVersion,
                                latestVersion)
                        .build(latestVersion);
        KafkaRequest request =
                newRequest(
                        ApiKeys.API_VERSIONS,
                        latestVersion,
                        new RequestHeader(ApiKeys.API_VERSIONS, latestVersion, "client-id", 0),
                        requestBody,
                        new TestingChannelHandlerContext());

        handler.processRequest(request);

        ApiVersionsResponse response = (ApiVersionsResponse) parseResponse(request);
        assertThat(response.errorCounts()).containsEntry(Errors.INVALID_REQUEST, 1);
    }

    @Test
    public void testUnregisteredApiIsNotRouted() {
        KafkaRequestHandler handler = createKafkaRequestHandler();
        short version = ApiKeys.CREATE_TOPICS.latestVersion();
        CreateTopicsRequestData requestData =
                new CreateTopicsRequestData()
                        .setTimeoutMs(1000)
                        .setTopics(
                                new CreateTopicsRequestData.CreatableTopicCollection(
                                        Collections.singletonList(
                                                        new CreateTopicsRequestData.CreatableTopic()
                                                                .setName("topic")
                                                                .setNumPartitions(1)
                                                                .setReplicationFactor((short) 1))
                                                .iterator()));
        CreateTopicsRequest requestBody =
                new CreateTopicsRequest.Builder(requestData).build(version);
        KafkaRequest request =
                newRequest(
                        ApiKeys.CREATE_TOPICS,
                        version,
                        new RequestHeader(ApiKeys.CREATE_TOPICS, version, "client-id", 0),
                        requestBody,
                        new TestingChannelHandlerContext());

        handler.processRequest(request);

        CreateTopicsResponse response = (CreateTopicsResponse) parseResponse(request);
        assertThat(response.errorCounts()).containsEntry(Errors.UNSUPPORTED_VERSION, 1);
    }

    private static KafkaRequest newRequest(
            ApiKeys apiKey,
            short apiVersion,
            RequestHeader header,
            AbstractRequest requestBody,
            ChannelHandlerContext context) {
        ByteBuf requestBuffer = ByteBufAllocator.DEFAULT.buffer();
        try {
            return new KafkaRequest(
                    apiKey,
                    apiVersion,
                    header,
                    requestBody,
                    requestBuffer,
                    context,
                    new CompletableFuture<>());
        } finally {
            // Mirror KafkaCommandDecoder's ownership transfer to KafkaRequest.
            requestBuffer.release();
        }
    }

    private static AbstractResponse parseResponse(KafkaRequest request) {
        ByteBuf responseBuffer = request.responseBuffer();
        try {
            return AbstractResponse.parseResponse(responseBuffer.nioBuffer(), request.header());
        } finally {
            responseBuffer.release();
        }
    }

    private static KafkaRequestHandler createKafkaRequestHandler() {
        TestingTabletGatewayService service = new TestingTabletGatewayService();
        return new KafkaRequestHandler(service, service, "kafka");
    }
}
