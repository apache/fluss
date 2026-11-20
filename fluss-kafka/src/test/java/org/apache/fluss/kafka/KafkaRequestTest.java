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

import org.apache.fluss.kafka.security.KafkaSaslConnection;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link KafkaRequest}. */
public class KafkaRequestTest {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testPrincipalIsAnImmutableConnectionSnapshot(boolean closeConnection)
            throws Exception {
        FlussPrincipal principal = new FlussPrincipal("writer", "User");
        TestingSaslAuthenticator authenticator =
                new TestingSaslAuthenticator(principal, false, false);
        KafkaSaslConnection connection = KafkaSaslConnection.sasl(() -> authenticator);
        connection.beginAuthentication("PLAIN", "KAFKA", null);
        connection.authenticate(new byte[0]);
        assertThat(authenticator.closed).isTrue();
        assertThat(connection.isReady()).isTrue();
        ByteBuf buffer = Unpooled.buffer(1);
        short version = ApiKeys.API_VERSIONS.oldestVersion();
        KafkaRequest request =
                new KafkaRequest(
                        ApiKeys.API_VERSIONS,
                        version,
                        new RequestHeader(ApiKeys.API_VERSIONS, version, "client", 1),
                        new ApiVersionsRequest.Builder().build(version),
                        "KAFKA",
                        connection,
                        buffer,
                        new TestingChannelHandlerContext(),
                        new CompletableFuture<>());
        buffer.release();
        KafkaRequestContext context = KafkaRequestContext.fromRequest(request);
        if (closeConnection) {
            connection.close();
        } else {
            connection.failAuthentication();
        }
        assertThat(connection.shouldClose()).isTrue();
        assertThat(connection.principal())
                .isEqualTo(closeConnection ? principal : FlussPrincipal.ANONYMOUS);
        assertThat(request.principal()).isEqualTo(principal);
        assertThat(context.principal()).isEqualTo(principal);
        assertThat(context.saslConnection()).isSameAs(connection);
        request.releaseBuffer();
    }

    @Test
    public void testReleaseBufferIsIdempotent() {
        short version = ApiKeys.API_VERSIONS.oldestVersion();
        ByteBuf buffer = mock(ByteBuf.class);
        when(buffer.retain()).thenReturn(buffer);
        KafkaRequest request =
                new KafkaRequest(
                        ApiKeys.API_VERSIONS,
                        version,
                        new RequestHeader(ApiKeys.API_VERSIONS, version, "client-id", 1),
                        new ApiVersionsRequest.Builder().build(version),
                        "KAFKA",
                        KafkaSaslConnection.plaintext(),
                        buffer,
                        mock(ChannelHandlerContext.class),
                        new CompletableFuture<>());

        request.releaseBuffer();
        request.releaseBuffer();

        verify(buffer).retain();
        verify(buffer).release();
    }
}
