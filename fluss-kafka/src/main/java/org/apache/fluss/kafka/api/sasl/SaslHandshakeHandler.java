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

package org.apache.fluss.kafka.api.sasl;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.kafka.KafkaRequestContext;
import org.apache.fluss.kafka.dispatcher.KafkaApiHandler;
import org.apache.fluss.kafka.dispatcher.KafkaApiSpec;
import org.apache.fluss.kafka.security.KafkaSaslConnection;

import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.apache.kafka.common.requests.SaslHandshakeResponse;

import java.net.SocketAddress;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

/** Implements the Kafka SASL handshake for the PLAIN mechanism. */
@Internal
public final class SaslHandshakeHandler implements KafkaApiHandler<SaslHandshakeRequest> {

    private static final KafkaApiSpec API_SPEC =
            new KafkaApiSpec(ApiKeys.SASL_HANDSHAKE, (short) 1, (short) 1, true);

    @Override
    public KafkaApiSpec apiSpec() {
        return API_SPEC;
    }

    @Override
    public CompletableFuture<? extends AbstractResponse> handle(
            KafkaRequestContext context, SaslHandshakeRequest request) {
        return handle(
                context.saslConnection(),
                context.listenerName(),
                context.remoteAddress(),
                request,
                context::closeConnectionAfterResponse);
    }

    CompletableFuture<SaslHandshakeResponse> handle(
            KafkaSaslConnection connection,
            String listenerName,
            SocketAddress remoteAddress,
            SaslHandshakeRequest request) {
        return handle(connection, listenerName, remoteAddress, request, () -> {});
    }

    CompletableFuture<SaslHandshakeResponse> handle(
            KafkaSaslConnection connection,
            String listenerName,
            SocketAddress remoteAddress,
            SaslHandshakeRequest request,
            Runnable closeConnectionAfterResponse) {
        if (!connection.authenticationEnabled() || !connection.isAwaitingHandshake()) {
            return failure(
                    connection,
                    closeConnectionAfterResponse,
                    Errors.ILLEGAL_SASL_STATE,
                    Collections.singletonList(KafkaSaslConnection.PLAIN_MECHANISM));
        }

        String mechanism = request.data().mechanism();
        if (!KafkaSaslConnection.PLAIN_MECHANISM.equals(mechanism)) {
            return failure(
                    connection,
                    closeConnectionAfterResponse,
                    Errors.UNSUPPORTED_SASL_MECHANISM,
                    Collections.singletonList(KafkaSaslConnection.PLAIN_MECHANISM));
        }

        try {
            connection.beginAuthentication(mechanism, listenerName, remoteAddress);
            return CompletableFuture.completedFuture(
                    response(
                            Errors.NONE,
                            Collections.singletonList(KafkaSaslConnection.PLAIN_MECHANISM)));
        } catch (RuntimeException e) {
            return failure(
                    connection,
                    closeConnectionAfterResponse,
                    Errors.SASL_AUTHENTICATION_FAILED,
                    Collections.singletonList(KafkaSaslConnection.PLAIN_MECHANISM));
        }
    }

    private static CompletableFuture<SaslHandshakeResponse> failure(
            KafkaSaslConnection connection,
            Runnable closeConnectionAfterResponse,
            Errors error,
            java.util.List<String> mechanisms) {
        connection.failAuthentication();
        closeConnectionAfterResponse.run();
        return CompletableFuture.completedFuture(response(error, mechanisms));
    }

    private static SaslHandshakeResponse response(Errors error, java.util.List<String> mechanisms) {
        return new SaslHandshakeResponse(
                new SaslHandshakeResponseData()
                        .setErrorCode(error.code())
                        .setMechanisms(mechanisms));
    }
}
