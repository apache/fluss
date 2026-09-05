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

import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.SaslAuthenticateRequest;
import org.apache.kafka.common.requests.SaslAuthenticateResponse;

import java.util.concurrent.CompletableFuture;

/** Implements Kafka-framed SASL/PLAIN token authentication. */
@Internal
public final class SaslAuthenticateHandler implements KafkaApiHandler<SaslAuthenticateRequest> {

    private static final KafkaApiSpec API_SPEC =
            new KafkaApiSpec(ApiKeys.SASL_AUTHENTICATE, (short) 0, (short) 2, true);
    private static final String AUTHENTICATION_FAILURE_MESSAGE =
            "Authentication failed due to invalid credentials with SASL mechanism PLAIN.";
    private static final String ILLEGAL_STATE_MESSAGE =
            "SASL authentication is not active on this connection.";

    @Override
    public KafkaApiSpec apiSpec() {
        return API_SPEC;
    }

    @Override
    public CompletableFuture<? extends AbstractResponse> handle(
            KafkaRequestContext context, SaslAuthenticateRequest request) {
        return handle(context.saslConnection(), request, context::closeConnectionAfterResponse);
    }

    CompletableFuture<SaslAuthenticateResponse> handle(
            KafkaSaslConnection connection, SaslAuthenticateRequest request) {
        return handle(connection, request, () -> {});
    }

    CompletableFuture<SaslAuthenticateResponse> handle(
            KafkaSaslConnection connection,
            SaslAuthenticateRequest request,
            Runnable closeConnectionAfterResponse) {
        if (!connection.authenticationEnabled() || !connection.isAuthenticating()) {
            return failure(
                    connection,
                    closeConnectionAfterResponse,
                    Errors.ILLEGAL_SASL_STATE,
                    ILLEGAL_STATE_MESSAGE);
        }

        try {
            byte[] challenge = connection.authenticate(request.data().authBytes());
            return CompletableFuture.completedFuture(response(Errors.NONE, null, challenge));
        } catch (RuntimeException e) {
            return failure(
                    connection,
                    closeConnectionAfterResponse,
                    Errors.SASL_AUTHENTICATION_FAILED,
                    AUTHENTICATION_FAILURE_MESSAGE);
        }
    }

    private static CompletableFuture<SaslAuthenticateResponse> failure(
            KafkaSaslConnection connection,
            Runnable closeConnectionAfterResponse,
            Errors error,
            String errorMessage) {
        connection.failAuthentication();
        closeConnectionAfterResponse.run();
        return CompletableFuture.completedFuture(response(error, errorMessage, new byte[0]));
    }

    private static SaslAuthenticateResponse response(
            Errors error, String errorMessage, byte[] authBytes) {
        return new SaslAuthenticateResponse(
                new SaslAuthenticateResponseData()
                        .setErrorCode(error.code())
                        .setErrorMessage(errorMessage)
                        .setAuthBytes(authBytes)
                        .setSessionLifetimeMs(0L));
    }
}
