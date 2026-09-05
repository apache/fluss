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

package org.apache.fluss.kafka.dispatcher;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.kafka.KafkaRequest;
import org.apache.fluss.kafka.KafkaRequestContext;
import org.apache.fluss.kafka.error.KafkaErrorMapper;

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;

import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Validates and dispatches parsed Kafka requests to independently registered API handlers. */
@Internal
public final class KafkaRequestDispatcher {

    private final KafkaApiRegistry registry;
    private final KafkaErrorMapper errorMapper;

    /** Creates a dispatcher backed by the supplied registry and error mapper. */
    public KafkaRequestDispatcher(KafkaApiRegistry registry, KafkaErrorMapper errorMapper) {
        this.registry = checkNotNull(registry);
        this.errorMapper = checkNotNull(errorMapper);
    }

    /** Dispatches a request and always completes with a Kafka protocol response. */
    public CompletableFuture<AbstractResponse> dispatch(KafkaRequest request) {
        AbstractRequest abstractRequest = request.request();
        KafkaApiHandler<?> handler = registry.lookup(request.apiKey());
        if (handler == null) {
            return completedErrorResponse(
                    abstractRequest,
                    new UnsupportedVersionException(
                            "Kafka API " + request.apiKey() + " is not supported by this server."));
        }

        KafkaApiSpec spec = handler.apiSpec();
        if (!spec.supportsVersion(request.apiVersion())
                && !shouldDispatchBeforeVersionValidation(request)) {
            return completedErrorResponse(
                    abstractRequest,
                    new UnsupportedVersionException(
                            String.format(
                                    "Version %s is not supported for %s. Supported versions are [%s, %s].",
                                    request.apiVersion(),
                                    request.apiKey(),
                                    spec.minVersion(),
                                    spec.maxVersion())));
        }

        CompletableFuture<? extends AbstractResponse> responseFuture;
        try {
            responseFuture =
                    invoke(handler, KafkaRequestContext.fromRequest(request), abstractRequest);
            if (responseFuture == null) {
                throw new NullPointerException("Kafka API handler returned a null future.");
            }
        } catch (Throwable t) {
            return completedErrorResponse(abstractRequest, t);
        }

        CompletableFuture<AbstractResponse> result = new CompletableFuture<>();
        responseFuture.whenComplete(
                (response, failure) -> {
                    if (failure == null && response != null) {
                        result.complete(response);
                    } else {
                        Throwable responseFailure =
                                failure == null
                                        ? new NullPointerException(
                                                "Kafka API handler returned a null response.")
                                        : failure;
                        result.complete(errorMapper.toResponse(abstractRequest, responseFailure));
                    }
                });
        return result;
    }

    private static boolean shouldDispatchBeforeVersionValidation(KafkaRequest request) {
        return request.apiKey() == ApiKeys.API_VERSIONS
                && request.saslConnection().isAuthenticating();
    }

    @SuppressWarnings("unchecked")
    private static CompletableFuture<? extends AbstractResponse> invoke(
            KafkaApiHandler<?> handler, KafkaRequestContext context, AbstractRequest request) {
        return ((KafkaApiHandler<AbstractRequest>) handler).handle(context, request);
    }

    private CompletableFuture<AbstractResponse> completedErrorResponse(
            AbstractRequest request, Throwable failure) {
        return CompletableFuture.completedFuture(errorMapper.toResponse(request, failure));
    }
}
