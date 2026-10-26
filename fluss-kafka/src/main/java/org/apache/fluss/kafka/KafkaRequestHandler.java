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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.kafka.api.metadata.MetadataHandler;
import org.apache.fluss.kafka.api.produce.ProduceHandler;
import org.apache.fluss.kafka.api.sasl.SaslAuthenticateHandler;
import org.apache.fluss.kafka.api.sasl.SaslHandshakeHandler;
import org.apache.fluss.kafka.api.versions.ApiVersionsHandler;
import org.apache.fluss.kafka.backend.metadata.GatewayKafkaMetadataBackend;
import org.apache.fluss.kafka.backend.metadata.KafkaMetadataBackend;
import org.apache.fluss.kafka.backend.produce.GatewayKafkaProduceBackend;
import org.apache.fluss.kafka.backend.produce.KafkaProduceBackend;
import org.apache.fluss.kafka.dispatcher.KafkaApiRegistry;
import org.apache.fluss.kafka.dispatcher.KafkaRequestDispatcher;
import org.apache.fluss.kafka.error.KafkaErrorMapper;
import org.apache.fluss.kafka.mapping.KafkaTopicMapper;
import org.apache.fluss.rpc.netty.server.RequestHandler;
import org.apache.fluss.rpc.protocol.RequestType;
import org.apache.fluss.server.tablet.TabletService;

import java.util.Arrays;

/** Entry point that dispatches Kafka protocol requests to registered API handlers. */
public class KafkaRequestHandler implements RequestHandler<KafkaRequest> {

    private final KafkaApiRegistry registry;
    private final KafkaRequestDispatcher dispatcher;
    // TODO: having a generic backend interface for all APIs, and close backends in the close()
    private final KafkaProduceBackend produceBackend;

    /** Creates a Kafka request handler with the capabilities provided by a TabletServer. */
    public KafkaRequestHandler(KafkaTopicMapper topicMapper, TabletService service) {
        this(
                topicMapper,
                new GatewayKafkaMetadataBackend(service, topicMapper),
                new GatewayKafkaProduceBackend(service, topicMapper));
    }

    @VisibleForTesting
    KafkaRequestHandler(
            KafkaTopicMapper topicMapper,
            KafkaMetadataBackend metadataBackend,
            KafkaProduceBackend produceBackend) {
        this.produceBackend = produceBackend;
        this.registry =
                new KafkaApiRegistry(
                        Arrays.asList(
                                new ApiVersionsHandler(this::apiRegistry),
                                new SaslHandshakeHandler(),
                                new SaslAuthenticateHandler(),
                                new MetadataHandler(metadataBackend, topicMapper),
                                new ProduceHandler(produceBackend)));
        this.dispatcher = new KafkaRequestDispatcher(registry, new KafkaErrorMapper());
    }

    @Override
    public RequestType requestType() {
        return RequestType.KAFKA;
    }

    @Override
    public void processRequest(KafkaRequest request) {
        request.processingStarted();
        dispatcher
                .dispatch(request)
                .whenComplete(
                        (response, failure) -> {
                            if (failure == null) {
                                request.complete(response);
                            } else {
                                request.fail(failure);
                            }
                        });
    }

    /** Releases conversion resources after request processing has stopped. */
    @Override
    public void close() throws Exception {
        produceBackend.close();
    }

    private KafkaApiRegistry apiRegistry() {
        return registry;
    }
}
