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

import org.apache.fluss.kafka.api.admin.CreateTopicsHandler;
import org.apache.fluss.kafka.api.admin.DeleteTopicsHandler;
import org.apache.fluss.kafka.api.metadata.MetadataHandler;
import org.apache.fluss.kafka.api.produce.ProduceHandler;
import org.apache.fluss.kafka.api.sasl.SaslAuthenticateHandler;
import org.apache.fluss.kafka.api.sasl.SaslHandshakeHandler;
import org.apache.fluss.kafka.api.versions.ApiVersionsHandler;
import org.apache.fluss.kafka.backend.admin.GatewayKafkaTopicAdminBackend;
import org.apache.fluss.kafka.backend.metadata.GatewayKafkaMetadataBackend;
import org.apache.fluss.kafka.backend.produce.GatewayKafkaProduceBackend;
import org.apache.fluss.kafka.dispatcher.KafkaApiRegistry;
import org.apache.fluss.kafka.dispatcher.KafkaRequestDispatcher;
import org.apache.fluss.kafka.error.KafkaErrorMapper;
import org.apache.fluss.kafka.format.KafkaDataFormat;
import org.apache.fluss.kafka.transcode.ArrowKafkaRecordTranscoder;
import org.apache.fluss.rpc.RpcGatewayService;
import org.apache.fluss.rpc.gateway.AdminGateway;
import org.apache.fluss.rpc.gateway.AdminOperationAuthorizer;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.netty.server.RequestHandler;
import org.apache.fluss.rpc.protocol.RequestType;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Entry point that dispatches Kafka protocol requests to registered API handlers. */
public class KafkaRequestHandler implements RequestHandler<KafkaRequest> {

    private final KafkaRequestDispatcher dispatcher;

    /** Creates a Kafka request handler with the capabilities provided by a TabletServer. */
    public KafkaRequestHandler(
            RpcGatewayService service, TabletServerGateway gateway, String kafkaDatabase) {
        checkNotNull(service);
        checkNotNull(gateway);
        checkNotNull(kafkaDatabase);
        KafkaApiRegistry registry = new KafkaApiRegistry();
        registry.register(new ApiVersionsHandler(registry));
        registry.register(new SaslHandshakeHandler());
        registry.register(new SaslAuthenticateHandler());
        registry.register(
                new MetadataHandler(
                        new GatewayKafkaMetadataBackend(service, gateway, kafkaDatabase)));
        registry.register(
                new ProduceHandler(
                        new GatewayKafkaProduceBackend(
                                service,
                                gateway,
                                kafkaDatabase,
                                new ArrowKafkaRecordTranscoder())));
        registry.freeze();
        this.dispatcher = new KafkaRequestDispatcher(registry, new KafkaErrorMapper());
    }

    /** Creates a Kafka request handler including topic lifecycle capabilities. */
    public KafkaRequestHandler(
            RpcGatewayService service,
            TabletServerGateway gateway,
            AdminGateway adminGateway,
            String kafkaDatabase) {
        this(service, gateway, adminGateway, adminOperationAuthorizer(service), kafkaDatabase);
    }

    /** Creates a Kafka request handler with explicit authorization for topic lifecycle requests. */
    public KafkaRequestHandler(
            RpcGatewayService service,
            TabletServerGateway gateway,
            AdminGateway adminGateway,
            AdminOperationAuthorizer adminOperationAuthorizer,
            String kafkaDatabase) {
        this(
                service,
                gateway,
                adminGateway,
                adminOperationAuthorizer,
                kafkaDatabase,
                KafkaDataFormat.RAW,
                KafkaDataFormat.RAW);
    }

    /**
     * Creates a Kafka request handler including topic lifecycle and default format capabilities.
     */
    public KafkaRequestHandler(
            RpcGatewayService service,
            TabletServerGateway gateway,
            AdminGateway adminGateway,
            String kafkaDatabase,
            KafkaDataFormat defaultKeyFormat,
            KafkaDataFormat defaultValueFormat) {
        this(
                service,
                gateway,
                adminGateway,
                adminOperationAuthorizer(service),
                kafkaDatabase,
                defaultKeyFormat,
                defaultValueFormat);
    }

    /**
     * Creates a Kafka request handler with explicit authorization and default format capabilities.
     */
    public KafkaRequestHandler(
            RpcGatewayService service,
            TabletServerGateway gateway,
            AdminGateway adminGateway,
            AdminOperationAuthorizer adminOperationAuthorizer,
            String kafkaDatabase,
            KafkaDataFormat defaultKeyFormat,
            KafkaDataFormat defaultValueFormat) {
        checkNotNull(service);
        checkNotNull(gateway);
        checkNotNull(adminGateway);
        checkNotNull(adminOperationAuthorizer);
        checkNotNull(kafkaDatabase);
        checkNotNull(defaultKeyFormat);
        checkNotNull(defaultValueFormat);
        KafkaApiRegistry registry = new KafkaApiRegistry();
        registry.register(new ApiVersionsHandler(registry));
        registry.register(new SaslHandshakeHandler());
        registry.register(new SaslAuthenticateHandler());
        registry.register(
                new MetadataHandler(
                        new GatewayKafkaMetadataBackend(service, gateway, kafkaDatabase), true));
        registry.register(
                new ProduceHandler(
                        new GatewayKafkaProduceBackend(
                                service,
                                gateway,
                                kafkaDatabase,
                                new ArrowKafkaRecordTranscoder())));
        GatewayKafkaTopicAdminBackend topicAdminBackend =
                new GatewayKafkaTopicAdminBackend(
                        service, adminGateway, adminOperationAuthorizer, kafkaDatabase);
        registry.register(
                new CreateTopicsHandler(topicAdminBackend, defaultKeyFormat, defaultValueFormat));
        registry.register(new DeleteTopicsHandler(topicAdminBackend));
        registry.freeze();
        this.dispatcher = new KafkaRequestDispatcher(registry, new KafkaErrorMapper());
    }

    private static AdminOperationAuthorizer adminOperationAuthorizer(RpcGatewayService service) {
        if (!(service instanceof AdminOperationAuthorizer)) {
            throw new IllegalArgumentException(
                    "Kafka topic administration requires an AdminOperationAuthorizer.");
        }
        return (AdminOperationAuthorizer) service;
    }

    @Override
    public RequestType requestType() {
        return RequestType.KAFKA;
    }

    @Override
    public void processRequest(KafkaRequest request) {
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
}
