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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.kafka.security.KafkaSaslConnection;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.shaded.netty4.io.netty.channel.Channel;

import org.apache.kafka.common.protocol.ApiKeys;

import java.net.SocketAddress;

/** Immutable wire-level context made available to Kafka API handlers. */
@Internal
public final class KafkaRequestContext {

    private final int correlationId;
    private final String clientId;
    private final ApiKeys apiKey;
    private final short apiVersion;
    private final String listenerName;
    private final SocketAddress localAddress;
    private final SocketAddress remoteAddress;
    private final long receivedTimeMs;
    private final KafkaRequest request;

    private KafkaRequestContext(KafkaRequest request) {
        this.request = request;
        this.correlationId = request.header().correlationId();
        this.clientId = request.header().clientId();
        this.apiKey = request.apiKey();
        this.apiVersion = request.apiVersion();
        this.listenerName = request.listenerName();
        Channel channel = request.ctx().channel();
        this.localAddress = channel == null ? null : channel.localAddress();
        this.remoteAddress = channel == null ? null : channel.remoteAddress();
        this.receivedTimeMs = request.startTimeMs();
    }

    /** Creates a context from a network request. */
    public static KafkaRequestContext fromRequest(KafkaRequest request) {
        return new KafkaRequestContext(request);
    }

    /** Returns the request correlation ID. */
    public int correlationId() {
        return correlationId;
    }

    /** Returns the client ID, or {@code null} when the request did not provide one. */
    public String clientId() {
        return clientId;
    }

    /** Returns the Kafka API key. */
    public ApiKeys apiKey() {
        return apiKey;
    }

    /** Returns the Kafka request version. */
    public short apiVersion() {
        return apiVersion;
    }

    /** Returns the listener that accepted the request. */
    public String listenerName() {
        return listenerName;
    }

    /** Returns the local socket address. */
    public SocketAddress localAddress() {
        return localAddress;
    }

    /** Returns the remote socket address. */
    public SocketAddress remoteAddress() {
        return remoteAddress;
    }

    /** Returns the wall-clock time at which the request was received. */
    public long receivedTimeMs() {
        return receivedTimeMs;
    }

    /** Returns the authenticated principal captured when this request was received. */
    public FlussPrincipal principal() {
        return request.principal();
    }

    /** Returns this network connection's SASL state machine. */
    public KafkaSaslConnection saslConnection() {
        return request.saslConnection();
    }

    /** Closes the connection after this request's response has been flushed. */
    public void closeConnectionAfterResponse() {
        request.closeConnectionAfterResponse();
    }
}
