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

import org.apache.fluss.cluster.ServerNode;

import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

/** Socket framing shared by Kafka Produce integration tests. */
final class KafkaSocketTestUtils {
    private KafkaSocketTestUtils() {}

    static Socket connect(ServerNode node, int readTimeoutMillis) throws Exception {
        Socket socket = new Socket();
        try {
            socket.connect(new InetSocketAddress(node.host(), node.port()), 5000);
            socket.setSoTimeout(readTimeoutMillis);
            return socket;
        } catch (Exception failure) {
            socket.close();
            throw failure;
        }
    }

    static AbstractResponse sendRequest(Socket socket, AbstractRequest request) throws Exception {
        return receive(socket, send(socket, request, 1));
    }

    static RequestHeader send(Socket socket, AbstractRequest request, int correlationId)
            throws Exception {
        RequestHeader header =
                new RequestHeader(request.apiKey(), request.version(), "kafka-test", correlationId);
        ByteBuffer encoded = request.serializeWithHeader(header);
        byte[] bytes = new byte[encoded.remaining()];
        encoded.get(bytes);
        DataOutputStream output = new DataOutputStream(socket.getOutputStream());
        output.writeInt(bytes.length);
        output.write(bytes);
        output.flush();
        return header;
    }

    static AbstractResponse receive(Socket socket, RequestHeader header) throws Exception {
        DataInputStream input = new DataInputStream(socket.getInputStream());
        int size = input.readInt();
        assertThat(size).isBetween(1, 1024 * 1024);
        byte[] bytes = new byte[size];
        input.readFully(bytes);
        assertThat(ByteBuffer.wrap(bytes).getInt()).isEqualTo(header.correlationId());
        return AbstractResponse.parseResponse(ByteBuffer.wrap(bytes), header);
    }
}
