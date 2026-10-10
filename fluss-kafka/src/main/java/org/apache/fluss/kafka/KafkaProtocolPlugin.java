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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.kafka.backend.produce.KafkaProduceConversionExecutor;
import org.apache.fluss.rpc.RpcGatewayService;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.netty.server.RequestChannel;
import org.apache.fluss.rpc.netty.server.RequestHandler;
import org.apache.fluss.rpc.protocol.NetworkProtocolPlugin;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandler;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.utils.Preconditions.checkState;

/** The Kafka protocol plugin. */
public class KafkaProtocolPlugin implements NetworkProtocolPlugin {

    private Configuration conf;
    private KafkaProduceConversionExecutor conversionExecutor;
    private CompletableFuture<Void> closeFuture;

    @Override
    public String name() {
        return KAFKA_PROTOCOL_NAME;
    }

    @Override
    public void setup(Configuration conf) {
        this.conf = new Configuration(conf);
    }

    @Override
    public List<String> listenerNames() {
        return conf.get(ConfigOptions.KAFKA_LISTENER_NAMES);
    }

    @Override
    public ChannelHandler createChannelHandler(
            RequestChannel[] requestChannels, String listenerName) {
        return new KafkaChannelInitializer(
                requestChannels,
                listenerName,
                conf.get(ConfigOptions.KAFKA_CONNECTION_MAX_IDLE_TIME).getSeconds(),
                (int) conf.get(ConfigOptions.NETTY_SERVER_MAX_REQUEST_SIZE).getBytes(),
                conf.getBoolean(ConfigOptions.NETTY_CLIENT_ALLOCATOR_HEAP_BUFFER_FIRST));
    }

    @Override
    public synchronized RequestHandler<?> createRequestHandler(RpcGatewayService service) {
        if (!(service instanceof TabletServerGateway)) {
            throw new IllegalArgumentException(
                    "Kafka protocol endpoints can only be enabled on TabletServers, but the service is "
                            + service.getClass().getSimpleName());
        }
        checkState(closeFuture == null, "Kafka protocol plugin has already been closed.");
        if (conversionExecutor == null) {
            // Mirror the RPC worker concurrency, with a separate queue capped at 1024 requests.
            int queueCapacity =
                    Math.max(
                            1,
                            Math.min(
                                    1024,
                                    conf.get(ConfigOptions.NETTY_SERVER_MAX_QUEUED_REQUESTS)));
            int threads =
                    Math.max(
                            1,
                            Math.min(
                                    queueCapacity,
                                    conf.get(ConfigOptions.NETTY_SERVER_NUM_WORKER_THREADS)));
            conversionExecutor = new KafkaProduceConversionExecutor(threads, queueCapacity);
        }
        TabletServerGateway gateway = (TabletServerGateway) service;
        return new KafkaRequestHandler(service, gateway, conversionExecutor);
    }

    @Override
    public synchronized CompletableFuture<Void> closeAsync() {
        if (closeFuture == null) {
            closeFuture =
                    conversionExecutor == null
                            ? CompletableFuture.completedFuture(null)
                            : conversionExecutor.closeAsync();
        }
        return closeFuture;
    }
}
