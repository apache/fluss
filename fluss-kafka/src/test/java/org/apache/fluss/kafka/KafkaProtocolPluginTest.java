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
import org.apache.fluss.rpc.TestingTabletGatewayService;
import org.apache.fluss.rpc.netty.server.RequestChannel;
import org.apache.fluss.shaded.netty4.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.fluss.shaded.netty4.io.netty.handler.timeout.IdleStateHandler;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests listener configuration and required TabletService support. */
class KafkaProtocolPluginTest {
    @Test
    void testZeroDisablesNettyIdleTimeout() throws Exception {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.NETTY_CONNECTION_MAX_IDLE_TIME, Duration.ZERO);
        NioSocketChannel channel = new NioSocketChannel();
        try {
            KafkaProtocolPlugin plugin = new KafkaProtocolPlugin();
            plugin.setup(conf);
            KafkaChannelInitializer initializer =
                    (KafkaChannelInitializer)
                            plugin.createChannelHandler(
                                    new RequestChannel[] {new RequestChannel(1)}, "KAFKA");
            initializer.initChannel(channel);
            IdleStateHandler idleHandler = channel.pipeline().get(IdleStateHandler.class);
            assertThat(idleHandler.getAllIdleTimeInMillis()).isZero();
            assertThat(idleHandler.getReaderIdleTimeInMillis()).isZero();
            assertThat(idleHandler.getWriterIdleTimeInMillis()).isZero();
        } finally {
            // This unregistered channel has no event loop to handle an asynchronous close.
            channel.unsafe().closeForcibly();
        }
    }

    @Test
    void testRejectsNonTabletService() {
        KafkaProtocolPlugin plugin = new KafkaProtocolPlugin();
        plugin.setup(new Configuration());
        assertThatThrownBy(() -> plugin.createRequestHandler(new TestingTabletGatewayService()))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
