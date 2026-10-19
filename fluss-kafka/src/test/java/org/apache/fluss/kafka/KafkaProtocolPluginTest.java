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
import org.apache.fluss.rpc.netty.server.RequestChannel;
import org.apache.fluss.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.fluss.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.fluss.shaded.netty4.io.netty.handler.timeout.IdleStateHandler;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests disabling the Kafka listener's idle timeout. */
class KafkaProtocolPluginTest {

    @Test
    void testZeroDisablesNettyIdleTimeout() throws Exception {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.NETTY_CONNECTION_MAX_IDLE_TIME, Duration.ZERO);
        EmbeddedChannel channel = new EmbeddedChannel();
        try {
            // Adapt the socket type while installing the real pipeline on the embedded event loop.
            SocketChannel socket = mock(SocketChannel.class);
            when(socket.pipeline()).thenReturn(channel.pipeline());
            KafkaProtocolPlugin plugin = new KafkaProtocolPlugin();
            plugin.setup(conf);
            KafkaChannelInitializer initializer =
                    (KafkaChannelInitializer)
                            plugin.createChannelHandler(
                                    new RequestChannel[] {new RequestChannel(1)}, "KAFKA");
            initializer.initChannel(socket);
            IdleStateHandler idleHandler = channel.pipeline().get(IdleStateHandler.class);
            assertThat(idleHandler.getAllIdleTimeInMillis()).isZero();
            assertThat(idleHandler.getReaderIdleTimeInMillis()).isZero();
            assertThat(idleHandler.getWriterIdleTimeInMillis()).isZero();
        } finally {
            channel.finishAndReleaseAll();
        }
    }
}
