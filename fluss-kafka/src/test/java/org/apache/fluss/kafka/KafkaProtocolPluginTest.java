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
import org.apache.fluss.shaded.netty4.io.netty.handler.timeout.IdleStateEvent;
import org.apache.fluss.shaded.netty4.io.netty.handler.timeout.IdleStateHandler;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests Kafka listener idle-time configuration and idle-event handling. */
class KafkaProtocolPluginTest {

    @Test
    void testDefaultNettyIdleTimeout() throws Exception {
        EmbeddedChannel channel = createChannel(new Configuration());
        try {
            IdleStateHandler idleHandler = channel.pipeline().get(IdleStateHandler.class);
            assertThat(idleHandler.getAllIdleTimeInMillis())
                    .isEqualTo(Duration.ofMinutes(10).toMillis());
            assertThat(idleHandler.getReaderIdleTimeInMillis()).isZero();
            assertThat(idleHandler.getWriterIdleTimeInMillis()).isZero();
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 7})
    void testConfiguredNettyIdleTimeoutOverridesLegacyKafkaOption(long timeoutSeconds)
            throws Exception {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.NETTY_CONNECTION_MAX_IDLE_TIME, Duration.ofSeconds(timeoutSeconds));
        conf.setString("kafka.connection.max-idle-time", "2 s");
        EmbeddedChannel channel = createChannel(conf);
        try {
            IdleStateHandler idleHandler = channel.pipeline().get(IdleStateHandler.class);
            assertThat(idleHandler.getAllIdleTimeInMillis())
                    .isEqualTo(Duration.ofSeconds(timeoutSeconds).toMillis());
            assertThat(idleHandler.getReaderIdleTimeInMillis()).isZero();
            assertThat(idleHandler.getWriterIdleTimeInMillis()).isZero();
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void testOnlyAllIdleClosesConnection() throws Exception {
        EmbeddedChannel channel = createChannel(new Configuration());
        try {
            channel.pipeline().fireUserEventTriggered(IdleStateEvent.FIRST_READER_IDLE_STATE_EVENT);
            assertThat(channel.isActive()).isTrue();
            channel.pipeline().fireUserEventTriggered(IdleStateEvent.FIRST_WRITER_IDLE_STATE_EVENT);
            assertThat(channel.isActive()).isTrue();
            channel.pipeline().fireUserEventTriggered(IdleStateEvent.FIRST_ALL_IDLE_STATE_EVENT);
            assertThat(channel.isActive()).isFalse();
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    private static EmbeddedChannel createChannel(Configuration conf) throws Exception {
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
            return channel;
        } catch (Exception | Error failure) {
            channel.finishAndReleaseAll();
            throw failure;
        }
    }
}
