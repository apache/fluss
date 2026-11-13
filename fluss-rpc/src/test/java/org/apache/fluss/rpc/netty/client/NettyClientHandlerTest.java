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

package org.apache.fluss.rpc.netty.client;

import org.apache.fluss.exception.AuthenticationException;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.protocol.ApiMethod;
import org.apache.fluss.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.fluss.shaded.netty4.io.netty.handler.ssl.SslHandshakeCompletionEvent;

import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLException;

import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link NettyClientHandler}. */
class NettyClientHandlerTest {

    @Test
    void testHandshakeFailureWithoutMessage() {
        List<Throwable> failures = new ArrayList<>();
        EmbeddedChannel channel =
                new EmbeddedChannel(new NettyClientHandler(new FailureCollector(failures)));

        channel.pipeline()
                .fireUserEventTriggered(
                        new SslHandshakeCompletionEvent(new SSLException((String) null)));

        assertThat(failures).hasSize(1);
        assertThat(failures.get(0))
                .isInstanceOf(AuthenticationException.class)
                .hasMessageContaining("failed: javax.net.ssl.SSLException")
                .hasMessageNotContaining("null");
        channel.close();
    }

    @Test
    void testChannelClosedDuringHandshakeStaysRetriable() {
        List<Throwable> failures = new ArrayList<>();
        EmbeddedChannel channel =
                new EmbeddedChannel(new NettyClientHandler(new FailureCollector(failures)));

        channel.pipeline()
                .fireUserEventTriggered(
                        new SslHandshakeCompletionEvent(new ClosedChannelException()));

        // the later channelInactive reports the close as a retriable failure.
        assertThat(failures).isEmpty();
        channel.close();
    }

    /** Records the failures that reach the callback; the request callbacks are unused here. */
    private static final class FailureCollector implements ClientHandlerCallback {
        private final List<Throwable> failures;

        FailureCollector(List<Throwable> failures) {
            this.failures = failures;
        }

        @Override
        public ApiMethod getRequestApiMethod(int requestId) {
            return null;
        }

        @Override
        public void onRequestResult(int requestId, ApiMessage response) {}

        @Override
        public void onRequestFailure(int requestId, Throwable cause) {}

        @Override
        public void onFailure(Throwable cause) {
            failures.add(cause);
        }
    }
}
