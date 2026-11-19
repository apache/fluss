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

import org.apache.fluss.rpc.netty.server.RequestChannel;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.fluss.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.fluss.shaded.netty4.io.netty.handler.flow.FlowControlHandler;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestUtils;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that Kafka channels participate in {@link RequestChannel} backpressure: a channel is paused
 * when its request queue crosses the backpressure threshold, resumed when the queue drains, and no
 * longer touched once closed.
 */
public class KafkaChannelBackpressureTest {

    @Test
    public void testQueueBackpressurePausesAndResumesChannelReads() {
        RequestChannel requestChannel = new RequestChannel(1);
        EmbeddedChannel channel =
                new EmbeddedChannel(new KafkaCommandDecoder(requestChannel, "KAFKA"));
        try {
            // A request decoded by the channel and queued for processing must pause reads on
            // this channel. The pause task runs on the channel's event loop, like on a server.
            channel.writeInbound(apiVersionsRequest(1));
            channel.runPendingTasks();
            assertThat(channel.config().isAutoRead()).isFalse();

            // Draining the queue below the resume threshold must resume reads.
            KafkaRequest queued = (KafkaRequest) requestChannel.pollRequest(0);
            assertThat(queued).isNotNull();
            channel.runPendingTasks();
            assertThat(channel.config().isAutoRead()).isTrue();
            queued.releaseBuffer();
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void testClosedChannelIsNoLongerPausedByBackpressure() {
        RequestChannel requestChannel = new RequestChannel(1);
        EmbeddedChannel openChannel =
                new EmbeddedChannel(new KafkaCommandDecoder(requestChannel, "KAFKA"));
        EmbeddedChannel closedChannel =
                new EmbeddedChannel(new KafkaCommandDecoder(requestChannel, "KAFKA"));
        try {
            // Closing a channel unregisters it, or the closed channel (and its pipeline)
            // would stay referenced by the request channel; later backpressure must not
            // touch it either.
            closedChannel.close();
            assertThat(requestChannel.isChannelRegistered(openChannel)).isTrue();
            assertThat(requestChannel.isChannelRegistered(closedChannel)).isFalse();
            assertThat(closedChannel.config().isAutoRead()).isTrue();

            openChannel.writeInbound(apiVersionsRequest(1));
            openChannel.runPendingTasks();
            closedChannel.runPendingTasks();

            assertThat(openChannel.config().isAutoRead()).isFalse();
            assertThat(closedChannel.config().isAutoRead()).isTrue();

            KafkaRequest queued = (KafkaRequest) requestChannel.pollRequest(0);
            assertThat(queued).isNotNull();
            queued.releaseBuffer();
        } finally {
            openChannel.finishAndReleaseAll();
            closedChannel.finishAndReleaseAll();
        }
    }

    @Test
    public void testFrameSentWhilePausedIsDeliveredAfterResume() {
        RequestChannel requestChannel = new RequestChannel(1);
        // Mirror the production pipeline: FlowControlHandler withholds frames while reads are
        // paused and re-delivers them once reads resume.
        EmbeddedChannel channel =
                new EmbeddedChannel(
                        new FlowControlHandler(), new KafkaCommandDecoder(requestChannel, "KAFKA"));
        try {
            // The first request crosses the threshold and pauses reads. The pause lands only
            // after the read that delivered it, so that read's one-message credit also lets
            // the second request through; FlowControlHandler grants exactly one message per
            // read, on a real server as well.
            channel.writeInbound(apiVersionsRequest(1));
            channel.runPendingTasks();
            assertThat(channel.config().isAutoRead()).isFalse();
            channel.writeInbound(apiVersionsRequest(2));

            // With the credit spent and reads paused, a further frame is withheld by the
            // FlowControlHandler and never decoded into a queued request.
            channel.writeInbound(apiVersionsRequest(3));
            KafkaRequest first = (KafkaRequest) requestChannel.pollRequest(0);
            KafkaRequest second = (KafkaRequest) requestChannel.pollRequest(0);
            assertThat(first.header().correlationId()).isEqualTo(1);
            assertThat(second.header().correlationId()).isEqualTo(2);
            assertThat(requestChannel.pollRequest(0)).isNull();

            // Draining the queue resumes reads, and the withheld frame is re-delivered for
            // processing rather than lost.
            channel.runPendingTasks();
            KafkaRequest withheld = (KafkaRequest) requestChannel.pollRequest(1000);
            assertThat(withheld.header().correlationId()).isEqualTo(3);
            first.releaseBuffer();
            second.releaseBuffer();
            withheld.releaseBuffer();
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    private static ByteBuf apiVersionsRequest(int correlationId) {
        short version = ApiKeys.API_VERSIONS.latestVersion();
        RequestHeader header =
                new RequestHeader(ApiKeys.API_VERSIONS, version, "client", correlationId);
        ApiVersionsRequest request =
                new ApiVersionsRequest(new ApiVersionsRequestData(), version, version);
        ByteBuffer serialized =
                RequestUtils.serialize(
                        header.data(), header.headerVersion(), request.data(), request.version());
        return Unpooled.wrappedBuffer(serialized);
    }
}
