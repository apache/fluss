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

package com.alibaba.fluss.rpc.protocol;

import com.alibaba.fluss.rpc.messages.FetchLogRequest;
import com.alibaba.fluss.rpc.messages.GetTableInfoRequest;
import com.alibaba.fluss.rpc.netty.server.FlussRequest;
import com.alibaba.fluss.rpc.netty.server.RequestChannel;
import com.alibaba.fluss.rpc.netty.server.RpcRequest;

import org.apache.fluss.shaded.netty4.io.netty.buffer.EmptyByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** The test for {@link RequestChannel}. */
public class RequestChannelTest {

    @Test
    void testRequestsFIFO() throws Exception {
        RequestChannel channel = new RequestChannel(100);

        // 1. Same request type, Use FIFO.
        List<RpcRequest> rpcRequests = new ArrayList<>();
        // push rpc requests
        for (int i = 0; i < 100; i++) {
            RpcRequest rpcRequest =
                    new FlussRequest(
                            ApiKeys.GET_TABLE_INFO.id,
                            (short) 0,
                            i,
                            null,
                            new GetTableInfoRequest(),
                            new EmptyByteBuf(new UnpooledByteBufAllocator(true, true)),
                            "FLUSS",
                            true,
                            null,
                            null,
                            new CompletableFuture<>());
            channel.putRequest(rpcRequest);
            rpcRequests.add(rpcRequest);
        }
        // pop rpc requests
        for (int i = 0; i < 100; i++) {
            RpcRequest gotRequest = channel.pollRequest(100);
            assertThat(gotRequest).isEqualTo(rpcRequests.get(i));
        }

        // 2. Different request type, Use FIFO.
        RpcRequest rpcRequest1 =
                new FlussRequest(
                        ApiKeys.GET_TABLE_INFO.id,
                        (short) 0,
                        3,
                        null,
                        new GetTableInfoRequest(),
                        new EmptyByteBuf(new UnpooledByteBufAllocator(true, true)),
                        "FLUSS",
                        true,
                        null,
                        null,
                        new CompletableFuture<>());
        RpcRequest rpcRequest2 =
                new FlussRequest(
                        ApiKeys.FETCH_LOG.id,
                        (short) 0,
                        100,
                        null,
                        new FetchLogRequest().setMaxBytes(100).setFollowerServerId(2),
                        new EmptyByteBuf(new UnpooledByteBufAllocator(true, true)),
                        "FLUSS",
                        true,
                        null,
                        null,
                        new CompletableFuture<>());
        channel.putRequest(rpcRequest1);
        channel.putRequest(rpcRequest2);
        RpcRequest rpcRequest = channel.pollRequest(100);
        assertThat(rpcRequest).isEqualTo(rpcRequest1);
        rpcRequest = channel.pollRequest(100);
        assertThat(rpcRequest).isEqualTo(rpcRequest2);
    }
}
