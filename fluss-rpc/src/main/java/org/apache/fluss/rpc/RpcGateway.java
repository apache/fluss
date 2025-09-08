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

package org.apache.fluss.rpc;

import org.apache.fluss.rpc.messages.ApiVersionsRequest;
import org.apache.fluss.rpc.messages.ApiVersionsResponse;
import org.apache.fluss.rpc.messages.AuthenticateRequest;
import org.apache.fluss.rpc.messages.AuthenticateResponse;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.rpc.protocol.RPC;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;

import java.util.concurrent.CompletableFuture;

/** Rpc gateway interface which has to be implemented by Rpc gateways. */
public interface RpcGateway {

    /** Returns the APIs and Versions of the RPC Gateway supported. */
    @RPC(api = ApiKeys.API_VERSIONS)
    CompletableFuture<ApiVersionsResponse> apiVersions(ApiVersionsRequest request);

    /**
     * This method just to registers the AUTHENTICATE API in the API manager for client-side
     * request/response object generation.
     *
     * <p>This method does not handle the authentication logic itself. Instead, the {@link
     * AuthenticateRequest} is processed preemptively by {@link
     * org.apache.fluss.rpc.netty.server.NettyServerHandler} during the initial connection
     * handshake. The client uses this method definition to generate corresponding request/response
     * objects for API version compatibility.
     *
     * @param request The authenticate request (not used in this method's implementation).
     * @return Always returns {@code null} since the actual authentication handling occurs in {@link
     *     org.apache.fluss.rpc.netty.server.NettyServerHandler}.
     * @see org.apache.fluss.rpc.netty.server.NettyServerHandler#channelRead(ChannelHandlerContext,
     *     Object) For authentication processing implementation.
     */
    @RPC(api = ApiKeys.AUTHENTICATE)
    default CompletableFuture<AuthenticateResponse> authenticate(AuthenticateRequest request) {
        throw new UnsupportedOperationException("This method should not be called directly.");
    }
}
