/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.rpc;

import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.shaded.guava32.com.google.common.cache.Cache;
import com.alibaba.fluss.shaded.guava32.com.google.common.cache.CacheBuilder;

import java.time.Duration;
import java.util.concurrent.ExecutionException;

/** The cache utils class for {@link RpcGateway}. */
public class RpcGatewayCache<T extends RpcGateway> {

    private final Cache<ServerNode, T> cache;

    public RpcGatewayCache() {
        this(8, Duration.ofHours(1L), 64);
    }

    public RpcGatewayCache(int concurrencyLevel, Duration expireAfterAccess, int maximumSize) {
        this.cache =
                CacheBuilder.newBuilder()
                        .concurrencyLevel(concurrencyLevel)
                        .expireAfterAccess(expireAfterAccess)
                        .maximumSize(maximumSize)
                        .build();
    }

    public T getOrCreateGatewayProxy(
            ServerNode serverNode, RpcClient client, Class<T> gatewayClass) {
        try {
            return cache.get(
                    serverNode,
                    () ->
                            GatewayClientProxy.createGatewayProxy(
                                    () -> serverNode, client, gatewayClass));
        } catch (ExecutionException e) {
            throw new FlussRuntimeException(e);
        }
    }
}
