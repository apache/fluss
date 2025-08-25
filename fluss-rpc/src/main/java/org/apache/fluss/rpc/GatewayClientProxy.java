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

import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.protocol.ApiManager;
import org.apache.fluss.rpc.protocol.ApiMethod;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.function.Supplier;

/** Proxy for a {@link RpcGateway} that forwards all method calls to the remote gateway service. */
public class GatewayClientProxy implements InvocationHandler {

    private final Supplier<ServerNode> nodeSupplier;
    private final RpcClient client;

    GatewayClientProxy(Supplier<ServerNode> nodeSupplier, RpcClient client) {
        this.nodeSupplier = nodeSupplier;
        this.client = client;
    }

    /**
     * Creates a proxy for the given gateway class. The proxy will forward all method calls to the
     * remote gateway service.
     */
    public static <T extends RpcGateway> T createGatewayProxy(
            Supplier<ServerNode> nodeSupplier, RpcClient client, Class<T> gatewayClass) {
        // Rather than using the System ClassLoader directly, we derive the
        // ClassLoader from gateway class. That works better in cases where Fluss
        // runs embedded and all Fluss code is loaded dynamically (for example
        // from an OSGI bundle) through a custom ClassLoader
        ClassLoader classLoader = gatewayClass.getClassLoader();

        @SuppressWarnings("unchecked")
        T proxy =
                (T)
                        Proxy.newProxyInstance(
                                classLoader,
                                new Class<?>[] {gatewayClass},
                                new GatewayClientProxy(nodeSupplier, client));
        return proxy;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (args.length == 1) {
            Object arg = args[0];
            if (arg instanceof ApiMessage) {
                return invokeRpc(method.getName(), (ApiMessage) arg);
            }
        }
        throw new IllegalArgumentException(
                "RpcGateway methods must have exactly one argument of type ApiMessage");
    }

    private Object invokeRpc(String methodName, ApiMessage request) {
        ApiMethod apiMethod = ApiManager.forMethodName(methodName);
        if (apiMethod == null) {
            throw new IllegalArgumentException("Unknown RPC method: " + methodName);
        }
        return client.sendRequest(nodeSupplier.get(), apiMethod.getApiKey(), request);
    }
}
