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

import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.rpc.netty.server.Session;

// TODO: support MainThreadRpcGateway which ensures that all methods are executed on the main
//   thread.
/**
 * RPC gateway extension for servers which is able to get the API version of current API invocation.
 */
public abstract class RpcGatewayService implements RpcGateway {

    private final ThreadLocal<Session> currentSession = new ThreadLocal<>();

    /**
     * Sets API version and listener name of the RPC call to the current session. This method is
     * thread-safe
     */
    public void setCurrentSession(Session session) {
        currentSession.set(session);
    }

    public Session currentSession() {
        Session session = currentSession.get();
        if (session == null) {
            throw new IllegalStateException(
                    "No session set. This method should only be called from within an RPC call.");
        } else {
            return session;
        }
    }

    /**
     * Returns the current API version of an RPC call. This method is thread-safe and can only be
     * accessed in RPC methods.
     */
    public short currentApiVersion() {
        Session session = currentSession.get();
        if (session == null) {
            throw new IllegalStateException(
                    "No API version set. This method should only be called from within an RPC call.");
        } else {
            return session.getApiVersion();
        }
    }

    /**
     * Returns the current listener name of an RPC call. This method is thread-safe and can only be
     * accessed in RPC methods.
     *
     * @return
     */
    public String currentListenerName() {
        Session session = currentSession.get();
        if (session == null || session.getListenerName() == null) {
            throw new IllegalStateException(
                    "No listener name set. This method should only be called from within an RPC call.");
        } else {
            return session.getListenerName();
        }
    }

    /** Returns the provider type of this RPC gateway service. */
    public abstract ServerType providerType();

    /** The service name of the gateway used for logging. */
    public abstract String name();

    /** Shutdown the gateway service, release any resources. */
    public abstract void shutdown();
}
