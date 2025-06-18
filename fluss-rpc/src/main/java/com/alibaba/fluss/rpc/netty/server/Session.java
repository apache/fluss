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

package com.alibaba.fluss.rpc.netty.server;

import com.alibaba.fluss.security.acl.FlussPrincipal;

import java.io.Serializable;
import java.net.InetAddress;

/** The connection session of a request. */
public class Session implements Serializable {
    private final short apiVersion;
    private final String listenerName;
    private final boolean isInternal;
    private final InetAddress inetAddress;
    private final FlussPrincipal principal;

    public Session(
            short apiVersion,
            String listenerName,
            boolean isInternal,
            InetAddress inetAddress,
            FlussPrincipal principal) {
        this.apiVersion = apiVersion;
        this.listenerName = listenerName;
        this.isInternal = isInternal;
        this.inetAddress = inetAddress;
        this.principal = principal;
    }

    public short getApiVersion() {
        return apiVersion;
    }

    public String getListenerName() {
        return listenerName;
    }

    public InetAddress getInetAddress() {
        return inetAddress;
    }

    public FlussPrincipal getPrincipal() {
        return principal;
    }

    public boolean isInternal() {
        return isInternal;
    }
}
