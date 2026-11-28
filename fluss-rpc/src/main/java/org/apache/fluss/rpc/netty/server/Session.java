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

package org.apache.fluss.rpc.netty.server;

import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.shaded.guava32.com.google.common.collect.ImmutableList;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.Collections;
import java.util.List;

/** The connection session of a request. */
public class Session implements Serializable {
    private final short apiVersion;
    private final String listenerName;
    private final boolean isInternal;
    private final InetAddress inetAddress;
    private final List<FlussPrincipal> principals;

    public Session(
            short apiVersion,
            String listenerName,
            boolean isInternal,
            InetAddress inetAddress,
            FlussPrincipal principal) {
        this(
                apiVersion,
                listenerName,
                isInternal,
                inetAddress,
                Collections.singletonList(principal));
    }

    /** Creates a session with an immutable snapshot of the authenticated principals. */
    public Session(
            short apiVersion,
            String listenerName,
            boolean isInternal,
            InetAddress inetAddress,
            Iterable<FlussPrincipal> principals) {
        this.apiVersion = apiVersion;
        this.listenerName = listenerName;
        this.isInternal = isInternal;
        this.inetAddress = inetAddress;
        this.principals = ImmutableList.copyOf(principals);
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

    /** Returns the client identity used for logging, user metrics, and resource ownership. */
    public FlussPrincipal getPrincipal() {
        return principals.isEmpty() ? null : principals.get(0);
    }

    /** Returns all authenticated principals used for authorization. */
    public List<FlussPrincipal> getPrincipals() {
        return principals;
    }

    public boolean isInternal() {
        return isInternal;
    }
}
