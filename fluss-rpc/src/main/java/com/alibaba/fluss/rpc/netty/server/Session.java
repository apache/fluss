/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.rpc.netty.server;

import java.io.Serializable;

/** The connection session of a request. */
public class Session implements Serializable {
    private final short apiVersion;
    private final String listenerName;

    public Session(short apiVersion, String listenerName) {
        this.apiVersion = apiVersion;
        this.listenerName = listenerName;
    }

    public short getApiVersion() {
        return apiVersion;
    }

    public String getListenerName() {
        return listenerName;
    }
}
