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

import java.util.Iterator;

/** A testing request channel that can receive requests and deal with requests. */
final class TestingRequestChannel extends RequestChannel {

    public TestingRequestChannel(int queueCapacity) {
        super(queueCapacity);
    }

    public RpcRequest getAndRemoveRequest(int index) {
        if (requestQueue.isEmpty()) {
            throw new IllegalStateException("No requests pending for request channel.");
        }

        // Index out of bounds check.
        if (index >= requestQueue.size()) {
            throw new IllegalArgumentException(
                    "Index " + index + " is out of bounds for request channel.");
        }

        int currentIndex = 0;
        RpcRequest request = null;
        for (Iterator<RpcRequest> it = requestQueue.iterator(); it.hasNext(); ) {
            RpcRequest rpcRequest = it.next();
            if (currentIndex == index) {
                it.remove();
                request = rpcRequest;
                break;
            }
            currentIndex++;
        }

        return request;
    }
}
