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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/** A blocking queue channel that can receive requests and send responses. */
@ThreadSafe
public final class RequestChannel {
    private static final Logger LOG = LoggerFactory.getLogger(RequestChannel.class);

    private final BlockingQueue<RpcRequest> requestQueue;

    public RequestChannel(int queueCapacity) {
        this.requestQueue = new ArrayBlockingQueue<>(queueCapacity);
    }

    /**
     * Send a request to be handled, potentially blocking until there is room in the queue for the
     * request.
     */
    public void putRequest(RpcRequest request) throws Exception {
        requestQueue.put(request);
    }

    /**
     * Sends a shutdown request to the channel. This can allow request processor gracefully
     * shutdown.
     */
    public void putShutdownRequest() throws Exception {
        putRequest(ShutdownRequest.INSTANCE);
    }

    /**
     * Get the next request or block until specified time has elapsed.
     *
     * @return the head of this queue, or null if the specified waiting time elapses before an
     *     element is available.
     */
    public RpcRequest pollRequest(long timeoutMs) {
        try {
            return requestQueue.poll(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while polling requests from channel queue.", e);
            return null;
        }
    }

    /** Get the number of requests in the queue. */
    int requestsCount() {
        return requestQueue.size();
    }
}
