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

import org.apache.fluss.shaded.netty4.io.netty.channel.Channel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A queue channel that can receive requests and send responses.
 *
 * <p>Uses an unbounded LinkedBlockingQueue to ensure that putRequest() never blocks, preventing
 * EventLoop threads from being blocked. Backpressure is applied at the TCP level by pausing channel
 * reads when the queue size exceeds the backpressure threshold.
 *
 * <p>Each RequestChannel instance manages its own associated Netty channels (those hashed to this
 * RequestChannel) and independently controls their backpressure state. This design encapsulates all
 * backpressure logic within the RequestChannel, eliminating the need for global state management.
 */
@ThreadSafe
public class RequestChannel {
    private static final Logger LOG = LoggerFactory.getLogger(RequestChannel.class);

    /** Unbounded blocking queue to hold incoming requests. Never blocks on put. */
    protected final BlockingQueue<RpcRequest> requestQueue;

    /**
     * The threshold at which backpressure should be applied (pausing channel reads). When queue
     * size exceeds this value, channels should be paused to prevent memory exhaustion.
     */
    private final int backpressureThreshold;

    /**
     * The threshold at which to resume paused channels. Set to 50% of backpressureThreshold to
     * provide hysteresis and avoid thrashing.
     */
    private final int resumeThreshold;

    /**
     * All Netty channels that are hashed to this RequestChannel. Channels are registered when they
     * become active and unregistered when they become inactive.
     *
     * <p>When backpressure is applied, ALL channels in this set are paused simultaneously. When
     * backpressure is released, ALL channels are resumed simultaneously.
     */
    private final Set<Channel> associatedChannels = ConcurrentHashMap.newKeySet();

    /**
     * Indicates whether backpressure is currently active. When true, all associated channels have
     * been paused (setAutoRead(false)). When false, all channels are running normally.
     *
     * <p>Volatile ensures visibility for fast-path reads (outside the lock). All modifications are
     * protected by backpressureLock, so atomicity is guaranteed by the lock, not by atomic
     * operations.
     */
    private volatile boolean isBackpressureActive = false;

    /**
     * Lock to protect backpressure state transitions and task submissions. This lock ensures that:
     * 1. State checks and task submissions are atomic (preventing permanent channel blocking) 2.
     * Pause and resume operations are mutually exclusive 3. New channel registration correctly
     * synchronizes with current backpressure state
     *
     * <p>The lock eliminates the need for CAS operations - simple boolean checks and assignments
     * under the lock are sufficient for correctness.
     */
    private final ReentrantLock backpressureLock = new ReentrantLock();

    public RequestChannel(int backpressureThreshold) {
        this.requestQueue = new LinkedBlockingQueue<>();
        this.backpressureThreshold = backpressureThreshold;
        this.resumeThreshold = backpressureThreshold / 2;
    }

    /**
     * Send a request to be handled. Since this uses an unbounded queue, this method never blocks,
     * ensuring EventLoop threads are never blocked by queue operations.
     *
     * <p>After adding the request, automatically checks if backpressure should be applied. If the
     * queue size exceeds the backpressure threshold, ALL channels associated with this
     * RequestChannel will be paused to prevent further memory growth.
     *
     * <p>OPTIMIZATION: Only check backpressure if not already active (avoid redundant checks).
     */
    public void putRequest(RpcRequest request) {
        requestQueue.add(request);

        // CRITICAL OPTIMIZATION: Skip check if already in backpressure state.
        // This avoids lock contention on every putRequest() call when system is under pressure.
        // The volatile read is very cheap compared to lock acquisition.
        if (!isBackpressureActive) {
            pauseAllChannelsIfNeeded();
        }
    }

    /**
     * Sends a shutdown request to the channel. This can allow request processor gracefully
     * shutdown.
     */
    public void putShutdownRequest() {
        putRequest(ShutdownRequest.INSTANCE);
    }

    /**
     * Get the next request, waiting up to the specified timeout if the queue is empty. After
     * successfully polling a request, attempts to resume paused channels if the queue size has
     * dropped below the resume threshold.
     *
     * @return the head of this queue, or null if the specified waiting time elapses before an
     *     element is available.
     */
    public RpcRequest pollRequest(long timeoutMs) {
        try {
            RpcRequest request = requestQueue.poll(timeoutMs, TimeUnit.MILLISECONDS);
            if (isBackpressureActive) {
                tryResumeChannels();
            }
            return request;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    /** Get the number of requests in the queue. */
    public int requestsCount() {
        return requestQueue.size();
    }

    /**
     * Registers a Netty channel as being associated with this RequestChannel. This is called when a
     * channel becomes active and is hashed to this RequestChannel.
     *
     * <p>IMPORTANT: New channels are NOT immediately paused even if backpressure is active. This is
     * critical for system health: 1. Health check connections must not be blocked at startup 2. New
     * connections will naturally be controlled by the next backpressure check 3. Immediately
     * pausing new connections can cause deadlock at startup when queue is full but processing is
     * slow
     *
     * @param channel the channel to register
     */
    public void registerChannel(Channel channel) {
        associatedChannels.add(channel);

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Registered channel {} to RequestChannel (backpressure threshold: {}, associated channels: {}, backpressure active: {})",
                    channel.remoteAddress(),
                    backpressureThreshold,
                    associatedChannels.size(),
                    isBackpressureActive);
        }
    }

    /**
     * Unregisters a Netty channel from this RequestChannel. This is called when a channel becomes
     * inactive.
     *
     * @param channel the channel to unregister
     */
    public void unregisterChannel(Channel channel) {
        associatedChannels.remove(channel);
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Unregistered channel {} from RequestChannel (associated channels: {}, backpressure active: {})",
                    channel.remoteAddress(),
                    associatedChannels.size(),
                    isBackpressureActive);
        }
    }

    /**
     * Check if the queue size has exceeded the backpressure threshold. When true, channel reads
     * should be paused to prevent memory exhaustion.
     */
    private boolean shouldApplyBackpressure() {
        return requestQueue.size() >= backpressureThreshold;
    }

    /**
     * Check if the queue size has dropped below the resume threshold. When true, paused channels
     * can be resumed to accept new requests.
     */
    private boolean shouldResumeChannels() {
        return requestQueue.size() <= resumeThreshold;
    }

    /**
     * Pauses ALL channels associated with this RequestChannel if the queue size exceeds the
     * backpressure threshold. This ensures that when the queue is full, all channels stop sending
     * requests to prevent memory exhaustion.
     *
     * <p>Uses a lock to protect the entire operation (state check + state change + task submission)
     * as an atomic unit. This prevents race conditions with resume operations and channel
     * registrations.
     *
     * <p>TODO: In the future, consider pausing only a subset of channels instead of all channels to
     * reduce the impact on upstream traffic. A selective pause strategy could minimize disruption
     * to the overall system while still providing effective backpressure control.
     */
    private void pauseAllChannelsIfNeeded() {
        if (!shouldApplyBackpressure()) {
            return;
        }

        // Lock protects: state check + state change + task submission as atomic operation
        backpressureLock.lock();
        try {
            // Check if already in backpressure state
            if (isBackpressureActive) {
                return; // Already paused, nothing to do
            }

            // Activate backpressure and pause all channels
            isBackpressureActive = true;

            for (Channel channel : associatedChannels) {
                if (channel.isActive()) {
                    // Submit to the channel's EventLoop to ensure thread safety
                    channel.eventLoop()
                            .execute(
                                    () -> {
                                        if (channel.isActive() && channel.config().isAutoRead()) {
                                            channel.config().setAutoRead(false);
                                            LOG.warn(
                                                    "Queue size ({}) exceeded backpressure threshold ({}), paused channel: {}",
                                                    requestsCount(),
                                                    backpressureThreshold,
                                                    channel.remoteAddress());
                                        }
                                    });
                }
            }
        } finally {
            backpressureLock.unlock();
        }
    }

    /**
     * Attempts to resume all associated channels if the queue size has dropped below the resume
     * threshold. This method is called automatically after a request is dequeued.
     *
     * <p>Uses a lock to protect the entire operation (state check + state change + task submission)
     * as an atomic unit. This prevents race conditions with pause operations and channel
     * registrations.
     */
    private void tryResumeChannels() {
        if (!shouldResumeChannels()) {
            return;
        }

        // Lock protects: state check + state change + task submission as atomic operation
        backpressureLock.lock();
        try {
            // Check if backpressure is not active
            if (!isBackpressureActive) {
                return; // Already resumed, nothing to do
            }

            // Deactivate backpressure and resume all channels
            isBackpressureActive = false;

            for (Channel channel : associatedChannels) {
                if (channel.isActive()) {
                    // Submit resume task to the channel's EventLoop to ensure thread safety
                    channel.eventLoop()
                            .execute(
                                    () -> {
                                        if (channel.isActive() && !channel.config().isAutoRead()) {
                                            channel.config().setAutoRead(true);
                                            LOG.info(
                                                    "Queue size ({}) dropped below resume threshold ({}), resumed channel: {}",
                                                    requestsCount(),
                                                    resumeThreshold,
                                                    channel.remoteAddress());
                                        }
                                    });
                }
            }
        } finally {
            backpressureLock.unlock();
        }
    }
}
