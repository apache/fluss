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

import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.fluss.utils.Preconditions.checkState;

/** A worker's view of the TCP connection queues in its listener resource group. */
@ThreadSafe
public class RequestChannel {
    private final Group group;
    private final Condition available;
    private final ArrayDeque<Connection> ready = new ArrayDeque<>();
    private Connection running;
    private boolean waiting;
    private boolean stealWakeup;
    // Updates still use the group lock. Per-worker gauges read a single snapshot.
    private volatile int queued;

    /** Creates a standalone request channel. */
    public RequestChannel(int queueCapacity) {
        this(new Group(1, queueCapacity, queueCapacity));
        group.workers[0] = this;
    }

    private RequestChannel(Group group) {
        this.group = group;
        this.available = group.lock.newCondition();
    }

    static RequestChannel[] forGroup(int workers, int capacity, int perConnection) {
        Group group = new Group(workers, capacity, perConnection);
        RequestChannel[] channels = group.workers;
        for (int i = 0; i < workers; i++) {
            channels[i] = new RequestChannel(group);
        }
        return channels;
    }

    /** Creates an initially empty TCP connection with a callback to schedule read resumption. */
    public Connection newConnection(Runnable scheduleResume) {
        return new Connection(this, scheduleResume);
    }

    /** Reserves a slot before delivering the next complete frame to the protocol handler. */
    public boolean acquireRead(Connection connection) {
        group.lock.lock();
        try {
            if (group.closed || connection.closed) {
                return false;
            }
            if (connection.readPermit) {
                return true;
            }
            if (connection.pending.size() == group.perConnection) {
                connection.wait = ReadWait.LOCAL_FRAME;
            } else if (group.used == group.capacity) {
                connection.wait = ReadWait.GROUP_FRAME;
                group.readWaiters.add(connection);
            } else {
                group.reserve(connection);
                return true;
            }
            return false;
        } finally {
            group.lock.unlock();
        }
    }

    boolean canReadSocket(Connection connection) {
        group.lock.lock();
        try {
            return !group.closed && connection.pending.size() < group.perConnection;
        } finally {
            group.lock.unlock();
        }
    }

    /** Returns an unused permit after inline processing or parsing failure. */
    public void returnRead(Connection connection) {
        group.lock.lock();
        try {
            group.returnPermit(connection);
        } finally {
            group.lock.unlock();
        }
    }

    /** Stops admitting frames and releases the connection's unused reading permit. */
    public void closeConnection(Connection connection) {
        group.lock.lock();
        try {
            if (connection.wait == ReadWait.GROUP_FRAME) {
                group.readWaiters.remove(connection);
            }
            connection.wait = ReadWait.NONE;
            connection.closed = true;
            group.returnPermit(connection);
        } finally {
            group.lock.unlock();
        }
    }

    /**
     * Converts the read permit into a queued request. Returns whether the local FIFO reached its
     * limit.
     */
    public boolean putRequest(Connection connection, RpcRequest request) {
        group.lock.lock();
        try {
            checkState(!group.closed && !connection.closed, "Connection is closed for requests.");
            checkState(connection.readPermit, "Request has no reading permit.");
            connection.readPermit = false;
            RequestChannel owner = connection.owner;
            connection.pending.addLast(request);
            int size = connection.pending.size();
            owner.queued++;
            if (size == 1 && owner.running != connection) {
                owner.makeReady(connection);
            }
            if (size == group.perConnection) {
                connection.wait = ReadWait.LOCAL_IDLE;
                return true;
            }
            return false;
        } finally {
            group.lock.unlock();
        }
    }

    /** Claims a connection head and advances that connection only after synchronous completion. */
    public RpcRequest pollRequest(long timeoutMs) {
        group.lock.lock();
        try {
            checkState(running == null, "Previous request has not finished.");
            long remaining = TimeUnit.MILLISECONDS.toNanos(timeoutMs);
            Connection connection;
            while ((connection = claimReady()) == null) {
                if (group.closed) {
                    return ShutdownRequest.INSTANCE;
                }
                if (remaining <= 0) {
                    return null;
                }
                waiting = true;
                try {
                    remaining = available.awaitNanos(remaining);
                } finally {
                    waiting = false;
                }
            }
            running = connection;
            queued--;
            signalThief();
            if (stealWakeup) {
                stealWakeup = false;
                // Local work may have arrived before this worker resumed its steal.
                for (RequestChannel donor : group.workers) {
                    if (donor != this && donor.signalThief()) {
                        break;
                    }
                }
            }
            RpcRequest request = connection.pending.removeFirst();
            group.releaseQueueSlot(connection);
            return request;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } finally {
            group.lock.unlock();
        }
    }

    /** Makes the next request from the claimed connection eligible to run. */
    public void requestFinished() {
        group.lock.lock();
        try {
            Connection connection = running;
            running = null;
            if (!connection.pending.isEmpty()) {
                makeReady(connection);
            }
        } finally {
            group.lock.unlock();
        }
    }

    /** Returns the number of requests queued for this worker. */
    public int requestsCount() {
        return queued;
    }

    int groupRequestsCount() {
        group.lock.lock();
        try {
            int total = 0;
            for (RequestChannel worker : group.workers) {
                total += worker.queued;
            }
            return total;
        } finally {
            group.lock.unlock();
        }
    }

    void close() {
        group.lock.lock();
        try {
            if (group.closed) {
                return;
            }
            group.closed = true;
            group.readWaiters.clear();
            for (RequestChannel worker : group.workers) {
                worker.signal();
            }
        } finally {
            group.lock.unlock();
        }
    }

    private void makeReady(Connection connection) {
        connection.readyOrder = group.nextReadyOrder++;
        ready.addLast(connection);
        signal();
        signalThief();
    }

    private Connection claimReady() {
        if (!ready.isEmpty()) {
            return ready.removeFirst();
        }
        Connection oldest = null;
        for (RequestChannel donor : group.workers) {
            if (!donor.canShare()) {
                continue;
            }
            Connection head = donor.ready.getFirst();
            if (donor.running == null) {
                // Keep the donor's first head available for its local claim.
                Iterator<Connection> candidates = donor.ready.iterator();
                candidates.next();
                head = candidates.next();
            }
            if (oldest == null || head.readyOrder < oldest.readyOrder) {
                oldest = head;
            }
        }
        if (oldest != null) {
            RequestChannel donor = oldest.owner;
            donor.ready.removeFirstOccurrence(oldest);
            int pending = oldest.pending.size();
            donor.queued -= pending;
            queued += pending;
            oldest.owner = this;
            donor.signalThief();
            return oldest;
        }
        return null;
    }

    private boolean canShare() {
        // Leave a head for the donor if it is between handlers.
        return ready.size() > (running == null ? 1 : 0);
    }

    private boolean signalThief() {
        if (canShare()) {
            for (RequestChannel worker : group.workers) {
                if (worker.waiting && worker != this) {
                    worker.stealWakeup = true;
                    worker.signal();
                    return true;
                }
            }
        }
        return false;
    }

    private void signal() {
        if (waiting) {
            waiting = false;
            available.signal();
        }
    }

    /** The FIFO and current worker assignment for one TCP connection. */
    public static final class Connection {
        private final ArrayDeque<RpcRequest> pending = new ArrayDeque<>(1);
        private final Runnable scheduleResume;
        private RequestChannel owner;
        private long readyOrder;
        private ReadWait wait = ReadWait.NONE;
        private boolean readPermit;
        private boolean closed;

        private Connection(RequestChannel owner, Runnable scheduleResume) {
            this.owner = owner;
            this.scheduleResume = scheduleResume;
        }
    }

    private enum ReadWait {
        NONE,
        LOCAL_IDLE,
        LOCAL_FRAME,
        GROUP_FRAME
    }

    private static final class Group {
        private final ReentrantLock lock = new ReentrantLock();
        private final RequestChannel[] workers;
        private final int capacity;
        private final int perConnection;
        private final LinkedHashSet<Connection> readWaiters = new LinkedHashSet<>();
        private int used;
        private long nextReadyOrder;
        private boolean closed;

        private Group(int workers, int capacity, int perConnection) {
            this.workers = new RequestChannel[workers];
            this.capacity = Math.max(1, capacity);
            this.perConnection = Math.max(1, perConnection);
        }

        private void reserve(Connection connection) {
            used++;
            connection.readPermit = true;
            connection.wait = ReadWait.NONE;
        }

        private void returnPermit(Connection connection) {
            if (connection.readPermit) {
                connection.readPermit = false;
                used--;
                grantWaitingRead();
            }
        }

        private void releaseQueueSlot(Connection connection) {
            used--;
            ReadWait wait = connection.wait;
            if (wait == ReadWait.LOCAL_FRAME) {
                connection.wait = ReadWait.GROUP_FRAME;
                readWaiters.add(connection);
            } else if (wait == ReadWait.LOCAL_IDLE) {
                connection.wait = ReadWait.NONE;
                if (!closed) {
                    scheduleResume(connection);
                }
            }
            grantWaitingRead();
        }

        private void grantWaitingRead() {
            while (!closed && used < capacity && !readWaiters.isEmpty()) {
                Iterator<Connection> iterator = readWaiters.iterator();
                Connection connection = iterator.next();
                iterator.remove();
                reserve(connection);
                if (scheduleResume(connection)) {
                    return;
                }
                connection.readPermit = false;
                used--;
            }
        }

        private boolean scheduleResume(Connection connection) {
            try {
                connection.scheduleResume.run();
                return true;
            } catch (RejectedExecutionException ignored) {
                connection.closed = true;
                return false;
            }
        }
    }
}
