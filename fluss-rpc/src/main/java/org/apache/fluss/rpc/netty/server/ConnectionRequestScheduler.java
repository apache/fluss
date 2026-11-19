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

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.protocol.RequestType;
import org.apache.fluss.shaded.netty4.io.netty.channel.Channel;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelId;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.utils.ExceptionUtils.rethrowIfFatalErrorOrOOM;

/** Owns the connection queues and the shared index of local bucket writes. */
final class ConnectionRequestScheduler {
    // Guards bucket indexes and all mutable Queue, Connection and Parent state.
    private final Object lock = new Object();

    private final Map<TableBucket, Bucket> buckets = new HashMap<>();
    private final boolean splitBuckets;
    private final boolean handoff;
    private final List<Queue> queues = new ArrayList<>();
    private final LinkedHashSet<Connection> pendingReads = new LinkedHashSet<>();

    ConnectionRequestScheduler(boolean splitBuckets) {
        this(splitBuckets, false);
    }

    ConnectionRequestScheduler(boolean splitBuckets, boolean handoff) {
        this.splitBuckets = splitBuckets;
        this.handoff = handoff;
    }

    final class Queue extends RequestChannel {
        private final Map<ChannelId, Connection> connections = new HashMap<>();
        private final LinkedHashSet<Connection> ready = new LinkedHashSet<>();
        private final LinkedHashSet<Connection> members = new LinkedHashSet<>();
        private final int high;
        private final Object group;
        private volatile int depth;
        private boolean paused;
        private int running;
        private boolean sealed;

        Queue(int threshold) {
            this(threshold, ConnectionRequestScheduler.this);
        }

        Queue(int threshold, Object group) {
            super(threshold);
            this.high = Math.max(1, threshold);
            this.group = group;
            queues.add(this);
        }

        @Override
        public void registerChannel(Channel channel) {
            synchronized (lock) {
                Connection connection =
                        connections.computeIfAbsent(channel.id(), key -> new Connection(this));
                connection.channel = channel;
                connection.owner.members.add(connection);
            }
        }

        @Override
        public void unregisterChannel(Channel channel) {
            synchronized (lock) {
                Connection connection = connections.remove(channel.id());
                if (connection != null) {
                    connection.channel = null;
                    connection.owner.members.remove(connection);
                }
            }
        }

        private void updatePressure() {
            boolean next = paused ? depth > high / 2 : depth >= high;
            if (next != paused) {
                paused = next;
                pendingReads.addAll(members);
            }
        }

        boolean offer(ChannelId id, FlussRequest request) {
            synchronized (lock) {
                Connection connection =
                        connections.computeIfAbsent(id, key -> new Connection(this));
                Queue owner = connection.owner;
                if (sealed || owner.sealed) {
                    return false;
                }
                connection.incoming.addLast(request);
                owner.depth++;
                connection.depth++;
                owner.refresh(connection);
                tryPush(owner);
                owner.updatePressure();
                if (!connection.receivedRequest && connection.owner.paused) {
                    pendingReads.add(connection);
                }
                connection.receivedRequest = true;
                lock.notifyAll();
            }
            applyReadUpdates();
            return true;
        }

        @Override
        public RpcRequest pollRequest(long timeoutMs) {
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
            synchronized (lock) {
                while (ready.isEmpty()) {
                    if (tryPull(this)) {
                        break;
                    }
                    if (sealed && depth == 0) {
                        return ShutdownRequest.INSTANCE;
                    }
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0) {
                        return null;
                    }
                    try {
                        TimeUnit.NANOSECONDS.timedWait(lock, remaining);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return null;
                    }
                }
                Iterator<Connection> iterator = ready.iterator();
                Connection connection = iterator.next();
                iterator.remove();
                running++;
                Step step = firstReadyStep(connection);
                if (step != null) {
                    connection.running = true;
                    return new Work(this, connection, null, step);
                }
                FlussRequest request = connection.incoming.removeFirst();
                connection.running = true;
                return new Work(this, connection, request, null);
            }
        }

        void refresh(Connection connection) {
            if (!connection.running
                    && (firstReadyStep(connection) != null
                            || (!connection.incoming.isEmpty()
                                    && (connection.pending.isEmpty()
                                            || (splitBuckets
                                                    && FlussRequestHandler.canPrepareBucketRequest(
                                                            connection
                                                                    .incoming
                                                                    .peekFirst()
                                                                    .getMessage())))))) {
                ready.add(connection);
            }
        }

        @Override
        public int requestsCount() {
            return depth;
        }

        @Override
        public void putShutdownRequest() {
            synchronized (lock) {
                sealed = true;
                lock.notifyAll();
            }
        }

        @Override
        public void putRequest(RpcRequest request) {
            throw new UnsupportedOperationException(
                    "Connection identity is required for scheduling");
        }
    }

    final class Work implements RpcRequest {
        private final Queue queue;
        private final Connection connection;
        private final FlussRequest input;
        private final Step step;

        Work(Queue queue, Connection connection, FlussRequest input, Step step) {
            this.queue = queue;
            this.connection = connection;
            this.input = input;
            this.step = step;
        }

        void execute(FlussRequestHandler handler) {
            applyReadUpdates();
            Parent parent = step == null ? null : step.parent;
            boolean finished = false;
            boolean inputReleased = false;
            try {
                if (input != null) {
                    if (!splitBuckets
                            || !FlussRequestHandler.canPrepareBucketRequest(input.getMessage())) {
                        handler.processRequest(input);
                        finished = true;
                        return;
                    }
                    BucketRequestExecution execution = handler.prepareBucketRequest(input);
                    if (execution == null) {
                        handler.processRequest(input);
                        finished = true;
                        return;
                    }
                    parent = new Parent(connection, input, execution);
                    synchronized (lock) {
                        connection.pending.add(parent);
                        for (TableBucket bucket : execution.buckets()) {
                            Bucket waiting =
                                    buckets.computeIfAbsent(bucket, ignored -> new Bucket());
                            Step item = new Step(parent, bucket, waiting.nextOrder++);
                            parent.steps.add(item);
                            waiting.add(item);
                        }
                    }
                    while (executeNext(parent) && !hasOtherReadyConnection()) {
                        // Consume the original frame for as long as this connection has the worker.
                    }
                    execution.detachInputs();
                    inputReleased = true;
                    input.releaseScheduledInput();
                    synchronized (lock) {
                        Iterator<Step> iterator = parent.steps.iterator();
                        while (iterator.hasNext()) {
                            Step item = iterator.next();
                            if (!execution.hasInput(item.bucket)) {
                                iterator.remove();
                                unlink(item);
                            }
                        }
                    }
                } else {
                    executeStep(step);
                }
                if (parent.steps.isEmpty()) {
                    handler.completeBucketRequest(parent.request, parent.execution.finish());
                    finished = true;
                }
            } catch (Throwable error) {
                finished = true;
                FlussRequest failed = input == null ? parent.request : input;
                failed.fail(error);
                rethrowIfFatalErrorOrOOM(error);
            } finally {
                try {
                    if (input != null && !inputReleased) {
                        input.releaseScheduledInput();
                    }
                } finally {
                    synchronized (lock) {
                        if (finished) {
                            if (parent != null) {
                                for (Step remaining : parent.steps) {
                                    unlink(remaining);
                                }
                                connection.pending.remove(parent);
                            }
                            queue.depth--;
                            connection.depth--;
                        }
                        queue.running--;
                        connection.running = false;
                        connection.hasRunSinceHandoff = true;
                        queue.refresh(connection);
                        tryPush(queue);
                        queue.updatePressure();
                        lock.notifyAll();
                    }
                    applyReadUpdates();
                }
            }
        }

        private boolean hasOtherReadyConnection() {
            synchronized (lock) {
                return !queue.ready.isEmpty();
            }
        }

        private boolean executeNext(Parent parent) {
            Step next = null;
            synchronized (lock) {
                for (Step item : parent.steps) {
                    if (eligible(item)) {
                        next = item;
                        break;
                    }
                }
            }
            if (next == null) {
                return false;
            }
            executeStep(next);
            return true;
        }

        @Override
        public RequestType getRequestType() {
            return RequestType.FLUSS;
        }

        @Override
        public void releaseBuffer() {
            // The original request is released by its first Work, before making the connection
            // ready.
        }
    }

    private boolean tryPull(Queue target) {
        if (!handoff || target.sealed) {
            return false;
        }
        Queue donor = null;
        Connection candidate = null;
        for (Queue source : queues) {
            if (source == target
                    || source.group != target.group
                    || source.sealed
                    || source.ready.size() + source.running < 2) {
                continue;
            }
            for (Connection connection : source.ready) {
                if (connection.hasRunSinceHandoff
                        && (donor == null
                                || source.ready.size() > donor.ready.size()
                                || (source.ready.size() == donor.ready.size()
                                        && source.depth > donor.depth))) {
                    donor = source;
                    candidate = connection;
                    break;
                }
            }
        }
        if (candidate == null) {
            return false;
        }
        transfer(candidate, target);
        return true;
    }

    private void tryPush(Queue source) {
        if (!handoff || source.sealed || source.depth < Math.max(1, source.high - 1)) {
            return;
        }
        Queue target = null;
        for (Queue queue : queues) {
            if (queue != source
                    && queue.group == source.group
                    && !queue.sealed
                    && (target == null || queue.depth < target.depth)) {
                target = queue;
            }
        }
        if (target == null) {
            return;
        }
        Connection candidate = null;
        for (Connection connection : source.ready) {
            int destinationDepth = target.depth + connection.depth;
            if (connection.hasRunSinceHandoff
                    && destinationDepth < target.high
                    && destinationDepth < source.depth
                    && connection.depth > 0) {
                candidate = connection;
                break;
            }
        }
        if (candidate != null) {
            transfer(candidate, target);
        }
    }

    private void transfer(Connection connection, Queue target) {
        Queue source = connection.owner;
        source.ready.remove(connection);
        source.members.remove(connection);
        source.depth -= connection.depth;
        target.depth += connection.depth;
        connection.owner = target;
        connection.hasRunSinceHandoff = false;
        if (connection.channel != null) {
            target.members.add(connection);
            pendingReads.add(connection);
        }
        target.refresh(connection);
        source.updatePressure();
        target.updatePressure();
        lock.notifyAll();
    }

    private void applyReadUpdates() {
        List<Connection> updates;
        synchronized (lock) {
            if (pendingReads.isEmpty()) {
                return;
            }
            updates = new ArrayList<>(pendingReads);
            pendingReads.clear();
        }
        for (Connection connection : updates) {
            Channel channel;
            synchronized (lock) {
                channel = connection.channel;
            }
            if (channel == null || !channel.isActive()) {
                continue;
            }
            try {
                channel.eventLoop()
                        .execute(
                                () -> {
                                    boolean autoRead;
                                    synchronized (lock) {
                                        autoRead = !connection.owner.paused;
                                    }
                                    if (channel.isActive()
                                            && channel.config().isAutoRead() != autoRead) {
                                        channel.config().setAutoRead(autoRead);
                                    }
                                });
            } catch (RejectedExecutionException e) {
                if (!channel.eventLoop().isShuttingDown()) {
                    channel.close();
                }
            }
        }
    }

    private void executeStep(Step item) {
        try {
            item.parent.execution.execute(item.bucket);
        } finally {
            synchronized (lock) {
                item.parent.steps.remove(item);
                unlink(item);
                lock.notifyAll();
            }
        }
    }

    private void unlink(Step item) {
        Bucket waiting = buckets.get(item.bucket);
        boolean opensReadPhase = Bucket.first(waiting.writes) == item;
        waiting.remove(item);
        if (waiting.steps.isEmpty()) {
            buckets.remove(item.bucket);
            return;
        }
        if (opensReadPhase) {
            // Publish the newly eligible read prefix once. Read eligibility is constant time.
            for (Step next : waiting.steps) {
                refreshStep(next);
                if (!next.parent.execution.isRead()) {
                    break;
                }
            }
        } else {
            refreshStep(Bucket.first(waiting.steps));
            LinkedHashSet<Step> sameConnection = waiting.byConnection.get(item.parent.connection);
            if (sameConnection != null) {
                refreshStep(Bucket.first(sameConnection));
            }
        }
    }

    private void refreshStep(Step step) {
        if (eligible(step)) {
            Connection connection = step.parent.connection;
            connection.owner.refresh(connection);
        }
    }

    private boolean eligible(Step step) {
        return buckets.get(step.bucket).eligible(step);
    }

    private static final class Bucket {
        private final LinkedHashSet<Step> steps = new LinkedHashSet<>();
        private final LinkedHashSet<Step> writes = new LinkedHashSet<>();
        private final Map<Connection, LinkedHashSet<Step>> byConnection = new HashMap<>();
        private long nextOrder;

        void add(Step step) {
            steps.add(step);
            byConnection
                    .computeIfAbsent(step.parent.connection, ignored -> new LinkedHashSet<>())
                    .add(step);
            if (!step.parent.execution.isRead()) {
                writes.add(step);
            }
        }

        void remove(Step step) {
            steps.remove(step);
            writes.remove(step);
            LinkedHashSet<Step> connectionSteps = byConnection.get(step.parent.connection);
            connectionSteps.remove(step);
            if (connectionSteps.isEmpty()) {
                byConnection.remove(step.parent.connection);
            }
        }

        boolean eligible(Step step) {
            if (!step.parent.execution.isRead()) {
                return first(steps) == step;
            }
            return (writes.isEmpty() || step.order < first(writes).order)
                    && first(byConnection.get(step.parent.connection)) == step;
        }

        static Step first(LinkedHashSet<Step> steps) {
            return steps.isEmpty() ? null : steps.iterator().next();
        }
    }

    private Step firstReadyStep(Connection connection) {
        for (Parent parent : connection.pending) {
            for (Step step : parent.steps) {
                if (eligible(step)) {
                    return step;
                }
            }
        }
        return null;
    }

    final class Connection {
        private Queue owner;
        private Channel channel;
        private boolean hasRunSinceHandoff = true;
        private boolean receivedRequest;
        private int depth;
        private final Deque<FlussRequest> incoming = new ArrayDeque<>();
        private final LinkedHashSet<Parent> pending = new LinkedHashSet<>();
        private boolean running;

        Connection(Queue owner) {
            this.owner = owner;
        }
    }

    private static final class Parent {
        private final Connection connection;
        private final FlussRequest request;
        private final BucketRequestExecution execution;
        private final LinkedHashSet<Step> steps = new LinkedHashSet<>();

        Parent(Connection connection, FlussRequest request, BucketRequestExecution execution) {
            this.connection = connection;
            this.request = request;
            this.execution = execution;
        }
    }

    private static final class Step {
        private final Parent parent;
        private final TableBucket bucket;

        private final long order;

        Step(Parent parent, TableBucket bucket, long order) {
            this.parent = parent;
            this.bucket = bucket;
            this.order = order;
        }
    }
}
