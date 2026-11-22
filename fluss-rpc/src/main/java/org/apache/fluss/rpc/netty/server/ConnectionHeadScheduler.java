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

import org.apache.fluss.rpc.messages.LookupRequest;
import org.apache.fluss.rpc.messages.PbLookupReqForBucket;
import org.apache.fluss.rpc.messages.PbPrefixLookupReqForBucket;
import org.apache.fluss.rpc.messages.PbPutKvReqForBucket;
import org.apache.fluss.rpc.messages.PrefixLookupRequest;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.shaded.netty4.io.netty.channel.Channel;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.fluss.shaded.netty4.io.netty.util.Attribute;
import org.apache.fluss.shaded.netty4.io.netty.util.AttributeKey;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import static org.apache.fluss.rpc.util.CommonRpcMessageUtils.hasHistoricalLookup;
import static org.apache.fluss.rpc.util.CommonRpcMessageUtils.hasHistoricalPut;
import static org.apache.fluss.utils.Preconditions.checkState;

/** Schedules whole connection heads across the native request processors of one TabletServer. */
final class ConnectionHeadScheduler {
    private static final int MAX_PENDING_REQUESTS = 2;
    private static final int SMALL_FRAME_BYTES = 64 * 1024;
    private static final long MAX_BYPASS_BYTES = 1024 * 1024;

    // The decoder and admission owner stay with the Netty connection.
    private static final AttributeKey<Connection> CONNECTION =
            AttributeKey.valueOf(ConnectionHeadScheduler.class, "connection");

    private final ReentrantLock lock = new ReentrantLock();
    private final Consumer<Entry> bucketReady = this::bucketReady;
    private final Map<BucketPage, PageState> bitmapPages = new HashMap<>();

    private long nextTicket;

    boolean offer(Owner owner, Channel channel, FlussRequest request) {
        Access access = Access.from(request);
        lock.lock();
        try {
            if (owner.admissionClosed()) {
                return false;
            }
            Connection connection = connection(owner, channel);
            checkState(connection.readGranted, "Request admitted without a read permit");
            checkState(connection.queued < MAX_PENDING_REQUESTS, "TCP request queue is full");
            connection.queued++;
            connection.readGranted = false;
            Entry entry = new Entry(connection, request, access);
            entry.bytes = connection.frameBytes;
            entry.bypassed = connection.bypassed;
            connection.frameBytes = 0;
            connection.bypassed = false;
            connection.add(entry);
            owner.queued++;
            Owner executor = connection.executor;
            executor.executionQueued++;
            if (connection.head == entry && !connection.running) {
                register(executor, entry);
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    boolean hasReadGrant(Owner owner, Channel channel) {
        if (owner.admissionClosed()) {
            return false;
        }
        Connection connection = channel.attr(CONNECTION).get();
        // Once the callback runs, its EventLoop owns consumption and cancellation.
        return connection != null && connection.readGranted;
    }

    private Connection connection(Owner owner, Channel channel) {
        Attribute<Connection> state = channel.attr(CONNECTION);
        Connection connection = state.get();
        if (connection == null) {
            connection = new Connection(owner);
            state.set(connection);
        }
        return connection;
    }

    boolean acquireRead(Owner owner, ChannelHandlerContext ctx, Runnable resume, int frameBytes) {
        ReadWaiter wake = null;
        lock.lock();
        try {
            if (owner.admissionClosed()) {
                if (ctx.channel().hasAttr(CONNECTION)) {
                    Connection existing = ctx.channel().attr(CONNECTION).get();
                    if (existing != null && existing.readGranted) {
                        wake = releaseReadLocked(owner, existing, true);
                    }
                }
                return false;
            }
            Connection connection = connection(owner, ctx.channel());
            if (connection.readGranted) {
                return true;
            }
            if (connection.reader == null) {
                connection.reader = new ReadWaiter(owner, ctx, resume);
            }
            PrefetchBudget budget = owner.group.budget;
            checkState(
                    frameBytes > 0 && frameBytes <= budget.limit,
                    "Invalid frame length for the listener budget");
            connection.frameBytes = frameBytes;
            if (connection.queued == MAX_PENDING_REQUESTS) {
                connection.waitingForPendingSlot = true;
                return false;
            }
            if (budget.waiters.isEmpty() && frameBytes <= budget.limit - budget.used) {
                grantRead(budget, connection);
                return true;
            }
            budget.enqueue(connection);
            if (budget.isSmall(connection)) {
                wake = grantNextRead(owner.group);
            }
            return false;
        } finally {
            lock.unlock();
            notifyReader(wake);
        }
    }

    /** Returns an unused read permit or removes a reader when its connection closes. */
    void releaseRead(Owner owner, Channel channel, boolean cancel) {
        Attribute<Connection> state = channel.hasAttr(CONNECTION) ? channel.attr(CONNECTION) : null;
        Connection connection = state == null ? null : state.get();
        if (connection == null || (!cancel && !connection.readGranted)) {
            return;
        }
        ReadWaiter next;
        lock.lock();
        try {
            next = releaseReadLocked(owner, connection, cancel);
        } finally {
            lock.unlock();
        }
        notifyReader(next);
    }

    private ReadWaiter releaseReadLocked(Owner owner, Connection connection, boolean cancel) {
        PrefetchBudget budget = owner.group.budget;
        if (cancel) {
            budget.remove(connection);
            connection.reader = null;
            connection.waitingForPendingSlot = false;
        }
        if (connection.readGranted) {
            connection.readGranted = false;
            budget.release(connection.frameBytes, connection.bypassed);
            connection.frameBytes = 0;
            connection.bypassed = false;
        }
        return grantNextRead(owner.group);
    }

    private static void grantRead(PrefetchBudget budget, Connection connection) {
        connection.readGranted = true;
        budget.used += connection.frameBytes;
    }

    private ReadWaiter grantNextRead(WorkerGroup group) {
        return group.closing ? null : grantBudgetReaders(group.budget);
    }

    private ReadWaiter grantBudgetReaders(PrefetchBudget budget) {
        if (budget.waiters.isEmpty()) {
            return null;
        }
        ReadWaiter head = null;
        ReadWaiter tail = null;
        long bypassLimit = 0;
        boolean bypassing = false;
        Iterator<Map.Entry<Connection, ReadWaiter>> waiters = budget.waiters.entrySet().iterator();
        while (waiters.hasNext()) {
            Map.Entry<Connection, ReadWaiter> entry = waiters.next();
            Connection connection = entry.getKey();
            int bytes = connection.frameBytes;
            long available = budget.limit - budget.used;
            if (bypassing) {
                if (bytes > available || bytes > bypassLimit - budget.bypassedBytes) {
                    break;
                }
            } else if (bytes > available) {
                if (budget.isSmall(connection)) {
                    break;
                }
                // After ordinary grants retire, the FIFO head must fit even if every bypass
                // byte is still charged. A new head may require existing bypass grants to drain.
                bypassLimit = Math.min(budget.maxBypassBytes, budget.limit - bytes);
                bypassing = true;
                waiters = budget.smallWaiters.entrySet().iterator();
                continue;
            }
            ReadWaiter waiter = entry.getValue();
            waiters.remove();
            if (bypassing) {
                budget.waiters.remove(connection);
                budget.bypassedBytes += bytes;
                connection.bypassed = true;
            } else {
                budget.smallWaiters.remove(connection);
            }
            grantRead(budget, connection);
            if (tail == null) {
                head = waiter;
            } else {
                tail.next = waiter;
            }
            tail = waiter;
        }
        return head;
    }

    private void notifyReader(ReadWaiter waiter) {
        while (waiter != null) {
            ReadWaiter next = waiter.next;
            waiter.next = null;
            try {
                waiter.ctx.executor().execute(waiter.resume);
            } catch (RejectedExecutionException ignored) {
                // This EventLoop will never consume the grant. Pass it to another reader.
                lock.lock();
                try {
                    ReadWaiter replacement =
                            releaseReadLocked(
                                    waiter.owner,
                                    connection(waiter.owner, waiter.ctx.channel()),
                                    true);
                    if (next == null) {
                        next = replacement;
                    } else if (replacement != null) {
                        ReadWaiter tail = next;
                        while (tail.next != null) {
                            tail = tail.next;
                        }
                        tail.next = replacement;
                    }
                } finally {
                    lock.unlock();
                }
            }
            waiter = next;
        }
    }

    RpcRequest take(Owner owner) throws InterruptedException {
        ReadWaiter waiter = null;
        lock.lock();
        try {
            RpcRequest request;
            while ((request = claim(owner)) == null) {
                owner.beginWait();
                try {
                    owner.available.await();
                } finally {
                    owner.endWait();
                }
            }
            if (request != ShutdownRequest.INSTANCE) {
                waiter = grantNextRead(owner.group);
            }
            return request;
        } finally {
            if (owner.helperNotified) {
                owner.acknowledgeHelper();
                signalHelper(owner.group);
            }
            lock.unlock();
            notifyReader(waiter);
        }
    }

    RpcRequest poll(Owner owner) {
        ReadWaiter waiter = null;
        lock.lock();
        try {
            RpcRequest request = claim(owner);
            if (request != null && request != ShutdownRequest.INSTANCE) {
                waiter = grantNextRead(owner.group);
            }
            return request;
        } finally {
            lock.unlock();
            notifyReader(waiter);
        }
    }

    private RpcRequest claim(Owner owner) {
        if (owner.running != null) {
            return null;
        }
        WorkerGroup group = owner.group;
        owner.acknowledgeHelper();
        Map.Entry<Long, Entry> local = owner.heads.pollFirstEntry();
        if (local != null) {
            return start(owner, local.getValue(), owner);
        }
        if (!group.closing && group.transferable > 0) {
            Entry candidate = findTransfer(owner);
            if (candidate != null) {
                Owner previous = candidate.connection.executor;
                previous.heads.remove(candidate.ticket);
                return start(owner, candidate, previous);
            }
        }
        return owner.closing && owner.executionQueued == 0 ? ShutdownRequest.INSTANCE : null;
    }

    private Entry findTransfer(Owner recipient) {
        Entry oldest = null;
        for (Owner source : recipient.group.owners) {
            if (source == recipient || source.transferable == 0) {
                continue;
            }
            Long ticket = source.heads.firstKey();
            if (source.running == null) {
                ticket = source.heads.higherKey(ticket);
            }
            if (oldest == null || ticket < oldest.ticket) {
                oldest = source.heads.get(ticket);
            }
        }
        return oldest;
    }

    private RpcRequest start(Owner executor, Entry entry, Owner previous) {
        Connection connection = entry.connection;
        Owner home = connection.home;
        if (executor != previous) {
            // The current executor receives subsequent readiness notifications.
            previous.executionQueued -= connection.queued;
            executor.executionQueued += connection.queued;
            connection.executor = executor;
            updateTransferable(previous);
        }
        executor.executionQueued--;
        connection.removeHead();
        connection.running = true;
        executor.running = entry;
        home.queued--;
        connection.queued--;
        if (connection.waitingForPendingSlot) {
            connection.waitingForPendingSlot = false;
            if (!home.admissionClosed()) {
                // The frame just became eligible. Preserve the order of existing waiters.
                home.group.budget.enqueue(connection);
            }
        }
        updateTransferable(executor);
        signalHelper(executor.group);
        return entry.request;
    }

    private static void updateTransferable(Owner owner) {
        // An idle owner retains its first ready head.
        int spare = owner.heads.size() - (owner.running == null ? 1 : 0);
        int transferable = Math.max(0, spare);
        owner.group.transferable += transferable - owner.transferable;
        owner.transferable = transferable;
    }

    private static void signalHelper(WorkerGroup group) {
        if (group.closing || group.parked == 0 || group.transferable <= group.notifiedHelpers) {
            return;
        }
        for (Owner candidate : group.owners) {
            if (candidate.waiting && !candidate.closing) {
                candidate.helperNotified = true;
                group.notifiedHelpers++;
                candidate.signal();
                return;
            }
        }
    }

    void finish(Owner owner) {
        ReadWaiter reader = null;
        lock.lock();
        try {
            Entry entry = owner.running;
            owner.running = null;
            updateTransferable(owner);
            releaseBuckets(entry);
            Connection connection = entry.connection;
            connection.running = false;
            // Reading can overlap execution, but the next head takes its ticket at completion.
            if (connection.head != null) {
                register(owner, connection.head);
            }
            owner.group.budget.release(entry.bytes, entry.bypassed);
            reader = grantNextRead(owner.group);
        } finally {
            lock.unlock();
            notifyReader(reader);
        }
    }

    void close(Owner owner) {
        lock.lock();
        try {
            owner.closing = true;
            owner.group.closing = true;
            owner.group.budget.removeOwner(owner);
            owner.signal();
        } finally {
            lock.unlock();
        }
    }

    private void register(Owner owner, Entry entry) {
        entry.ticket = nextTicket++;
        for (PageMask page = entry.access.pages; page != null; page = page.next) {
            bitmapPages
                    .computeIfAbsent(page.key, ignored -> new PageState())
                    .register(page.mask, entry);
        }
        if (entry.blockedBuckets == 0) {
            ready(owner, entry);
        }
    }

    private void releaseBuckets(Entry entry) {
        for (PageMask page = entry.access.pages; page != null; page = page.next) {
            PageState state = bitmapPages.get(page.key);
            state.release(page.mask, entry, bucketReady);
            if (state.empty()) {
                bitmapPages.remove(page.key);
            }
        }
    }

    private void bucketReady(Entry entry) {
        if (--entry.blockedBuckets == 0) {
            ready(entry.connection.executor, entry);
        }
    }

    private void ready(Owner owner, Entry entry) {
        owner.heads.put(entry.ticket, entry);
        owner.signal();
        updateTransferable(owner);
        signalHelper(owner.group);
    }

    Owner createOwner(WorkerGroup group) {
        Owner owner = new Owner(lock.newCondition(), group);
        group.owners.add(owner);
        return owner;
    }

    static final class WorkerGroup {
        private final PrefetchBudget budget;
        private final List<Owner> owners = new ArrayList<>();
        private int parked;
        private int transferable;
        private int notifiedHelpers;
        private volatile boolean closing;

        WorkerGroup(long budgetBytes) {
            this(new PrefetchBudget(budgetBytes, SMALL_FRAME_BYTES, MAX_BYPASS_BYTES));
        }

        WorkerGroup(PrefetchBudget budget) {
            this.budget = budget;
        }
    }

    // Shared by the owners in one listener resource group under the scheduler lock.
    // Charges full wire-frame lengths from permit through synchronous request completion.
    static final class PrefetchBudget {
        private final long limit;
        private final Map<Connection, ReadWaiter> waiters = new LinkedHashMap<>();
        private final Map<Connection, ReadWaiter> smallWaiters = new LinkedHashMap<>();
        private final int smallFrameBytes;
        private final long maxBypassBytes;
        private long used;
        private long bypassedBytes;

        PrefetchBudget(long limit, int smallFrameBytes, long maxBypassBytes) {
            this.limit = limit;
            this.smallFrameBytes = smallFrameBytes;
            this.maxBypassBytes = maxBypassBytes;
        }

        private boolean isSmall(Connection connection) {
            return connection.frameBytes <= smallFrameBytes;
        }

        private void enqueue(Connection connection) {
            waiters.putIfAbsent(connection, connection.reader);
            if (isSmall(connection)) {
                smallWaiters.putIfAbsent(connection, connection.reader);
            }
        }

        private void remove(Connection connection) {
            waiters.remove(connection);
            smallWaiters.remove(connection);
        }

        private void removeOwner(Owner owner) {
            waiters.values().removeIf(waiter -> waiter.owner == owner);
            smallWaiters.values().removeIf(waiter -> waiter.owner == owner);
        }

        private void release(int bytes, boolean bypassed) {
            used -= bytes;
            if (bypassed) {
                bypassedBytes -= bytes;
            }
        }
    }

    static final class Owner {
        private final Condition available;
        private final NavigableMap<Long, Entry> heads = new TreeMap<>();
        private final WorkerGroup group;
        private boolean helperNotified;
        private int transferable;
        private int executionQueued;
        private Entry running;
        private volatile boolean closing;
        private boolean waiting;
        volatile int queued;

        private Owner(Condition available, WorkerGroup group) {
            this.available = available;
            this.group = group;
        }

        private boolean admissionClosed() {
            return group.closing;
        }

        private void acknowledgeHelper() {
            if (helperNotified) {
                helperNotified = false;
                group.notifiedHelpers--;
            }
        }

        private void beginWait() {
            waiting = true;
            group.parked++;
        }

        private void endWait() {
            if (waiting) {
                waiting = false;
                group.parked--;
            }
        }

        private void signal() {
            if (waiting) {
                // The awakened worker must reacquire the scheduler lock before it can wait again.
                endWait();
                available.signal();
            }
        }
    }

    private static final class ReadWaiter {
        private final Owner owner;
        private final ChannelHandlerContext ctx;
        private final Runnable resume;

        // Links the one-shot notification batch until this reader is scheduled.
        private ReadWaiter next;

        private ReadWaiter(Owner owner, ChannelHandlerContext ctx, Runnable resume) {
            this.owner = owner;
            this.ctx = ctx;
            this.resume = resume;
        }
    }

    private static final class Connection {
        private final Owner home;
        private Owner executor;
        private Entry head;
        private Entry second;
        private ReadWaiter reader;
        private boolean running;
        private boolean waitingForPendingSlot;
        // Admission and normal grant cleanup run synchronously on this connection's EventLoop.
        private volatile boolean readGranted;
        private boolean bypassed;
        private int frameBytes;
        private int queued;

        private Connection(Owner home) {
            this.home = home;
            this.executor = home;
        }

        private void add(Entry entry) {
            if (head == null) {
                head = entry;
            } else {
                second = entry;
            }
        }

        private void removeHead() {
            head = second;
            second = null;
        }
    }

    private static final class Entry {
        private final Connection connection;
        private final FlussRequest request;
        private final Access access;
        private long ticket;
        // The head becomes ready when the last conflicting bucket clears.
        private int blockedBuckets;
        private int bytes;
        private boolean bypassed;

        private Entry(Connection connection, FlussRequest request, Access access) {
            this.connection = connection;
            this.request = request;
            this.access = access;
        }
    }

    // A bucket is locally granted even while its request waits for another bucket.
    // With eager promotion, a nonempty wait queue implies a holder. If readers hold
    // the bucket then its first waiter is a writer.
    private static final class PageState {
        private long reads;
        private long writes;
        private long waiting;
        private long shared;
        private int[] readerCounts;
        private Map<Integer, Deque<Entry>> queues;

        void register(long mask, Entry entry) {
            boolean write = entry.access.write;
            long blocked = mask & (write ? (reads | writes) : (writes | waiting));
            long granted = mask & ~blocked;
            if (write) {
                writes |= granted;
            } else {
                for (long overlap = reads & granted; overlap != 0; overlap &= overlap - 1) {
                    int bucket = Long.numberOfTrailingZeros(overlap);
                    int current = readerCounts == null ? 0 : readerCounts[bucket];
                    countReaders(bucket, current == 0 ? 2 : current + 1);
                }
                reads |= granted;
            }
            if (blocked != 0) {
                if (queues == null) {
                    queues = new HashMap<>();
                }
                for (long remaining = blocked; remaining != 0; remaining &= remaining - 1) {
                    int bucket = Long.numberOfTrailingZeros(remaining);
                    queues.computeIfAbsent(bucket, ignored -> new ArrayDeque<>()).addLast(entry);
                }
                waiting |= blocked;
                entry.blockedBuckets += Long.bitCount(blocked);
            }
        }

        void release(long mask, Entry entry, Consumer<Entry> ready) {
            long vacant = mask;
            if (entry.access.write) {
                writes &= ~mask;
            } else {
                long overlap = shared & mask;
                vacant &= ~overlap;
                reads &= ~vacant;
                for (; overlap != 0; overlap &= overlap - 1) {
                    int bucket = Long.numberOfTrailingZeros(overlap);
                    int count = readerCounts[bucket];
                    if (count == 2) {
                        readerCounts[bucket] = 0;
                        shared &= ~(1L << bucket);
                    } else {
                        readerCounts[bucket] = count - 1;
                    }
                }
                if (shared == 0) {
                    readerCounts = null;
                }
            }
            for (long remaining = vacant & waiting; remaining != 0; remaining &= remaining - 1) {
                int bucket = Long.numberOfTrailingZeros(remaining);
                long bit = 1L << bucket;
                Deque<Entry> queue = queues.get(bucket);
                int granted = promote(queue, ready);
                if (granted == 0) {
                    writes |= bit;
                } else {
                    reads |= bit;
                    if (granted > 1) {
                        countReaders(bucket, granted);
                    }
                }
                if (queue.isEmpty()) {
                    queues.remove(bucket);
                    waiting &= ~bit;
                }
            }
            if (waiting == 0) {
                queues = null;
            }
        }

        private void countReaders(int bucket, int count) {
            if (readerCounts == null) {
                readerCounts = new int[Long.SIZE];
            }
            readerCounts[bucket] = count;
            shared |= 1L << bucket;
        }

        boolean empty() {
            return (reads | writes) == 0;
        }
    }

    // Returns zero for a writer, or the number of readers in the next read phase.
    private static int promote(Deque<Entry> waiters, Consumer<Entry> ready) {
        Entry first = waiters.removeFirst();
        ready.accept(first);
        if (first.access.write) {
            return 0;
        }
        int readers = 1;
        while (!waiters.isEmpty() && !waiters.getFirst().access.write) {
            ready.accept(waiters.removeFirst());
            readers++;
        }
        return readers;
    }

    private static final class BucketPage {
        private final long table;
        private final boolean partitioned;
        private final long partition;
        private final int page;

        BucketPage(long table, boolean partitioned, long partition, int page) {
            this.table = table;
            this.partitioned = partitioned;
            this.partition = partition;
            this.page = page;
        }

        boolean matches(boolean hasPartition, long partitionId, int pageId) {
            return page == pageId && partitioned == hasPartition && partition == partitionId;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof BucketPage)) {
                return false;
            }
            BucketPage that = (BucketPage) other;
            return table == that.table && matches(that.partitioned, that.partition, that.page);
        }

        @Override
        public int hashCode() {
            int result = Long.hashCode(table);
            result = 31 * result + Boolean.hashCode(partitioned);
            result = 31 * result + Long.hashCode(partition);
            return 31 * result + page;
        }
    }

    private static final class PageMask {
        private final BucketPage key;
        private long mask;
        private PageMask next;

        PageMask(BucketPage key) {
            this.key = key;
        }
    }

    private static final class BitmapAccessBuilder {
        private final long table;
        private PageMask first;
        private PageMask last;
        private PageMask current;
        private Map<BucketPage, PageMask> pages;

        BitmapAccessBuilder(long table) {
            this.table = table;
        }

        void add(boolean partitioned, long partition, int bucket) {
            int page = bucket >> 6;
            if (current == null || !current.key.matches(partitioned, partition, page)) {
                BucketPage key = new BucketPage(table, partitioned, partition, page);
                if (first == null) {
                    first = new PageMask(key);
                    last = first;
                    current = first;
                } else {
                    if (pages == null) {
                        pages = new HashMap<>();
                        pages.put(first.key, first);
                    }
                    current = pages.get(key);
                    if (current == null) {
                        current = new PageMask(key);
                        pages.put(key, current);
                        last.next = current;
                        last = current;
                    }
                }
            }
            current.mask |= 1L << (bucket & 63);
        }

        Access build(boolean write) {
            return new Access(write, first);
        }
    }

    static final class Access {
        private static final Access OPAQUE = new Access(false, null);
        private final boolean write;
        private final PageMask pages;

        private Access(boolean write, PageMask pages) {
            this.write = write;
            this.pages = pages;
        }

        static Access from(FlussRequest request) {
            if (request.getMessage() instanceof PutKvRequest) {
                PutKvRequest message = (PutKvRequest) request.getMessage();
                int count = message.getBucketsReqsCount();
                if (count > 0 && !hasHistoricalPut(message)) {
                    BitmapAccessBuilder builder = new BitmapAccessBuilder(message.getTableId());
                    for (int i = 0; i < count; i++) {
                        PbPutKvReqForBucket bucket = message.getBucketsReqAt(i);
                        boolean partitioned = bucket.hasPartitionId();
                        builder.add(
                                partitioned,
                                partitioned ? bucket.getPartitionId() : 0,
                                bucket.getBucketId());
                    }
                    return builder.build(true);
                }
            } else if (request.getMessage() instanceof LookupRequest) {
                LookupRequest message = (LookupRequest) request.getMessage();
                int count = message.getBucketsReqsCount();
                if (count > 0 && !hasHistoricalLookup(message)) {
                    BitmapAccessBuilder builder = new BitmapAccessBuilder(message.getTableId());
                    for (int i = 0; i < count; i++) {
                        PbLookupReqForBucket bucket = message.getBucketsReqAt(i);
                        boolean partitioned = bucket.hasPartitionId();
                        builder.add(
                                partitioned,
                                partitioned ? bucket.getPartitionId() : 0,
                                bucket.getBucketId());
                    }
                    return builder.build(
                            message.hasInsertIfNotExists() && message.isInsertIfNotExists());
                }
            } else if (request.getMessage() instanceof PrefixLookupRequest) {
                PrefixLookupRequest message = (PrefixLookupRequest) request.getMessage();
                int count = message.getBucketsReqsCount();
                if (count > 0) {
                    BitmapAccessBuilder builder = new BitmapAccessBuilder(message.getTableId());
                    for (int i = 0; i < count; i++) {
                        PbPrefixLookupReqForBucket bucket = message.getBucketsReqAt(i);
                        boolean partitioned = bucket.hasPartitionId();
                        builder.add(
                                partitioned,
                                partitioned ? bucket.getPartitionId() : 0,
                                bucket.getBucketId());
                    }
                    return builder.build(false);
                }
            }
            return OPAQUE;
        }
    }
}
