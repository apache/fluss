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

import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.messages.LookupRequest;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.rpc.netty.server.ConnectionHeadScheduler.Owner;
import org.apache.fluss.rpc.netty.server.ConnectionHeadScheduler.PrefetchBudget;
import org.apache.fluss.rpc.netty.server.ConnectionHeadScheduler.WorkerGroup;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.rpc.protocol.ApiManager;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.fluss.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetAddress;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

class ConnectionHeadSchedulerTest {
    private static final ApiManager APIS = new ApiManager(ServerType.TABLET_SERVER);

    @Test
    void testMultiBucketWriterAndConnectionFifo() {
        ConnectionHeadScheduler scheduler = new ConnectionHeadScheduler();
        Owner first = scheduler.createOwner(new WorkerGroup(1024));
        Owner writer = scheduler.createOwner(new WorkerGroup(1024));
        Owner later = scheduler.createOwner(new WorkerGroup(1024));
        EmbeddedChannel firstConnection = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
        EmbeddedChannel writeConnection = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
        EmbeddedChannel readConnection = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
        EmbeddedChannel unrelatedConnection =
                new EmbeddedChannel(new ChannelInboundHandlerAdapter());
        try {
            FlussRequest holding = read(1);
            FlussRequest write = write(1, 2);
            FlussRequest follower = read(4);
            FlussRequest laterReader = read(2);
            FlussRequest unrelated = read(3);
            offer(scheduler, first, firstConnection, holding);
            assertThat(scheduler.poll(first)).isSameAs(holding);
            offer(scheduler, writer, writeConnection, write);
            offer(scheduler, writer, writeConnection, follower);
            offer(scheduler, writer, unrelatedConnection, unrelated);
            offer(scheduler, later, readConnection, laterReader);

            assertThat(scheduler.poll(writer)).isSameAs(unrelated);
            scheduler.finish(writer);
            assertThat(scheduler.poll(writer)).isNull();
            assertThat(scheduler.poll(later)).isNull();

            scheduler.close(writer);
            assertThat(scheduler.offer(writer, writeConnection, write)).isFalse();
            scheduler.finish(first);
            assertThat(scheduler.poll(writer)).isSameAs(write);
            assertThat(scheduler.poll(writer)).isNull();
            assertThat(scheduler.poll(later)).isNull();
            scheduler.finish(writer);
            assertThat(scheduler.poll(writer)).isSameAs(follower);
            scheduler.finish(writer);
            assertThat(scheduler.poll(writer)).isSameAs(ShutdownRequest.INSTANCE);
            assertThat(scheduler.poll(later)).isSameAs(laterReader);
            scheduler.finish(later);
        } finally {
            firstConnection.finishAndReleaseAll();
            writeConnection.finishAndReleaseAll();
            readConnection.finishAndReleaseAll();
            unrelatedConnection.finishAndReleaseAll();
        }
    }

    @Test
    void testReadyConnectionsKeepRegistrationOrder() {
        ConnectionHeadScheduler scheduler = new ConnectionHeadScheduler();
        Owner owner = scheduler.createOwner(new WorkerGroup(1024));
        EmbeddedChannel firstConnection = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
        EmbeddedChannel secondConnection = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
        try {
            FlussRequest first = read(1);
            FlussRequest firstFollower = read(2);
            FlussRequest second = read(3);
            FlussRequest duringRun = read(4);
            offer(scheduler, owner, firstConnection, first);
            offer(scheduler, owner, firstConnection, firstFollower);
            offer(scheduler, owner, secondConnection, second);
            assertThat(scheduler.poll(owner)).isSameAs(first);
            offer(scheduler, owner, firstConnection, duringRun);
            scheduler.finish(owner);
            assertThat(scheduler.poll(owner)).isSameAs(second);
            scheduler.finish(owner);
            assertThat(scheduler.poll(owner)).isSameAs(firstFollower);
            scheduler.finish(owner);
            assertThat(scheduler.poll(owner)).isSameAs(duringRun);
            scheduler.finish(owner);
            assertThat(scheduler.poll(owner)).isNull();
            offer(scheduler, owner, firstConnection, first);
            assertThat(scheduler.poll(owner)).isSameAs(first);
            scheduler.finish(owner);
        } finally {
            firstConnection.finishAndReleaseAll();
            secondConnection.finishAndReleaseAll();
        }
    }

    @Test
    void testLastReaderWakesBlockedWriter() throws Exception {
        ConnectionHeadScheduler scheduler = new ConnectionHeadScheduler();
        Owner first = scheduler.createOwner(new WorkerGroup(1024));
        Owner second = scheduler.createOwner(new WorkerGroup(1024));
        Owner writer = scheduler.createOwner(new WorkerGroup(1024));
        EmbeddedChannel firstConnection = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
        EmbeddedChannel secondConnection = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
        EmbeddedChannel writeConnection = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
        FutureTask<RpcRequest> claim = new FutureTask<>(() -> scheduler.take(writer));
        Thread worker = new Thread(claim);
        try {
            FlussRequest firstRead = read(1);
            FlussRequest secondRead = read(1);
            FlussRequest write = write(1, 2, 1);
            offer(scheduler, first, firstConnection, firstRead);
            assertThat(scheduler.poll(first)).isSameAs(firstRead);
            offer(scheduler, second, secondConnection, secondRead);
            assertThat(scheduler.poll(second)).isSameAs(secondRead);
            offer(scheduler, writer, writeConnection, write);
            worker.start();
            waitUntil(
                    () -> worker.getState() == Thread.State.WAITING,
                    Duration.ofSeconds(5),
                    "Writer did not park while readers held the bucket");
            scheduler.finish(second);
            assertThat(claim).isNotDone();
            scheduler.finish(first);
            assertThat(claim.get(5, TimeUnit.SECONDS)).isSameAs(write);
            scheduler.finish(writer);
        } finally {
            worker.interrupt();
            worker.join(TimeUnit.SECONDS.toMillis(5));
            firstConnection.finishAndReleaseAll();
            secondConnection.finishAndReleaseAll();
            writeConnection.finishAndReleaseAll();
        }
    }

    @Test
    void testSharedPrefetchBudgetRetainedUntilRequestFinishes() {
        ConnectionHeadScheduler scheduler = new ConnectionHeadScheduler();
        WorkerGroup group = new WorkerGroup(100);
        Owner running = scheduler.createOwner(group);
        Owner waiting = scheduler.createOwner(group);
        ChannelInboundHandlerAdapter runningHandler = new ChannelInboundHandlerAdapter();
        ChannelInboundHandlerAdapter waitingHandler = new ChannelInboundHandlerAdapter();
        EmbeddedChannel first = new EmbeddedChannel(runningHandler);
        EmbeddedChannel second = new EmbeddedChannel(waitingHandler);
        CompletableFuture<Void> resumed = new CompletableFuture<>();
        try {
            assertThat(
                            scheduler.acquireRead(
                                    running,
                                    first.pipeline().context(runningHandler),
                                    () -> {},
                                    100))
                    .isTrue();
            FlussRequest request = read(1);
            offer(scheduler, running, first, request);
            assertThat(scheduler.poll(running)).isSameAs(request);
            assertThat(
                            scheduler.acquireRead(
                                    waiting,
                                    second.pipeline().context(waitingHandler),
                                    () -> resumed.complete(null),
                                    100))
                    .isFalse();
            second.runPendingTasks();
            assertThat(resumed).isNotDone();

            scheduler.finish(running);
            second.runPendingTasks();
            assertThat(resumed).isCompleted();
            assertThat(scheduler.hasReadGrant(waiting, second)).isTrue();
        } finally {
            scheduler.releaseRead(running, first, true);
            scheduler.releaseRead(waiting, second, true);
            first.finishAndReleaseAll();
            second.finishAndReleaseAll();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testSmallFrameBypassPreservesLargeHeadProgress(boolean cancelGrant) {
        ConnectionHeadScheduler scheduler = new ConnectionHeadScheduler();
        WorkerGroup group = new WorkerGroup(new PrefetchBudget(100, 8, 20));
        try (BudgetConnection holding = new BudgetConnection(scheduler, group);
                BudgetConnection large = new BudgetConnection(scheduler, group);
                BudgetConnection small = new BudgetConnection(scheduler, group);
                BudgetConnection abandoned = new BudgetConnection(scheduler, group);
                BudgetConnection next = new BudgetConnection(scheduler, group)) {
            assertThat(holding.requestRead(80)).isTrue();
            assertThat(large.requestRead(90)).isFalse();
            assertThat(small.requestRead(8)).isFalse();
            assertThat(small.resumed()).isTrue();
            assertThat(large.resumed()).isFalse();

            assertThat(abandoned.requestRead(8)).isFalse();
            assertThat(next.requestRead(8)).isFalse();
            assertThat(next.resumed()).isFalse();
            abandoned.close();
            if (cancelGrant) {
                small.close();
            } else {
                FlussRequest request = read(1);
                offer(scheduler, small.owner, small.channel, request);
                assertThat(scheduler.poll(small.owner)).isSameAs(request);
                assertThat(next.resumed()).isFalse();
                scheduler.finish(small.owner);
            }
            assertThat(next.resumed()).isTrue();
            assertThat(large.resumed()).isFalse();
            // The head fits with the second bypass grant still outstanding.
            holding.close();
            assertThat(large.resumed()).isTrue();
        }
    }

    @Test
    void testFullBudgetHeadBlocksBypass() {
        ConnectionHeadScheduler scheduler = new ConnectionHeadScheduler();
        WorkerGroup group = new WorkerGroup(new PrefetchBudget(100, 8, 20));
        try (BudgetConnection holding = new BudgetConnection(scheduler, group);
                BudgetConnection large = new BudgetConnection(scheduler, group);
                BudgetConnection remaining = new BudgetConnection(scheduler, group)) {
            assertThat(holding.requestRead(80)).isTrue();
            assertThat(large.requestRead(100)).isFalse();
            assertThat(remaining.requestRead(8)).isFalse();
            assertThat(remaining.resumed()).isFalse();
            holding.close();
            assertThat(large.resumed()).isTrue();
            assertThat(remaining.resumed()).isFalse();
            large.close();
            assertThat(remaining.resumed()).isTrue();
        }
    }

    @Test
    void testClosingGroupRejectsScheduledReadGrant() {
        ConnectionHeadScheduler scheduler = new ConnectionHeadScheduler();
        WorkerGroup group = new WorkerGroup(100);
        Owner owner = scheduler.createOwner(group);
        try (BudgetConnection holder = new BudgetConnection(scheduler, group)) {
            assertThat(holder.requestRead(100)).isTrue();
            EmbeddedChannel channel = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
            try {
                CompletableFuture<Boolean> accepted = new CompletableFuture<>();
                ChannelHandlerContext context = channel.pipeline().firstContext();
                assertThat(
                                scheduler.acquireRead(
                                        owner,
                                        context,
                                        () ->
                                                accepted.complete(
                                                        scheduler.acquireRead(
                                                                owner, context, () -> {}, 80)),
                                        80))
                        .isFalse();
                holder.close();
                assertThat(scheduler.hasReadGrant(owner, channel)).isTrue();
                scheduler.close(owner);
                channel.runPendingTasks();
                assertThat(accepted).isCompletedWithValue(false);
                assertThat(scheduler.poll(owner)).isSameAs(ShutdownRequest.INSTANCE);
            } finally {
                scheduler.releaseRead(owner, channel, true);
                channel.finishAndReleaseAll();
            }
        }
    }

    @Test
    void testIndependentHeadCanPassBlockedHeadWithPrefetchBudget() {
        ConnectionHeadScheduler scheduler = new ConnectionHeadScheduler();
        WorkerGroup group = new WorkerGroup(300);
        Owner holder = scheduler.createOwner(new WorkerGroup(1024));
        Owner owner = scheduler.createOwner(group);
        ChannelInboundHandlerAdapter firstHandler = new ChannelInboundHandlerAdapter();
        ChannelInboundHandlerAdapter secondHandler = new ChannelInboundHandlerAdapter();
        EmbeddedChannel writer = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
        EmbeddedChannel first = new EmbeddedChannel(firstHandler);
        EmbeddedChannel second = new EmbeddedChannel(secondHandler);
        try {
            FlussRequest blocking = write(1);
            offer(scheduler, holder, writer, blocking);
            assertThat(scheduler.poll(holder)).isSameAs(blocking);
            assertThat(
                            scheduler.acquireRead(
                                    owner, first.pipeline().context(firstHandler), () -> {}, 100))
                    .isTrue();
            FlussRequest sameBucket = read(1);
            offer(scheduler, owner, first, sameBucket);
            assertThat(scheduler.poll(owner)).isNull();

            assertThat(
                            scheduler.acquireRead(
                                    owner, second.pipeline().context(secondHandler), () -> {}, 100))
                    .isTrue();
            FlussRequest independent = read(2);
            offer(scheduler, owner, second, independent);
            assertThat(scheduler.poll(owner)).isSameAs(independent);
            scheduler.finish(owner);
            scheduler.finish(holder);
            assertThat(scheduler.poll(owner)).isSameAs(sameBucket);
            scheduler.finish(owner);
        } finally {
            scheduler.releaseRead(owner, first, true);
            scheduler.releaseRead(owner, second, true);
            writer.finishAndReleaseAll();
            first.finishAndReleaseAll();
            second.finishAndReleaseAll();
        }
    }

    @Test
    void testIdleWorkerTakesEntireConnectionAndKeepsAdmissionAtHome() throws Exception {
        ConnectionHeadScheduler scheduler = new ConnectionHeadScheduler();
        WorkerGroup group = new WorkerGroup(100);
        Owner home = scheduler.createOwner(group);
        Owner helper = scheduler.createOwner(group);
        ChannelInboundHandlerAdapter holdingHandler = new ChannelInboundHandlerAdapter();
        ChannelInboundHandlerAdapter transferredHandler = new ChannelInboundHandlerAdapter();
        EmbeddedChannel holding = new EmbeddedChannel(holdingHandler);
        EmbeddedChannel transferred = new EmbeddedChannel(transferredHandler);
        FutureTask<RpcRequest> claim = new FutureTask<>(() -> scheduler.take(helper));
        Thread worker = new Thread(claim);
        CompletableFuture<Void> resumed = new CompletableFuture<>();
        try {
            FlussRequest slow = write(1);
            offer(scheduler, home, holding, slow);
            assertThat(scheduler.poll(home)).isSameAs(slow);
            worker.start();
            waitUntil(
                    () -> worker.getState() == Thread.State.WAITING,
                    Duration.ofSeconds(5),
                    "Worker did not wait");

            FlussRequest independent = read(2);
            FlussRequest follower = read(3);
            assertThat(
                            scheduler.acquireRead(
                                    home,
                                    transferred.pipeline().firstContext(),
                                    () -> resumed.complete(null),
                                    40))
                    .isTrue();
            offer(scheduler, home, transferred, independent);
            assertThat(claim.get(5, TimeUnit.SECONDS)).isSameAs(independent);
            assertThat(home.queued).isZero();
            assertThat(helper.queued).isZero();
            assertThat(
                            scheduler.acquireRead(
                                    home,
                                    transferred.pipeline().firstContext(),
                                    () -> resumed.complete(null),
                                    40))
                    .isFalse();
            assertThat(scheduler.hasReadGrant(home, transferred)).isFalse();
            scheduler.finish(helper);
            transferred.runPendingTasks();
            assertThat(resumed).isCompleted();
            assertThat(scheduler.hasReadGrant(home, transferred)).isTrue();

            offer(scheduler, home, transferred, follower);
            // Finish the old execution owner; the next head still belongs to the new owner.
            scheduler.finish(home);
            assertThat(scheduler.poll(home)).isNull();
            assertThat(scheduler.poll(helper)).isSameAs(follower);
            scheduler.finish(helper);
            assertThat(home.queued).isZero();
            assertThat(
                            scheduler.acquireRead(
                                    home, transferred.pipeline().firstContext(), () -> {}, 40))
                    .isTrue();
            scheduler.releaseRead(home, transferred, true);
        } finally {
            worker.interrupt();
            worker.join(TimeUnit.SECONDS.toMillis(5));
            holding.finishAndReleaseAll();
            transferred.finishAndReleaseAll();
        }
    }

    @Test
    void testBucketReleaseWakesHelperAndPreservesWriterOrder() throws Exception {
        ConnectionHeadScheduler scheduler = new ConnectionHeadScheduler();
        WorkerGroup group = new WorkerGroup(1024);
        WorkerGroup otherGroup = new WorkerGroup(1024);
        Owner home = scheduler.createOwner(group);
        Owner helper = scheduler.createOwner(group);
        Owner remote = scheduler.createOwner(otherGroup);
        EmbeddedChannel first = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
        EmbeddedChannel second = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
        EmbeddedChannel third = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
        EmbeddedChannel late = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
        FutureTask<RpcRequest> claim = new FutureTask<>(() -> scheduler.take(helper));
        Thread worker = new Thread(claim);
        try {
            FlussRequest blockingRead = read(1);
            FlussRequest slow = write(4);
            FlussRequest writer = write(1, 2);
            FlussRequest younger = read(2);
            offer(scheduler, remote, first, blockingRead);
            assertThat(scheduler.poll(remote)).isSameAs(blockingRead);
            offer(scheduler, home, second, slow);
            assertThat(scheduler.poll(home)).isSameAs(slow);
            offer(scheduler, home, third, writer);

            worker.start();
            waitUntil(
                    () -> worker.getState() == Thread.State.WAITING,
                    Duration.ofSeconds(5),
                    "Conflicted writer must wait");
            assertThat(claim).isNotDone();
            scheduler.finish(remote);
            assertThat(claim.get(5, TimeUnit.SECONDS)).isSameAs(writer);
            offer(scheduler, remote, late, younger);
            assertThat(scheduler.poll(remote)).isNull();
            scheduler.finish(helper);
            assertThat(scheduler.poll(remote)).isSameAs(younger);
            scheduler.finish(remote);
            scheduler.finish(home);
        } finally {
            worker.interrupt();
            worker.join(TimeUnit.SECONDS.toMillis(5));
            first.finishAndReleaseAll();
            second.finishAndReleaseAll();
            third.finishAndReleaseAll();
            late.finishAndReleaseAll();
        }
    }

    @Test
    void testWorkerKeepsOneLocalHeadAndCloseDrainsMigratedFollowers() {
        ConnectionHeadScheduler scheduler = new ConnectionHeadScheduler();
        WorkerGroup group = new WorkerGroup(160);
        Owner home = scheduler.createOwner(group);
        Owner helper = scheduler.createOwner(group);
        Owner isolated = scheduler.createOwner(new WorkerGroup(1024));
        EmbeddedChannel first = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
        EmbeddedChannel second = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
        try {
            FlussRequest oldest = read(1);
            FlussRequest migrated = read(2);
            FlussRequest follower = read(3);
            offer(scheduler, home, first, oldest);
            assertThat(scheduler.poll(helper)).isNull();
            offer(scheduler, home, second, migrated);
            assertThat(scheduler.poll(isolated)).isNull();
            assertThat(scheduler.poll(helper)).isSameAs(migrated);
            offer(scheduler, home, second, follower);
            assertThat(scheduler.poll(home)).isSameAs(oldest);

            scheduler.close(home);
            assertThat(scheduler.acquireRead(helper, first.pipeline().firstContext(), () -> {}, 40))
                    .isFalse();
            scheduler.close(helper);
            scheduler.finish(home);
            assertThat(scheduler.poll(home)).isSameAs(ShutdownRequest.INSTANCE);
            scheduler.finish(helper);
            assertThat(scheduler.poll(helper)).isSameAs(follower);
            scheduler.finish(helper);
            assertThat(scheduler.poll(helper)).isSameAs(ShutdownRequest.INSTANCE);
            assertThat(home.queued).isZero();
        } finally {
            first.finishAndReleaseAll();
            second.finishAndReleaseAll();
        }
    }

    @Test
    void testReadyHeadsWaitForEveryBucketAndKeepLocalWorkFirst() {
        ConnectionHeadScheduler scheduler = new ConnectionHeadScheduler();
        WorkerGroup group = new WorkerGroup(1024);
        Owner home = scheduler.createOwner(group);
        Owner helper = scheduler.createOwner(group);
        Owner firstBlocker = scheduler.createOwner(new WorkerGroup(1024));
        Owner secondBlocker = scheduler.createOwner(new WorkerGroup(1024));
        EmbeddedChannel[] connections = new EmbeddedChannel[8];
        for (int i = 0; i < connections.length; i++) {
            connections[i] = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
        }
        try {
            FlussRequest firstWrite = write(63);
            FlussRequest secondWrite = write(64);
            FlussRequest busy = read(3);
            FlussRequest bothBuckets = read(63, 64, 64);
            FlussRequest firstBucket = read(63);
            FlussRequest writer = write(63);
            FlussRequest lateReader = read(63);
            FlussRequest ownRead = read(4);
            offer(scheduler, firstBlocker, connections[0], firstWrite);
            offer(scheduler, secondBlocker, connections[1], secondWrite);
            offer(scheduler, home, connections[2], busy);
            assertThat(scheduler.poll(firstBlocker)).isSameAs(firstWrite);
            assertThat(scheduler.poll(secondBlocker)).isSameAs(secondWrite);
            assertThat(scheduler.poll(home)).isSameAs(busy);
            offer(scheduler, home, connections[3], bothBuckets);
            offer(scheduler, home, connections[4], firstBucket);
            offer(scheduler, home, connections[5], writer);
            offer(scheduler, home, connections[6], lateReader);
            assertThat(scheduler.poll(helper)).isNull();

            scheduler.finish(firstBlocker);
            offer(scheduler, helper, connections[7], ownRead);
            assertThat(scheduler.poll(helper)).isSameAs(ownRead);
            scheduler.finish(helper);
            assertThat(scheduler.poll(helper)).isSameAs(firstBucket);
            scheduler.finish(helper);
            assertThat(scheduler.poll(helper)).isNull();

            scheduler.finish(secondBlocker);
            assertThat(scheduler.poll(helper)).isSameAs(bothBuckets);
            scheduler.finish(helper);
            assertThat(scheduler.poll(helper)).isSameAs(writer);
            scheduler.finish(helper);
            assertThat(scheduler.poll(helper)).isSameAs(lateReader);
            scheduler.finish(helper);
            scheduler.finish(home);
        } finally {
            for (EmbeddedChannel connection : connections) {
                connection.finishAndReleaseAll();
            }
        }
    }

    @Test
    void testWorkersCannotClaimHeadsFromAnotherListenerGroup() {
        ConnectionHeadScheduler scheduler = new ConnectionHeadScheduler();
        Owner home = scheduler.createOwner(new WorkerGroup(1024));
        Owner peer = scheduler.createOwner(new WorkerGroup(1024));
        EmbeddedChannel holding = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
        EmbeddedChannel waiting = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
        EmbeddedChannel independent = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
        try {
            FlussRequest reader = read(1, 2);
            FlussRequest writer = write(1, 2);
            FlussRequest local = read(3);
            offer(scheduler, peer, holding, reader);
            assertThat(scheduler.poll(peer)).isSameAs(reader);
            offer(scheduler, home, waiting, writer);
            offer(scheduler, home, independent, local);
            scheduler.finish(peer);
            assertThat(scheduler.poll(peer)).isNull();
            assertThat(scheduler.poll(home)).isSameAs(writer);
            scheduler.finish(home);
            assertThat(scheduler.poll(home)).isSameAs(local);
            scheduler.finish(home);
        } finally {
            holding.finishAndReleaseAll();
            waiting.finishAndReleaseAll();
            independent.finishAndReleaseAll();
        }
    }

    @Test
    void testConflictIndexSeparatesTablesPartitionsAndBucketSegments() {
        ConnectionHeadScheduler scheduler = new ConnectionHeadScheduler();
        Owner holder = scheduler.createOwner(new WorkerGroup(1024));
        Owner reader = scheduler.createOwner(new WorkerGroup(1024));
        EmbeddedChannel[] connections = new EmbeddedChannel[5];
        for (int i = 0; i < connections.length; i++) {
            connections[i] = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
        }
        try {
            PutKvRequest put = new PutKvRequest().setTableId(1);
            put.addBucketsReq().setBucketId(63).setPartitionId(0);
            FlussRequest write = request(ApiKeys.PUT_KV, put);
            offer(scheduler, holder, connections[0], write);
            assertThat(scheduler.poll(holder)).isSameAs(write);

            LookupRequest lookup = new LookupRequest().setTableId(1);
            lookup.addBucketsReq().setBucketId(63).setPartitionId(0);
            FlussRequest conflicting = request(ApiKeys.LOOKUP, lookup);
            offer(scheduler, reader, connections[1], conflicting);
            assertThat(scheduler.poll(reader)).isNull();

            lookup = new LookupRequest().setTableId(2);
            lookup.addBucketsReq().setBucketId(63).setPartitionId(0);
            FlussRequest otherTable = request(ApiKeys.LOOKUP, lookup);
            FlussRequest unpartitioned = read(63);
            lookup = new LookupRequest().setTableId(1);
            lookup.addBucketsReq().setBucketId(127).setPartitionId(0);
            FlussRequest otherSegment = request(ApiKeys.LOOKUP, lookup);
            offer(scheduler, reader, connections[2], otherTable);
            offer(scheduler, reader, connections[3], unpartitioned);
            offer(scheduler, reader, connections[4], otherSegment);
            for (FlussRequest independent :
                    new FlussRequest[] {otherTable, unpartitioned, otherSegment}) {
                assertThat(scheduler.poll(reader)).isSameAs(independent);
                scheduler.finish(reader);
            }

            scheduler.finish(holder);
            assertThat(scheduler.poll(reader)).isSameAs(conflicting);
            scheduler.finish(reader);
        } finally {
            for (EmbeddedChannel connection : connections) {
                connection.finishAndReleaseAll();
            }
        }
    }

    private static void offer(
            ConnectionHeadScheduler scheduler,
            Owner owner,
            EmbeddedChannel channel,
            FlussRequest request) {
        if (!scheduler.hasReadGrant(owner, channel)) {
            assertThat(
                            scheduler.acquireRead(
                                    owner, channel.pipeline().firstContext(), () -> {}, 40))
                    .isTrue();
        }
        assertThat(scheduler.offer(owner, channel, request)).isTrue();
    }

    private static FlussRequest read(int... buckets) {
        LookupRequest read = new LookupRequest().setTableId(1);
        for (int bucket : buckets) {
            read.addBucketsReq().setBucketId(bucket);
        }
        return request(ApiKeys.LOOKUP, read);
    }

    private static FlussRequest write(int... buckets) {
        PutKvRequest write = new PutKvRequest().setTableId(1);
        for (int bucket : buckets) {
            write.addBucketsReq().setBucketId(bucket);
        }
        return request(ApiKeys.PUT_KV, write);
    }

    private static FlussRequest request(ApiKeys key, ApiMessage message) {
        return new FlussRequest(
                key.id,
                (short) 0,
                0,
                APIS.getApi(key.id),
                message,
                Unpooled.EMPTY_BUFFER,
                "FLUSS",
                false,
                null,
                InetAddress.getLoopbackAddress(),
                new CompletableFuture<>());
    }

    private static final class BudgetConnection implements AutoCloseable {
        private final ConnectionHeadScheduler scheduler;
        private final Owner owner;
        private final EmbeddedChannel channel =
                new EmbeddedChannel(new ChannelInboundHandlerAdapter());
        private boolean resumed;

        private BudgetConnection(ConnectionHeadScheduler scheduler, WorkerGroup group) {
            this.scheduler = scheduler;
            this.owner = scheduler.createOwner(group);
        }

        private boolean requestRead(int bytes) {
            return scheduler.acquireRead(
                    owner, channel.pipeline().firstContext(), () -> resumed = true, bytes);
        }

        private boolean resumed() {
            channel.runPendingTasks();
            assertThat(scheduler.hasReadGrant(owner, channel)).isEqualTo(resumed);
            return resumed;
        }

        @Override
        public void close() {
            scheduler.releaseRead(owner, channel, true);
            channel.finishAndReleaseAll();
        }
    }
}
