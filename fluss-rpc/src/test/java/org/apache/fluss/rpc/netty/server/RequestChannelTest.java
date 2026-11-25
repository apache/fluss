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

import org.apache.fluss.rpc.protocol.RequestType;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

class RequestChannelTest {
    @Test
    void testConnectionOrderSurvivesShutdownDrain() {
        RequestChannel channel = new RequestChannel(10);
        RequestChannel.Connection connection = channel.newConnection(() -> {});
        RpcRequest first = new TestRequest();
        RpcRequest second = new TestRequest();
        submit(channel, connection, first);
        submit(channel, connection, second);
        channel.close();

        assertThat(channel.pollRequest(0)).isSameAs(first);
        channel.requestFinished();
        assertThat(channel.pollRequest(0)).isSameAs(second);
        channel.requestFinished();
        assertThat(channel.pollRequest(0)).isSameAs(ShutdownRequest.INSTANCE);
    }

    @Test
    void testIdleWorkerTakesConnectionAndKeepsItsNextRequests() {
        RequestChannel[] channels = RequestChannel.forGroup(2, 16, 5);
        RequestChannel firstWorker = channels[0];
        RequestChannel secondWorker = channels[1];
        RequestChannel.Connection busy = firstWorker.newConnection(() -> {});
        RequestChannel.Connection transferable = firstWorker.newConnection(() -> {});
        RpcRequest first = new TestRequest();
        RpcRequest second = new TestRequest();
        RpcRequest third = new TestRequest();

        submit(firstWorker, busy, first);
        assertThat(firstWorker.pollRequest(0)).isSameAs(first);
        submit(firstWorker, transferable, second);
        submit(firstWorker, transferable, third);
        assertThat(secondWorker.pollRequest(0)).isSameAs(second);
        assertThat(firstWorker.requestsCount()).isZero();
        assertThat(secondWorker.requestsCount()).isOne();

        firstWorker.requestFinished();
        assertThat(firstWorker.pollRequest(0)).isNull();
        secondWorker.requestFinished();
        assertThat(secondWorker.pollRequest(0)).isSameAs(third);
        secondWorker.requestFinished();

        RpcRequest next = new TestRequest();
        submit(firstWorker, transferable, next);
        assertThat(firstWorker.pollRequest(0)).isNull();
        assertThat(secondWorker.pollRequest(0)).isSameAs(next);
        secondWorker.requestFinished();
    }

    @Test
    void testRunnableHeadWakesAnIdleWorker() throws Exception {
        RequestChannel[] workers = RequestChannel.forGroup(2, 16, 3);
        RequestChannel busyWorker = workers[0];
        RequestChannel waitingWorker = workers[1];
        RequestChannel.Connection busy = busyWorker.newConnection(() -> {});
        RequestChannel.Connection next = busyWorker.newConnection(() -> {});
        RpcRequest first = new TestRequest();
        RpcRequest second = new TestRequest();
        submit(busyWorker, busy, first);
        assertThat(busyWorker.pollRequest(0)).isSameAs(first);
        FutureTask<RpcRequest> waiting = new FutureTask<>(() -> waitingWorker.pollRequest(30_000));
        Thread worker = new Thread(waiting);
        worker.start();
        try {
            waitUntil(
                    () -> worker.getState() == Thread.State.TIMED_WAITING,
                    Duration.ofSeconds(5),
                    "Worker did not wait for a runnable connection");
            submit(busyWorker, next, second);
            assertThat(waiting.get(5, TimeUnit.SECONDS)).isSameAs(second);
            waitingWorker.requestFinished();
        } finally {
            worker.interrupt();
            worker.join();
            busyWorker.requestFinished();
        }
    }

    @Test
    void testDisconnectReturnsReservationToNextGroupWaiter() {
        RequestChannel worker = new RequestChannel(1);
        RequestChannel.Connection leader = worker.newConnection(() -> {});
        FutureTask<Void> firstWakeup = new FutureTask<>(() -> null);
        FutureTask<Void> secondWakeup = new FutureTask<>(() -> null);
        RequestChannel.Connection first = worker.newConnection(firstWakeup);
        RequestChannel.Connection second = worker.newConnection(secondWakeup);
        RequestChannel.Connection arriving = worker.newConnection(() -> {});
        RpcRequest running = new TestRequest();
        submit(worker, leader, running);
        assertThat(worker.acquireRead(first)).isFalse();
        assertThat(worker.acquireRead(second)).isFalse();

        assertThat(worker.pollRequest(0)).isSameAs(running);
        assertThat(firstWakeup.isDone()).isTrue();
        assertThat(secondWakeup.isDone()).isFalse();
        assertThat(worker.acquireRead(arriving)).isFalse();

        worker.closeConnection(first);
        assertThat(secondWakeup.isDone()).isTrue();
        assertThat(worker.acquireRead(second)).isTrue();
        assertThat(worker.acquireRead(arriving)).isFalse();
        worker.closeConnection(second);
        assertThat(worker.acquireRead(arriving)).isTrue();
        worker.returnRead(arriving);
        worker.requestFinished();
    }

    private static void submit(
            RequestChannel worker, RequestChannel.Connection connection, RpcRequest request) {
        assertThat(worker.acquireRead(connection)).isTrue();
        worker.putRequest(connection, request);
    }

    private static final class TestRequest implements RpcRequest {
        @Override
        public RequestType getRequestType() {
            return RequestType.FLUSS;
        }

        @Override
        public void releaseBuffer() {}
    }
}
