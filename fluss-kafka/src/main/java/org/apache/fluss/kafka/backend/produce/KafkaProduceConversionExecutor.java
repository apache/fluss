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

package org.apache.fluss.kafka.backend.produce;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Dedicated, bounded FIFO lanes for Kafka metadata lookup, conversion, and native submission.
 *
 * <p>The caller uses its RPC processor identity as the ordering key. The server routes each
 * connection to one processor, so its requests remain ordered when moved off that processor. An
 * operation must invoke native append before returning, but need not wait for acknowledgements.
 */
@Internal
public final class KafkaProduceConversionExecutor {
    private final ThreadPoolExecutor[] workers;
    private volatile boolean closed;
    private CompletableFuture<Void> closeFuture;

    /** Creates fixed single-worker lanes whose combined queue capacity is bounded. */
    public KafkaProduceConversionExecutor(int threads, int queueCapacity) {
        checkArgument(threads > 0, "Conversion thread count must be positive.");
        checkArgument(
                queueCapacity >= threads, "Queue capacity must be at least the thread count.");
        workers = new ThreadPoolExecutor[threads];
        for (int i = 0; i < threads; i++) {
            int capacity = queueCapacity / threads + (i < queueCapacity % threads ? 1 : 0);
            workers[i] =
                    new ThreadPoolExecutor(
                            1,
                            1,
                            0L,
                            TimeUnit.MILLISECONDS,
                            new ArrayBlockingQueue<>(capacity),
                            new ExecutorThreadFactory("kafka-produce-conversion-" + i),
                            new ThreadPoolExecutor.AbortPolicy());
        }
    }

    /** Submits ordered work without ever running rejected work on the caller's thread. */
    public <T> CompletableFuture<T> submit(
            long orderingKey, Supplier<CompletableFuture<T>> operation) {
        ConversionTask<T> task = new ConversionTask<>(checkNotNull(operation));
        try {
            if (closed) {
                throw new RejectedExecutionException("Kafka conversion executor is closed.");
            }
            workers[(int) Math.floorMod(orderingKey, (long) workers.length)].execute(task);
        } catch (RejectedExecutionException failure) {
            task.cancel(failure);
        }
        return task.result;
    }

    /**
     * Stops new work, rejects queued requests, interrupts workers, and awaits their termination.
     */
    public synchronized CompletableFuture<Void> closeAsync() {
        if (closeFuture != null) {
            return closeFuture;
        }
        closed = true;
        RejectedExecutionException failure =
                new RejectedExecutionException("Kafka conversion executor is shutting down.");
        for (ThreadPoolExecutor worker : workers) {
            List<Runnable> queued = worker.shutdownNow();
            for (Runnable task : queued) {
                ((ConversionTask<?>) task).cancel(failure);
            }
        }
        closeFuture =
                CompletableFuture.runAsync(
                        () -> {
                            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                            try {
                                for (ThreadPoolExecutor worker : workers) {
                                    if (!worker.awaitTermination(
                                            Math.max(0L, deadline - System.nanoTime()),
                                            TimeUnit.NANOSECONDS)) {
                                        throw new IllegalStateException(
                                                "Kafka conversion worker did not terminate.");
                                    }
                                }
                            } catch (InterruptedException interrupted) {
                                Thread.currentThread().interrupt();
                                throw new IllegalStateException(
                                        "Interrupted closing Kafka conversion workers.",
                                        interrupted);
                            }
                        });
        return closeFuture;
    }

    private static final class ConversionTask<T> implements Runnable {
        private final AtomicReference<Supplier<CompletableFuture<T>>> operation;
        private final CompletableFuture<T> result = new CompletableFuture<>();
        private final AtomicBoolean claimed = new AtomicBoolean();

        private ConversionTask(Supplier<CompletableFuture<T>> operation) {
            this.operation = new AtomicReference<>(operation);
        }

        @Override
        public void run() {
            if (!claimed.compareAndSet(false, true)) {
                return;
            }
            Supplier<CompletableFuture<T>> action = operation.getAndSet(null);
            if (result.isDone()) {
                return;
            }
            try {
                checkNotNull(action.get(), "conversion result")
                        .whenComplete(
                                (value, failure) -> {
                                    if (failure == null) {
                                        result.complete(value);
                                    } else {
                                        result.completeExceptionally(failure);
                                    }
                                });
            } catch (Throwable failure) {
                result.completeExceptionally(failure);
            }
        }

        private void cancel(Throwable failure) {
            if (claimed.compareAndSet(false, true)) {
                operation.set(null);
                result.completeExceptionally(failure);
            }
        }
    }
}
