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

package com.alibaba.fluss.server.replica.delay;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.server.utils.timer.TimerTask;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.alibaba.fluss.utils.concurrent.LockUtils.inLock;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * An operation whose processing needs to be delayed for at most the given delayMs. For example a
 * delayed produce operation could be waiting for specified number of acks;
 *
 * <p>The logic upon completing a delayed operation is defined in onComplete() and will be called
 * exactly once. Once an operation is completed, isCompleted() will return true. onComplete() can be
 * triggered by either forceComplete(), which forces calling onComplete() after delayMs if the
 * operation is not yet completed, or tryComplete(), which first checks if the operation can be
 * completed or not now, and if yes calls forceComplete().
 *
 * <p>A subclass of DelayedOperation needs to provide an implementation of both onComplete() and
 * tryComplete().
 */
public abstract class DelayedOperation extends TimerTask {
    private final AtomicBoolean completed;
    @VisibleForTesting final Lock lock = new ReentrantLock();

    public DelayedOperation(long delayMs) {
        super(delayMs);
        this.completed = new AtomicBoolean(false);
    }

    /**
     * Force completing the delayed operation. This function can be triggered when
     *
     * <pre>
     *     1. The operation has been verified to be completable inside tryComplete()
     *     2. The operation has expired and hence needs to be completed right now
     * </pre>
     *
     * <p>Return true iff the operation is completed by the caller: note that concurrent threads can
     * try to complete the same operation, but only the first thread will succeed in completing the
     * operation and return true, others will still return false
     */
    boolean forceComplete() {
        if (completed.compareAndSet(false, true)) {
            // cancel the timeout timer.
            cancel();
            onComplete();
            return true;
        }
        return false;
    }

    /** Check if the delayed operation is already completed. */
    boolean isCompleted() {
        return completed.get();
    }

    /** Call-back to execute when a delayed operation gets expired and hence forced to complete. */
    public abstract void onExpiration();

    /**
     * Process for completing an operation; This function needs to be defined in subclasses and will
     * be called exactly once in forceComplete().
     */
    public abstract void onComplete();

    /**
     * Try to complete the delayed operation by first checking if the operation can be completed by
     * now. If yes execute the completion logic by calling forceComplete() and return true iff
     * forceComplete returns true; otherwise return false.
     *
     * <p>This function needs to be defined in subclasses.
     */
    public abstract boolean tryComplete();

    /**
     * Thread-safe variant of tryComplete() and call extra function if first tryComplete returns
     * false.
     *
     * @param f else function to be executed after first tryComplete returns false
     * @return result of tryComplete
     */
    public boolean safeTryCompleteOrElse(Runnable f) {
        return inLock(
                lock,
                () -> {
                    if (tryComplete()) {
                        return true;
                    } else {
                        f.run();
                        // last completion check.
                        return tryComplete();
                    }
                });
    }

    /** Thread-safe variant of tryComplete(). */
    public boolean safeTryComplete() {
        return inLock(lock, this::tryComplete);
    }

    @Override
    public void run() {
        if (forceComplete()) {
            onExpiration();
        }
    }
}
