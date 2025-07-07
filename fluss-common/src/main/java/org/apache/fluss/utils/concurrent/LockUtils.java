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

package org.apache.fluss.utils.concurrent;

import org.apache.fluss.utils.function.SupplierWithException;
import org.apache.fluss.utils.function.ThrowingRunnable;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/** Utils for {@link Lock}. */
public class LockUtils {
    public static <E extends Exception> void inLock(Lock lock, ThrowingRunnable<E> runnable)
            throws E {
        lock.lock();
        try {
            runnable.run();
        } finally {
            lock.unlock();
        }
    }

    public static <T, E extends Exception> T inLock(Lock lock, SupplierWithException<T, E> action)
            throws E {
        lock.lock();
        try {
            return action.get();
        } finally {
            lock.unlock();
        }
    }

    public static <E extends Exception> void inReadLock(
            ReadWriteLock lock, ThrowingRunnable<E> runnable) throws E {
        inLock(lock.readLock(), runnable);
    }

    public static <T, E extends Exception> T inReadLock(
            ReadWriteLock lock, SupplierWithException<T, E> action) throws E {
        return inLock(lock.readLock(), action);
    }

    public static <E extends Exception> void inWriteLock(
            ReadWriteLock lock, ThrowingRunnable<E> runnable) throws E {
        inLock(lock.writeLock(), runnable);
    }

    public static <T, E extends Exception> T inWriteLock(
            ReadWriteLock lock, SupplierWithException<T, E> action) throws E {
        return inLock(lock.writeLock(), action);
    }
}
