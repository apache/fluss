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

package org.apache.fluss.server.utils;

import org.apache.fluss.annotation.Internal;

import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Cooperative cancellation with callbacks whose removal waits for an entered callback to finish.
 * Callbacks must only set a native cancellation flag or wake a Java waiter; they must not wait for
 * work to exit or perform resource cleanup.
 */
@Internal
@ThreadSafe
public class CancellationToken {
    private final List<Registration> registrations = new ArrayList<>();
    private volatile boolean cancelled;

    /** Signals cancellation once and notifies all registered callbacks. */
    public synchronized void cancel() {
        if (cancelled) {
            return;
        }
        cancelled = true;
        RuntimeException failure = null;
        for (Registration registration : new ArrayList<>(registrations)) {
            try {
                registration.signal();
            } catch (RuntimeException e) {
                if (failure == null) {
                    failure = e;
                } else if (failure != e) {
                    failure.addSuppressed(e);
                }
            }
        }
        registrations.clear();
        if (failure != null) {
            throw failure;
        }
    }

    /** Returns whether cancellation has been requested. */
    public boolean isCancelled() {
        return cancelled;
    }

    /** Fails if cancellation has been requested. */
    public void checkCancelled() throws IOException {
        if (cancelled) {
            throw new IOException("Operation cancelled.");
        }
    }

    /**
     * Registers a short callback, invoking it immediately if already cancelled. Closing the
     * returned registration guarantees that no invocation can still access the callback's
     * resources.
     */
    public synchronized AutoCloseable register(Runnable callback) {
        Registration registration = new Registration(checkNotNull(callback));
        if (cancelled) {
            registration.signal();
        } else {
            registrations.add(registration);
        }
        return registration;
    }

    private final class Registration implements AutoCloseable {
        private Runnable callback;

        private Registration(Runnable callback) {
            this.callback = callback;
        }

        private void signal() {
            Runnable action = callback;
            callback = null;
            if (action != null) {
                action.run();
            }
        }

        @Override
        public void close() {
            synchronized (CancellationToken.this) {
                callback = null;
                registrations.remove(this);
            }
        }
    }
}
