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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.exception.NotCoordinatorLeaderException;
import org.apache.fluss.exception.OperationNotAttemptedException;
import org.apache.fluss.metadata.TablePath;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Leader-local admission for modifying database and table metadata.
 *
 * <p>Asynchronous requests move from WAITING to QUEUED (permit reserved), then RUNNING, COMPLETING
 * and DONE. Closing or cancellation ends WAITING immediately. A QUEUED request becomes CANCELLED
 * and releases its permit, but keeps its shared budget until the executor wrapper exits. RUNNING
 * work keeps its original execution/failure semantics and releases resources when it ends. New
 * leaders share only the budget; their permit ownership and waiting queues are independent.
 */
final class DdlAdmission implements AutoCloseable {
    static final int MAX_ASYNC_REQUESTS = 128;
    private final Budget budget;
    private final Map<String, DatabaseState> databases = new HashMap<>();
    private final List<Request<?>> waiting = new ArrayList<>();
    private final Set<Request<?>> requests = new HashSet<>();
    private boolean closed;

    DdlAdmission() {
        this(MAX_ASYNC_REQUESTS);
    }

    DdlAdmission(int maxRequests) {
        this(new Budget(maxRequests));
    }

    private DdlAdmission(Budget budget) {
        this.budget = budget;
    }

    DdlAdmission newLeadership() {
        return new DdlAdmission(budget);
    }

    synchronized Permit acquire(String database, @Nullable String table)
            throws InterruptedException {
        while (!closed && (conflicts(database, table) || queuedConflict(database, table))) {
            wait();
        }
        return tryAcquire(database, table);
    }

    synchronized Permit tryAcquire(String database, @Nullable String table) {
        checkOpen();
        if (conflicts(database, table) || queuedConflict(database, table)) {
            throw new OperationNotAttemptedException(
                    "Another DDL operation is modifying "
                            + (table == null ? database : database + "." + table)
                            + ". Please retry the operation.");
        }
        return allocate(database, table);
    }

    <T> CompletableFuture<T> submit(
            String database,
            @Nullable String table,
            Executor executor,
            Function<Permit, T> action) {
        return withRequest(context -> submit(context, database, table, executor, action));
    }

    <T> CompletableFuture<T> withRequest(Function<RequestContext, CompletableFuture<T>> flow) {
        Throwable failure = null;
        synchronized (this) {
            if (closed) {
                failure = notLeader();
            } else if (!budget.reserve()) {
                failure =
                        new OperationNotAttemptedException(
                                "Too many pending DDL operations. Please retry the operation.");
            }
        }
        if (failure != null) {
            CompletableFuture<T> rejected = new CompletableFuture<>();
            rejected.completeExceptionally(failure);
            return rejected;
        }
        RequestContext context = new RequestContext();
        CompletableFuture<T> result;
        try {
            result = flow.apply(context);
        } catch (Throwable t) {
            result = new CompletableFuture<>();
            result.completeExceptionally(t);
        }
        result.whenComplete((ignored, error) -> context.finish());
        return result;
    }

    <T> CompletableFuture<T> submit(
            RequestContext context,
            @Nullable String database,
            @Nullable String table,
            Executor executor,
            Function<Permit, T> action) {
        Request<T> request = new Request<>(context, database, table, executor, action);
        List<Request<?>> ready = Collections.emptyList();
        Throwable rejected = null;
        synchronized (this) {
            if (closed) {
                rejected = notLeader();
            } else if (!context.retain()) {
                rejected = new CancellationException("DDL request has completed");
            } else {
                requests.add(request);
                if (database == null) {
                    request.state = State.QUEUED;
                    ready = Collections.singletonList(request);
                } else {
                    waiting.add(request);
                    ready = drain();
                }
            }
        }
        if (rejected != null) {
            request.result.completeExceptionally(rejected);
        } else {
            request.result.whenComplete(
                    (ignored, error) -> {
                        if (request.result.isCancelled()) {
                            cancel(request);
                        }
                    });
            dispatch(ready);
        }
        return request.result;
    }

    /** Keeps one request's capacity across preparation and subsequent DDL scopes. */
    final class RequestContext {
        private int references = 1;
        private boolean finished;

        private synchronized boolean retain() {
            if (finished) {
                return false;
            }
            references++;
            return true;
        }

        private synchronized void release() {
            if (--references == 0) {
                budget.release();
            }
        }

        private void finish() {
            synchronized (this) {
                if (finished) {
                    return;
                }
                finished = true;
                release();
            }
            List<Request<?>> pending = new ArrayList<>();
            synchronized (DdlAdmission.this) {
                for (Request<?> request : requests) {
                    if (request.context == this
                            && (request.state == State.WAITING || request.state == State.QUEUED)) {
                        pending.add(request);
                    }
                }
            }
            pending.forEach(request -> request.result.cancel(false));
        }
    }

    private void checkOpen() {
        if (closed) {
            throw notLeader();
        }
    }

    private static NotCoordinatorLeaderException notLeader() {
        return new NotCoordinatorLeaderException(
                "The coordinator leadership for this DDL request has ended.");
    }

    private boolean conflicts(String database, @Nullable String table) {
        DatabaseState state = databases.get(database);
        return state != null && (state.exclusive || table == null || state.tables.contains(table));
    }

    private static boolean conflicts(Request<?> first, String database, @Nullable String table) {
        return first.database.equals(database)
                && (first.table == null || table == null || first.table.equals(table));
    }

    private boolean queuedConflict(String database, @Nullable String table) {
        return waiting.stream().anyMatch(request -> conflicts(request, database, table));
    }

    private Permit allocate(String database, @Nullable String table) {
        DatabaseState state = databases.computeIfAbsent(database, ignored -> new DatabaseState());
        if (table == null) {
            state.exclusive = true;
        } else {
            state.tables.add(table);
        }
        return new Permit(database, table, state);
    }

    /** Called under the admission monitor; no callbacks or task execution are allowed here. */
    private List<Request<?>> drain() {
        List<Request<?>> ready = new ArrayList<>();
        List<Request<?>> blocked = new ArrayList<>();
        if (!closed) {
            Iterator<Request<?>> iterator = waiting.iterator();
            while (iterator.hasNext()) {
                Request<?> request = iterator.next();
                if (conflicts(request.database, request.table)
                        || blocked.stream()
                                .anyMatch(
                                        older ->
                                                conflicts(
                                                        older, request.database, request.table))) {
                    blocked.add(request);
                } else {
                    iterator.remove();
                    request.permit = allocate(request.database, request.table);
                    request.state = State.QUEUED;
                    ready.add(request);
                }
            }
        }
        notifyAll();
        return ready;
    }

    private void dispatch(List<Request<?>> ready) {
        for (Request<?> request : ready) {
            try {
                request.executor.execute(request);
            } catch (RuntimeException failure) {
                request.failSubmission(failure);
            }
        }
    }

    private void cancel(Request<?> request) {
        List<Request<?>> ready;
        synchronized (this) {
            if (request.state == State.WAITING) {
                waiting.remove(request);
                request.state = State.DONE;
                retire(request);
            } else if (request.state == State.QUEUED) {
                request.state = State.CANCELLED;
                release(request.permit);
            }
            ready = drain();
        }
        dispatch(ready);
    }

    /** Stops admission without waiting for I/O already in progress. */
    @Override
    public void close() {
        List<Request<?>> rejected = new ArrayList<>();
        synchronized (this) {
            closed = true;
            for (Request<?> request : new ArrayList<>(requests)) {
                if (request.state == State.WAITING) {
                    waiting.remove(request);
                    request.state = State.DONE;
                    retire(request);
                    rejected.add(request);
                } else if (request.state == State.QUEUED) {
                    request.state = State.CANCELLED;
                    release(request.permit);
                    rejected.add(request);
                }
            }
            notifyAll();
        }
        rejected.forEach(request -> request.result.completeExceptionally(notLeader()));
    }

    private void retire(Request<?> request) {
        if (requests.remove(request)) {
            request.state = State.DONE;
            request.context.release();
        }
    }

    private void release(@Nullable Permit permit) {
        if (permit == null || permit.closed) {
            return;
        }
        permit.closed = true;
        if (permit.table == null) {
            permit.owner.exclusive = false;
        } else {
            permit.owner.tables.remove(permit.table);
        }
        if (!permit.owner.exclusive && permit.owner.tables.isEmpty()) {
            databases.remove(permit.database, permit.owner);
        }
    }

    private enum State {
        WAITING,
        QUEUED,
        RUNNING,
        CANCELLED,
        COMPLETING,
        DONE
    }

    private final class Request<T> implements Runnable {
        private final RequestContext context;
        @Nullable private final String database;
        @Nullable private final String table;
        private final Executor executor;
        private final Function<Permit, T> action;
        private final CompletableFuture<T> result = new CompletableFuture<>();
        private State state = State.WAITING;
        @Nullable private Permit permit;

        private Request(
                RequestContext context,
                @Nullable String database,
                @Nullable String table,
                Executor executor,
                Function<Permit, T> action) {
            this.context = context;
            this.database = database;
            this.table = table;
            this.executor = executor;
            this.action = action;
        }

        @Override
        public void run() {
            boolean execute;
            synchronized (DdlAdmission.this) {
                if (state == State.RUNNING || state == State.COMPLETING || state == State.DONE) {
                    return;
                }
                execute = state == State.QUEUED && !closed;
                if (execute) {
                    state = State.RUNNING;
                }
            }
            T value = null;
            Throwable failure = null;
            try {
                if (execute) {
                    value = action.apply(permit);
                }
            } catch (Throwable t) {
                failure = t;
            }
            try {
                releaseScope();
                if (execute) {
                    if (failure == null) {
                        result.complete(value);
                    } else {
                        result.completeExceptionally(failure);
                    }
                }
            } finally {
                synchronized (DdlAdmission.this) {
                    retire(this);
                }
            }
        }

        private void releaseScope() {
            List<Request<?>> ready;
            synchronized (DdlAdmission.this) {
                release(permit);
                state = State.COMPLETING;
                ready = drain();
            }
            dispatch(ready);
        }

        private void failSubmission(RuntimeException failure) {
            synchronized (DdlAdmission.this) {
                if (state == State.RUNNING || state == State.COMPLETING || state == State.DONE) {
                    return;
                }
            }
            try {
                releaseScope();
                result.completeExceptionally(failure);
            } finally {
                synchronized (DdlAdmission.this) {
                    retire(this);
                }
            }
        }
    }

    private static final class DatabaseState {
        private boolean exclusive;
        private final Set<String> tables = new HashSet<>();
    }

    /** Shared across leader views; queued cancelled tasks still own their capacity. */
    private static final class Budget {
        private final int limit;
        private final AtomicInteger retained = new AtomicInteger();

        private Budget(int limit) {
            checkArgument(limit > 0, "DDL request limit must be positive");
            this.limit = limit;
        }

        private boolean reserve() {
            int current;
            do {
                current = retained.get();
                if (current >= limit) {
                    return false;
                }
            } while (!retained.compareAndSet(current, current + 1));
            return true;
        }

        private void release() {
            retained.decrementAndGet();
        }
    }

    final class Permit implements AutoCloseable {
        private final String database;
        @Nullable private final String table;
        private final DatabaseState owner;
        private boolean closed;

        private Permit(String database, @Nullable String table, DatabaseState owner) {
            this.database = database;
            this.table = table;
            this.owner = owner;
        }

        void checkDatabase(String databaseName) {
            if (closed || !database.equals(databaseName) || table != null) {
                throw new IllegalStateException(
                        "DDL operation does not own database " + databaseName);
            }
        }

        void checkTable(TablePath tablePath) {
            if (closed
                    || !database.equals(tablePath.getDatabaseName())
                    || (table != null && !table.equals(tablePath.getTableName()))) {
                throw new IllegalStateException("DDL operation does not own table " + tablePath);
            }
        }

        @Override
        public void close() {
            List<Request<?>> ready;
            synchronized (DdlAdmission.this) {
                release(this);
                ready = drain();
            }
            dispatch(ready);
        }
    }
}
