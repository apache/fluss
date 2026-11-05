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

import org.apache.fluss.exception.OperationNotAttemptedException;
import org.apache.fluss.metadata.TablePath;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Leader-local admission for modifying database and table metadata. */
final class DdlAdmission {
    private final Map<String, DatabaseState> databases = new HashMap<>();

    synchronized Permit acquire(String database, @Nullable String table)
            throws InterruptedException {
        while (conflicts(database, table)) {
            wait();
        }
        return tryAcquire(database, table);
    }

    synchronized Permit tryAcquire(String database, @Nullable String table) {
        DatabaseState state = databases.get(database);
        if (conflicts(database, table)) {
            throw new OperationNotAttemptedException(
                    "Another DDL operation is modifying "
                            + (table == null ? database : database + "." + table)
                            + ". Please retry the operation.");
        }
        if (state == null) {
            state = new DatabaseState();
            databases.put(database, state);
        }
        if (table == null) {
            state.exclusive = true;
        } else {
            state.tables.add(table);
        }
        return new Permit(database, table, state);
    }

    private boolean conflicts(String database, @Nullable String table) {
        DatabaseState state = databases.get(database);
        return state != null && (state.exclusive || table == null || state.tables.contains(table));
    }

    private static final class DatabaseState {
        private boolean exclusive;
        private final Set<String> tables = new HashSet<>();
    }

    final class Permit implements AutoCloseable {
        private final String database;
        @Nullable private final String table;
        private final DatabaseState state;
        private boolean closed;

        private Permit(String database, @Nullable String table, DatabaseState state) {
            this.database = database;
            this.table = table;
            this.state = state;
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
            synchronized (DdlAdmission.this) {
                if (closed) {
                    return;
                }
                closed = true;
                if (table == null) {
                    state.exclusive = false;
                } else {
                    state.tables.remove(table);
                }
                if (!state.exclusive && state.tables.isEmpty()) {
                    databases.remove(database, state);
                }
                DdlAdmission.this.notifyAll();
            }
        }
    }
}
