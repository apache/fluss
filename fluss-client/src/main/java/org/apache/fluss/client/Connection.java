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

package org.apache.fluss.client;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A cluster connection encapsulating lower level individual connections to actual Fluss servers.
 * Connections are instantiated through the {@link ConnectionFactory} class. The lifecycle of the
 * connection is managed by the caller, who has to {@link #close()} the connection to release the
 * resources. The connection object contains logic to find the Coordinator, locate table buckets out
 * on the cluster, keeps a cache of locations and then knows how to re-calibrate after they move.
 * The individual connections to servers, meta cache, etc. are all shared by the {@link Table} and
 * {@link Admin} instances obtained from this connection.
 *
 * <p>Connection creation is a heavy-weight operation. Connection implementations are thread-safe,
 * so that the client can create a connection once, and share it with different threads. {@code
 * Table} and {@link Admin} instances, on the other hand, are light-weight and are not thread-safe.
 * Typically, a single connection per client application is instantiated and every thread will
 * obtain its own {@link Table} instance. Caching or pooling of {@link Table} and {@link Admin} is
 * not recommended.
 *
 * @since 0.1
 */
@PublicEvolving
@ThreadSafe
public interface Connection extends AutoCloseable {

    /** Retrieve the configuration used to create this connection. */
    Configuration getConfiguration();

    /** Retrieve a new Admin client to administer a Fluss cluster. */
    Admin getAdmin();

    /** Retrieve a new Table client to operate data in table. */
    Table getTable(TablePath tablePath);

    /**
     * Switch the connection from recovery mode to normal mode.
     *
     * <p>This is a one-way transition that can only be called once. After calling this method, all
     * subsequent writes through this connection will use normal merge engine semantics instead of
     * recovery mode (overwrite mode).
     *
     * <p>This method is typically used in undo recovery scenarios where you need to replay
     * historical state using recovery mode (overwrite mode) and then switch back to normal mode for
     * ongoing operations.
     *
     * <p>IMPORTANT: This method can only be called on connections created with {@link
     * ConnectionFactory#createRecoveryConnection(Configuration)}. Calling it on normal connections
     * has no effect.
     *
     * @throws IllegalStateException if this method is called more than once
     */
    void switchToNormalMode();

    /**
     * Check whether this connection is in recovery mode.
     *
     * <p>When in recovery mode, all write operations bypass the merge engine and write directly to
     * the KV store (overwrite mode). This is used for undo recovery to ensure data consistency
     * during state restoration.
     *
     * @return true if the connection is in recovery mode, false otherwise
     */
    boolean isRecoveryMode();

    /** Close the connection and release all resources. */
    @Override
    void close() throws Exception;
}
