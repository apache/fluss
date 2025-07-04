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

package com.alibaba.fluss.server;

import com.alibaba.fluss.server.coordinator.CoordinatorServer;
import com.alibaba.fluss.server.tablet.TabletServer;

/**
 * The server state.
 *
 * <p>For {@link CoordinatorServer}, The expected state transitions are:
 *
 * <p>NOT_RUNNING -> STARTING -> RUNNING -> SHUTTING_DOWN
 *
 * <p>For {@link TabletServer}, The expected state transitions are:
 *
 * <p>NOT_RUNNING -> STARTING -> RECOVERY -> RUNNING -> PENDING_CONTROLLED_SHUTDOWN -> SHUTTING_DOWN
 */
public enum ServerState {
    /** The state the server is in when it first starts up. */
    NOT_RUNNING,

    /** The state the server is in when it is catching up with cluster metadata. */
    STARTING,

    /**
     * The state the TabletServer is in when it is catching up with cluster metadata (like reload
     * log).
     */
    RECOVERY,

    /** The state the server is in when it has registered, and is accepting client requests. */
    RUNNING,

    /** The state the TabletServer is in when it is attempting to perform a controlled shutdown. */
    PENDING_CONTROLLED_SHUTDOWN,

    /** The state the server is in when it is shutting down. */
    SHUTTING_DOWN,

    /** The state the server is in when it is unknown. */
    UNKNOWN
}
