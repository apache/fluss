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

package org.apache.fluss.server.coordinator.remote;

import org.apache.fluss.fs.FsPath;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Interface for selecting remote data directories from a list of available directories.
 *
 * <p>This interface is used to implement different selection strategies for choosing remote data
 * directories when creating tables or partitions. The selection strategy can be configured via
 * {@link org.apache.fluss.config.ConfigOptions#REMOTE_DATA_DIRS_STRATEGY}.
 *
 * <p>Implementations of this interface should be thread-safe as they may be accessed concurrently
 * from multiple threads.
 *
 * @see RoundRobinRemoteDirSelector
 * @see WeightedRoundRobinRemoteDirSelector
 */
@ThreadSafe
public interface RemoteDirSelector {

    /**
     * Returns the next remote data directory path to use.
     *
     * <p>This method should implement the selection strategy (e.g., round-robin, weighted
     * round-robin) to choose from the available remote data directories.
     *
     * @return the next remote data directory path to use. If {@link
     *     org.apache.fluss.config.ConfigOptions#REMOTE_DATA_DIRS} is empty, should always return
     *     {@link org.apache.fluss.config.ConfigOptions#REMOTE_DATA_DIR}.
     */
    FsPath nextDataDir();
}
