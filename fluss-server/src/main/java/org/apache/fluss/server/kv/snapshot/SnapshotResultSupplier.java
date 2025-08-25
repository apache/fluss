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

package org.apache.fluss.server.kv.snapshot;

import org.apache.fluss.utils.CloseableRegistry;

/**
 * A supplier for a {@link SnapshotResult} with an access to a {@link CloseableRegistry} for io
 * tasks that need to be closed when cancelling the async part of the snapshot.
 */
@FunctionalInterface
public interface SnapshotResultSupplier {

    /**
     * Performs the asynchronous part of one snapshot and returns the snapshot result.
     *
     * @param snapshotCloseableRegistry A registry for io tasks to close on cancel.
     * @return A snapshot result
     */
    SnapshotResult get(CloseableRegistry snapshotCloseableRegistry) throws Exception;
}
