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

package org.apache.fluss.server.zk.data.bulkload;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.TablePath;

import javax.annotation.Nullable;

/** Durable transaction facts; target and bucket records are stored separately. */
@Internal
public final class BulkLoadTransaction {
    public static final int RUNNING = 0;
    public static final int COMMITTED = 1;
    public static final int ABORTED = 2;

    public final long tableId;
    public final String bulkLoadId;
    public final TablePath tablePath;
    public final int schemaId;
    public final String remoteDataDir;
    public final int state;
    public final long createdTime;
    public final @Nullable Long finishedTime;
    public final @Nullable String abortMessage;

    /** Creates a transaction record without embedding its task set. */
    public BulkLoadTransaction(
            long tableId,
            String bulkLoadId,
            TablePath tablePath,
            int schemaId,
            String remoteDataDir,
            int state,
            long createdTime,
            @Nullable Long finishedTime,
            @Nullable String abortMessage) {
        this.tableId = tableId;
        this.bulkLoadId = bulkLoadId;
        this.tablePath = tablePath;
        this.schemaId = schemaId;
        this.remoteDataDir = remoteDataDir;
        this.state = state;
        this.createdTime = createdTime;
        this.finishedTime = finishedTime;
        this.abortMessage = abortMessage;
    }
}
