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

import java.io.IOException;

/**
 * Wrapping {@link org.apache.fluss.server.kv.snapshot.CompletedSnapshot} as {@link
 * org.apache.fluss.server.kv.snapshot.CompletedSnapshotHandle} for testing purpose.
 */
public class TestingCompletedSnapshotHandle extends CompletedSnapshotHandle {

    private final CompletedSnapshot snapshot;

    private final boolean shouldFailWhenRetrieve;

    public TestingCompletedSnapshotHandle(CompletedSnapshot snapshot) {
        this(snapshot, false);
    }

    public TestingCompletedSnapshotHandle(
            CompletedSnapshot snapshot, boolean shouldFailWhenRetrieve) {
        super(snapshot.getSnapshotID(), snapshot.getSnapshotLocation(), snapshot.getLogOffset());
        this.snapshot = snapshot;
        this.shouldFailWhenRetrieve = shouldFailWhenRetrieve;
    }

    public CompletedSnapshot retrieveCompleteSnapshot() throws IOException {
        if (shouldFailWhenRetrieve) {
            throw new IOException("Failed to retrieve snapshot: " + snapshot.getSnapshotID());
        }
        return snapshot;
    }
}
