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

package org.apache.fluss.config;

import org.apache.fluss.annotation.PublicEvolving;

/** Strategy for reading the full state of a primary key table bucket in a bounded scan. */
@PublicEvolving
public enum KvBatchStrategy {
    /**
     * Merge the latest kv snapshot with the bounded changelog range that follows it. The scan is
     * resumable and reflects a single point in time across all buckets.
     */
    SNAPSHOT_MERGE("snapshot-merge"),

    /**
     * Scan the live kv state on the tablet server. Avoids downloading snapshot files and replaying
     * the changelog, but the scan is not resumable and each bucket is read at the point in time its
     * scanner was opened.
     */
    SERVER_SCAN("server-scan");

    private final String name;

    KvBatchStrategy(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }
}
