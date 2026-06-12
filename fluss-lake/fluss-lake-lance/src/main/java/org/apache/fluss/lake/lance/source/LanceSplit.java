/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.lance.source;

import org.apache.fluss.lake.source.LakeSplit;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/** Split for Lance table. */
public class LanceSplit implements LakeSplit, Serializable {
    private static final long serialVersionUID = 1L;

    private final int fragmentId;
    private final long snapshotId;
    private final long fragmentRows;
    private final long scanLimit;
    private final int bucket;
    private final List<String> partition;

    public LanceSplit(
            int fragmentId,
            long snapshotId,
            long fragmentRows,
            long scanLimit,
            int bucket,
            List<String> partition) {
        this.fragmentId = fragmentId;
        this.snapshotId = snapshotId;
        this.fragmentRows = fragmentRows;
        this.scanLimit = scanLimit;
        this.bucket = bucket;
        this.partition =
                partition == null ? Collections.<String>emptyList() : Collections.unmodifiableList(partition);
    }

    public int fragmentId() {
        return fragmentId;
    }

    public long snapshotId() {
        return snapshotId;
    }

    public long fragmentRows() {
        return fragmentRows;
    }

    public long scanLimit() {
        return scanLimit;
    }

    @Override
    public int bucket() {
        return bucket;
    }

    @Override
    public List<String> partition() {
        return partition;
    }

    @Override
    public String toString() {
        return "LanceSplit{"
                + "fragmentId="
                + fragmentId
                + ", snapshotId="
                + snapshotId
                + ", fragmentRows="
                + fragmentRows
                + ", scanLimit="
                + scanLimit
                + ", bucket="
                + bucket
                + ", partition="
                + partition
                + '}';
    }
}
