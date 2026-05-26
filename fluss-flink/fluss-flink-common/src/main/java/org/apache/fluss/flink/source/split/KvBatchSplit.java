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

package org.apache.fluss.flink.source.split;

import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

/**
 * A bounded split that reads the full primary-key state of a bucket via the server-side KV scan
 * (FIP-17). Emitted by the enumerator when a primary-key table has no KV snapshot file available
 * for the bucket and the source is running in bounded mode.
 *
 * <p>This split has no resumable position: on Flink task restart the bucket is rescanned from
 * scratch. Snapshot isolation is provided by the server (a consistent point-in-time view of the
 * RocksDB state at the moment {@code ScanKv} opens the scan), but the client cannot resume an
 * expired or invalidated session, so progress is not checkpointed.
 *
 * <p>Unlike {@link HybridSnapshotLogSplit}, this split has no log handoff phase: when the bucket
 * is drained the split is marked finished.
 */
public class KvBatchSplit extends SourceSplitBase {

    private static final String KV_BATCH_SPLIT_PREFIX = "kv-batch-";

    public KvBatchSplit(TableBucket tableBucket, @Nullable String partitionName) {
        super(tableBucket, partitionName);
    }

    @Override
    public String splitId() {
        return toSplitId(KV_BATCH_SPLIT_PREFIX, tableBucket);
    }

    @Override
    protected byte splitKind() {
        return KV_BATCH_SPLIT_FLAG;
    }

    @Override
    public String toString() {
        return "KvBatchSplit{"
                + "tableBucket="
                + tableBucket
                + ", partitionName='"
                + partitionName
                + '\''
                + '}';
    }
}
