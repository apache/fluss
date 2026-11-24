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

package org.apache.fluss.server.index;

import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.ToIntFunction;

import static org.apache.fluss.testutils.DataTestUtils.compactedRow;

/** Small index entries with a deterministic destination for source and scheduling tests. */
final class TestingIndexEncoders {
    private TestingIndexEncoders() {}

    static IndexReplicationEncoder encoder(String name, long tableId) {
        return encoder(name, tableId, IndexVisibility.SYNC);
    }

    static IndexReplicationEncoder encoder(String name, long tableId, IndexVisibility visibility) {
        return encoder(name, tableId, visibility, row -> 0);
    }

    static IndexReplicationEncoder fanout(String name, long tableId) {
        return encoder(name, tableId, IndexVisibility.SYNC, row -> row.getInt(0));
    }

    static IndexReplicationEncoder countingEncoder(
            String name, long tableId, IndexVisibility visibility, AtomicInteger encodes) {
        return encoder(
                name,
                tableId,
                visibility,
                row -> {
                    encodes.incrementAndGet();
                    return 0;
                });
    }

    private static IndexReplicationEncoder encoder(
            String name,
            long tableId,
            IndexVisibility visibility,
            ToIntFunction<InternalRow> bucket) {
        CompactedRow value = compactedRow(RowType.of(DataTypes.TINYINT()), new Object[] {(byte) 1});
        return encoder(
                name,
                tableId,
                visibility,
                row ->
                        new IndexReplicationEncoder.IndexEntry(
                                name.getBytes(), value, bucket.applyAsInt(row)),
                (target, end) ->
                        new IndexReplicationEncoder.IndexEntry(
                                (name + "-progress").getBytes(), value, target));
    }

    static IndexReplicationEncoder encoder(
            String name,
            long tableId,
            IndexVisibility visibility,
            IndexReplicationEncoder.DataEntryEncoder data,
            IndexReplicationEncoder.ProgressEncoder progress) {
        return new IndexReplicationEncoder(
                name, visibility, tableId, 1, KvFormat.COMPACTED, new int[] {0}, data, progress);
    }
}
