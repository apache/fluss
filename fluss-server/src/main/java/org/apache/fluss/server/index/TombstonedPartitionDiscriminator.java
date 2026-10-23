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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.PartitionTombstone;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.encode.KvValueLayout;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.utils.IndexTableUtils;

import io.github.fluss_contrib.rocksdb.AbstractCompactionFilter;
import io.github.fluss_contrib.rocksdb.AbstractCompactionFilterFactory;
import io.github.fluss_contrib.rocksdb.FloorSetCompactionFilter;

import javax.annotation.Nullable;

import java.util.Set;
import java.util.function.ToLongFunction;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Judges whether a partitioned Index Table value row belongs to a tombstoned partition.
 *
 * <p>Named "Discriminator" (not "Predicate") deliberately: this is the <em>judge</em> that reads
 * the tag (partition ID) from the main-branch tagged value layout and consults the cluster-wide
 * {@link PartitionTombstone}. It is not a {@code Predicate<byte[]>} implementation — callers wire
 * one of its methods into the three filter paths (write-time value filter, prefix-lookup filter,
 * RocksDB compaction filter) themselves.
 *
 * <p>Construction is gated by {@link #forIndexTable(TableInfo, TabletServerMetadataCache)}: only
 * partitioned Index Tables that carry the {@code __partition_id} system column and a resolvable
 * main-table back-link yield an instance; all other call sites get {@code null} and skip the
 * tombstone path entirely.
 *
 * <p>Thread-safe: {@link #isTombstoned(byte[])} reads the tag at a fixed offset without any shared
 * mutable state, and the metadata-cache lookup is itself concurrent-safe.
 */
@Internal
final class TombstonedPartitionDiscriminator {

    private static final KvValueLayout INDEX_VALUE_LAYOUT = KvValueLayout.TAGGED;

    private final long mainTableId;
    private final int partitionIdPosition;
    private final TabletServerMetadataCache metadataCache;

    private TombstonedPartitionDiscriminator(
            long mainTableId, int partitionIdPosition, TabletServerMetadataCache metadataCache) {
        this.mainTableId = mainTableId;
        this.partitionIdPosition = partitionIdPosition;
        this.metadataCache = metadataCache;
    }

    /**
     * Builds a discriminator iff this table is a partitioned Index Table with a resolvable main
     * table back-link and a {@code __partition_id} system column; returns {@code null} otherwise.
     */
    @Nullable
    static TombstonedPartitionDiscriminator forIndexTable(
            TableInfo tableInfo, TabletServerMetadataCache metadataCache) {
        if (!tableInfo.isIndexTable()) {
            return null;
        }
        Schema schema = tableInfo.getSchema();
        int pidPos = schema.getColumnNames().indexOf(IndexTableUtils.PARTITION_ID_SYSTEM_COLUMN);
        if (pidPos < 0) {
            return null;
        }
        KvValueLayout valueLayout = KvValueLayout.fromTableConfig(tableInfo.getTableConfig());
        checkArgument(
                valueLayout == KvValueLayout.TAGGED,
                "Partitioned Index Table must use tagged KV value layout, but got version %s",
                valueLayout.version());
        return new TombstonedPartitionDiscriminator(
                tableInfo.getMainTableId().getAsLong(), pidPos, metadataCache);
    }

    /**
     * Returns {@code true} when encoded tagged value bytes contain a tombstoned partition ID. The
     * tag is read without deserializing the row.
     */
    boolean isTombstoned(byte[] valueBytes) {
        if (valueBytes == null || valueBytes.length < INDEX_VALUE_LAYOUT.rowPayloadOffset()) {
            return false;
        }
        long tag = INDEX_VALUE_LAYOUT.readValueTag(MemorySegment.wrap(valueBytes));
        return currentTombstone().isTombstoned(tag);
    }

    /**
     * Wires the compaction-filter factory with a live supplier of the current {@link
     * PartitionTombstone}. The native filter reads the tag at a fixed offset, so no schema metadata
     * is needed during compaction.
     */
    AbstractCompactionFilterFactory<?> createCompactionFilterFactory() {
        return new AbstractCompactionFilterFactory<FloorSetCompactionFilter>() {
            @Override
            public FloorSetCompactionFilter createCompactionFilter(
                    AbstractCompactionFilter.Context context) {
                PartitionTombstone tombstone = currentTombstone();
                return new FloorSetCompactionFilter(
                        INDEX_VALUE_LAYOUT.valueTagOffset(),
                        tombstone.getFloor(),
                        toLongArray(tombstone.getExplicitSet()));
            }

            @Override
            public String name() {
                return "PartitionTombstoneCompactionFilterFactory";
            }
        };
    }

    /**
     * Returns a tag extractor for tagged value encoding. The extractor reads the partitionId from
     * the row at the known position.
     */
    ToLongFunction<BinaryRow> createTagExtractor() {
        final int pos = partitionIdPosition;
        return row -> row.getLong(pos);
    }

    private PartitionTombstone currentTombstone() {
        return metadataCache.getPartitionTombstone(mainTableId);
    }

    private static long[] toLongArray(Set<Long> values) {
        long[] result = new long[values.size()];
        int position = 0;
        for (long value : values) {
            result[position++] = value;
        }
        return result;
    }
}
