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
import org.apache.fluss.shaded.guava32.com.google.common.primitives.Longs;
import org.apache.fluss.utils.IndexTableUtils;

import io.github.fluss_contrib.rocksdb.AbstractCompactionFilter;
import io.github.fluss_contrib.rocksdb.AbstractCompactionFilterFactory;
import io.github.fluss_contrib.rocksdb.FloorSetCompactionFilter;

import javax.annotation.Nullable;

import java.util.function.ToLongFunction;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Filters partitioned Index Table values using the main table's cached partition tombstone.
 *
 * <p>Read filtering consults the current tombstone; each native compaction filter captures one
 * tombstone snapshot. Value tags contain the source partition ID, avoiding row decoding during
 * filtering. Instances are thread-safe and do not own the KV tablet or its native filters.
 */
@Internal
public final class IndexPartitionTombstoneFilter {

    private static final KvValueLayout INDEX_VALUE_LAYOUT = KvValueLayout.TAGGED;

    private final long mainTableId;
    private final int partitionIdPosition;
    private final TabletServerMetadataCache metadataCache;

    private IndexPartitionTombstoneFilter(
            long mainTableId, int partitionIdPosition, TabletServerMetadataCache metadataCache) {
        this.mainTableId = mainTableId;
        this.partitionIdPosition = partitionIdPosition;
        this.metadataCache = metadataCache;
    }

    /**
     * Creates a filter when this table is a partitioned Index Table with a resolvable main table
     * back-link and a {@code __partition_id} system column; returns {@code null} otherwise.
     */
    @Nullable
    public static IndexPartitionTombstoneFilter forIndexTable(
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
        return new IndexPartitionTombstoneFilter(
                tableInfo.getMainTableId().getAsLong(), pidPos, metadataCache);
    }

    /**
     * Returns {@code true} when encoded tagged value bytes contain a tombstoned partition ID. The
     * tag is read without deserializing the row.
     */
    public boolean isTombstoned(byte[] valueBytes) {
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
    public AbstractCompactionFilterFactory<?> createCompactionFilterFactory() {
        return new AbstractCompactionFilterFactory<FloorSetCompactionFilter>() {
            @Override
            public FloorSetCompactionFilter createCompactionFilter(
                    AbstractCompactionFilter.Context context) {
                PartitionTombstone tombstone = currentTombstone();
                return new FloorSetCompactionFilter(
                        INDEX_VALUE_LAYOUT.valueTagOffset(),
                        tombstone.getFloor(),
                        Longs.toArray(tombstone.getExplicitSet()));
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
    public ToLongFunction<BinaryRow> createTagExtractor() {
        final int pos = partitionIdPosition;
        return row -> row.getLong(pos);
    }

    private PartitionTombstone currentTombstone() {
        return metadataCache.getPartitionTombstoneOrEmpty(mainTableId);
    }
}
