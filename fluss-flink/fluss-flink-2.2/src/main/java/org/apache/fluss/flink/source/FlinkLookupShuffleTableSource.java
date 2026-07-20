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

package org.apache.fluss.flink.source;

import org.apache.fluss.flink.source.lookup.FlussLookupInputPartitioner;
import org.apache.fluss.flink.source.lookup.LookupNormalizer;
import org.apache.fluss.flink.utils.FlinkUtils;
import org.apache.fluss.metadata.DataLakeFormat;

import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsLookupCustomShuffle;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * A {@link FlinkTableSource} variant for Flink 2.x that implements {@link
 * SupportsLookupCustomShuffle}, shuffling the lookup-join probe stream by Fluss bucket so that rows
 * of the same bucket are co-located on the same lookup subtask (better cache locality and lower RPC
 * fan-out).
 *
 * <p>This subtype is only created (by {@code LookupShuffleSourceAdapter}) when the table exposes
 * the metadata required to reproduce Fluss bucket routing: a non-empty bucket key and a positive
 * {@code bucket.num}. When that metadata is incomplete the source is left unwrapped so Flink falls
 * back to its default (hash) lookup shuffle; when it is present but illegal the adapter fails fast.
 * As a result the invariant of this class is that {@link #getPartitioner()} always returns a
 * partitioner ({@link Optional#empty()} would suppress the shuffle entirely rather than fall back
 * to a hash shuffle, because the planner stops inserting its own shuffle once this ability is
 * advertised).
 */
public class FlinkLookupShuffleTableSource extends FlinkTableSource
        implements SupportsLookupCustomShuffle {

    /** Number of buckets of the Fluss table; validated positive by the wrapping adapter. */
    private final int numBuckets;

    /**
     * Creates a table source with the Flink 2.x custom lookup shuffle ability.
     *
     * @param base base Fluss table source
     * @param numBuckets positive number of buckets in the Fluss table
     */
    public FlinkLookupShuffleTableSource(FlinkTableSource base, int numBuckets) {
        super(base);
        checkArgument(numBuckets > 0, "numBuckets must be positive, but was %s.", numBuckets);
        this.numBuckets = numBuckets;
    }

    @Override
    public DynamicTableSource copy() {
        // Copy once via the copy constructor; this preserves the subtype (and thus the ability) and
        // the stashed lookup normalizer carried over by the base copy constructor.
        return new FlinkLookupShuffleTableSource(this, numBuckets);
    }

    @Override
    public Optional<InputDataPartitioner> getPartitioner() {
        LookupNormalizer normalizer = lastLookupNormalizer();
        // The planner always calls getLookupRuntimeProvider() (which stashes the normalizer) before
        // getPartitioner(); a null normalizer would mean the call order this source relies on was
        // violated. Likewise this source is only ever created for a table with a bucket key. Both
        // are internal invariants: surface them instead of silently returning empty (which the
        // planner would treat as "keep the arbitrary distribution", i.e. no shuffle at all).
        checkState(
                normalizer != null,
                "getPartitioner() was invoked before getLookupRuntimeProvider(); "
                        + "the Fluss custom lookup shuffle cannot determine the lookup keys.");
        int[] bucketKeyIndexes = bucketKeyIndexes();
        checkState(
                bucketKeyIndexes.length > 0,
                "Fluss custom lookup shuffle was enabled for a table without a bucket key.");

        // The normalized lookup key row is the expected lookup keys in Fluss key order: the full
        // primary key for a primary-key lookup, or the bucket keys + partition keys for a prefix
        // lookup on the bucket key. Both are shuffled by the same bucket-key routing, so build the
        // key row type from the normalizer's expected-key indexes (this is what makes prefix
        // lookups shuffle rather than being excluded).
        RowType keyFlinkRowType =
                FlinkUtils.projectRowType(tableOutputType(), normalizer.getLookupKeyIndexes());

        List<String> allNames = tableOutputType().getFieldNames();
        List<String> bucketKeyNames = new ArrayList<>(bucketKeyIndexes.length);
        for (int idx : bucketKeyIndexes) {
            bucketKeyNames.add(allNames.get(idx));
        }

        DataLakeFormat lakeFormat = tableConfigInternal().getDataLakeFormat().orElse(null);

        return Optional.of(
                new FlussLookupInputPartitioner(
                        normalizer, keyFlinkRowType, bucketKeyNames, lakeFormat, numBuckets));
    }
}
