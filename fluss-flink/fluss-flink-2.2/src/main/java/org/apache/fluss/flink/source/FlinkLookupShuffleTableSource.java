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

import org.apache.fluss.client.lookup.LookupType;
import org.apache.fluss.flink.FlinkConnectorOptions;
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

/**
 * A {@link FlinkTableSource} variant for Flink 2.x that implements {@link
 * SupportsLookupCustomShuffle}, shuffling the lookup-join probe stream by Fluss bucket key so that
 * rows of the same bucket are co-located on the same lookup subtask (better cache locality and
 * lower RPC fan-out).
 */
public class FlinkLookupShuffleTableSource extends FlinkTableSource
        implements SupportsLookupCustomShuffle {

    public FlinkLookupShuffleTableSource(FlinkTableSource base) {
        super(base);
    }

    @Override
    public DynamicTableSource copy() {
        // Copy once via the copy constructor; this preserves the subtype (and thus the ability).
        return new FlinkLookupShuffleTableSource(this);
    }

    @Override
    public Optional<InputDataPartitioner> getPartitioner() {
        LookupNormalizer normalizer = lastLookupNormalizer();
        int[] bucketKeyIndexes = bucketKeyIndexes();
        // Not ready or no bucket key -> keep Flink's default distribution.
        if (normalizer == null || bucketKeyIndexes.length == 0) {
            return Optional.empty();
        }
        // Only full primary-key lookups are shuffled here. A prefix lookup's normalized key does
        // contain all bucket-key fields, so it could be shuffled too; supporting it would mean
        // keying off the normalizer's expected-key row type instead of the full PK projection.
        // TODO: extend custom shuffle to prefix lookups on the bucket key.
        if (normalizer.getLookupType() != LookupType.LOOKUP) {
            return Optional.empty();
        }

        // Number of buckets of the Fluss table. Catalog tables always have this injected; a
        // temporary table created without an explicit (or with a malformed) 'bucket.num' silently
        // keeps Flink's default distribution.
        String bucketNumStr = tableOptions().get(FlinkConnectorOptions.BUCKET_NUMBER.key());
        if (bucketNumStr == null) {
            return Optional.empty();
        }
        final int numBuckets;
        try {
            numBuckets = Integer.parseInt(bucketNumStr);
        } catch (NumberFormatException e) {
            return Optional.empty();
        }

        // The normalized lookup key row is the primary key in Fluss key order.
        RowType keyFlinkRowType = FlinkUtils.projectRowType(tableOutputType(), primaryKeyIndexes());

        // bucket key field names (bucket key is a subset of the primary key)
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
