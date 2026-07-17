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

package org.apache.fluss.flink.source.lookup;

import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.flink.row.FlinkAsFlussRow;
import org.apache.fluss.flink.utils.FlinkConversions;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;

import org.apache.flink.table.connector.source.abilities.SupportsLookupCustomShuffle.InputDataPartitioner;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.util.List;

/**
 * Partitions the lookup-join probe stream by Fluss bucket key, consistent with the client-side
 * bucketing used by {@code PrimaryKeyLookuper} (bucket key encoding + {@link BucketingFunction}).
 * Rows mapping to the same Fluss bucket are always routed to the same partition.
 */
public class FlussLookupInputPartitioner implements InputDataPartitioner {

    private static final long serialVersionUID = 1L;

    private final LookupNormalizer normalizer;
    // Flink row type of the normalized lookup key row (primary key in Fluss key order).
    private final RowType keyFlinkRowType;
    private final List<String> bucketKeyNames;
    @Nullable private final DataLakeFormat lakeFormat;
    private final int numBuckets;

    private transient KeyEncoder bucketKeyEncoder;
    private transient BucketingFunction bucketingFunction;
    private transient FlinkAsFlussRow reuseRow;

    public FlussLookupInputPartitioner(
            LookupNormalizer normalizer,
            RowType keyFlinkRowType,
            List<String> bucketKeyNames,
            @Nullable DataLakeFormat lakeFormat,
            int numBuckets) {
        this.normalizer = normalizer;
        this.keyFlinkRowType = keyFlinkRowType;
        this.bucketKeyNames = bucketKeyNames;
        this.lakeFormat = lakeFormat;
        this.numBuckets = numBuckets;
    }

    private void ensureInitialized() {
        if (bucketKeyEncoder == null) {
            org.apache.fluss.types.RowType flussKeyType =
                    FlinkConversions.toFlussRowType(keyFlinkRowType);
            // bucketing uses the bucket-key encoder consistent with the client's bucket routing
            bucketKeyEncoder =
                    KeyEncoder.ofBucketKeyEncoder(flussKeyType, bucketKeyNames, lakeFormat);
            bucketingFunction = BucketingFunction.of(lakeFormat);
            reuseRow = new FlinkAsFlussRow();
        }
    }

    @Override
    public int partition(RowData joinKeys, int numPartitions) {
        ensureInitialized();
        // normalize the projected join keys into Fluss primary key order
        RowData normalizedKey = normalizer.normalizeLookupKey(joinKeys);
        InternalRow flussKeyRow = reuseRow.replace(normalizedKey);
        byte[] bucketKeyBytes = bucketKeyEncoder.encodeKey(flussKeyRow);
        int bucketId = bucketingFunction.bucketing(bucketKeyBytes, numBuckets);
        int p = bucketId % numPartitions;
        return p < 0 ? p + numPartitions : p;
    }

    @Override
    public boolean isDeterministic() {
        return true;
    }
}
