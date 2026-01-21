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

package org.apache.fluss.client.initializer;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.client.table.scanner.log.LogScanner.EARLIEST_OFFSET;

/**
 * A initializer that initialize the buckets to the earliest offsets.
 *
 * <p>Package private and should be instantiated via {@link OffsetsInitializer}.
 */
class EarliestOffsetsInitializer implements OffsetsInitializer {
    private static final long serialVersionUID = 172938052008787981L;

    @Override
    public Map<Integer, Long> getBucketOffsets(
            @Nullable String partitionName,
            Collection<Integer> buckets,
            BucketOffsetsRetriever bucketOffsetsRetriever) {
        Map<Integer, Long> initialOffsets = new HashMap<>();
        for (Integer tb : buckets) {
            initialOffsets.put(tb, EARLIEST_OFFSET);
        }
        return initialOffsets;
    }
}
