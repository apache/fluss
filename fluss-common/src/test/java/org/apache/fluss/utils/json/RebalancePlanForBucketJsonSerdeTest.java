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

package org.apache.fluss.utils.json;

import org.apache.fluss.cluster.rebalance.RebalancePlanForBucket;
import org.apache.fluss.metadata.TableBucket;

import java.util.Arrays;

/** Test for {@link RebalancePlanForBucketJsonSerde}. */
public class RebalancePlanForBucketJsonSerdeTest extends JsonSerdeTestBase<RebalancePlanForBucket> {

    RebalancePlanForBucketJsonSerdeTest() {
        super(RebalancePlanForBucketJsonSerde.INSTANCE);
    }

    @Override
    protected RebalancePlanForBucket[] createObjects() {
        return new RebalancePlanForBucket[] {
            new RebalancePlanForBucket(
                    new TableBucket(0L, 0), 0, 3, Arrays.asList(0, 1, 2), Arrays.asList(3, 4, 5))
        };
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"version\":1,\"table_id\":0,\"bucket\":0,\"original_leader\":0,\"new_leader\":3,\"origin_replicas\":[0,1,2],\"new_replicas\":[3,4,5]}"
        };
    }
}
