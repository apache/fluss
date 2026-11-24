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

package org.apache.fluss.server.zk.data.bulkload;

import org.apache.fluss.utils.json.JsonSerdeTestBase;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests the persisted BulkLoadBucket format. */
class BulkLoadBucketJsonSerdeTest extends JsonSerdeTestBase<BulkLoadBucket> {
    BulkLoadBucketJsonSerdeTest() {
        super(BulkLoadBucketJsonSerde.INSTANCE);
    }

    @Override
    protected BulkLoadBucket[] createObjects() {
        return new BulkLoadBucket[] {
            new BulkLoadBucket(7, 100, BulkLoadBucket.ALLOCATED),
            new BulkLoadBucket(9, 120, BulkLoadBucket.COMMITTED)
        };
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"version\":1,\"snapshot_id\":7,\"log_offset\":100,\"state\":0}",
            "{\"version\":1,\"snapshot_id\":9,\"log_offset\":120,\"state\":1}"
        };
    }

    @Override
    protected void assertEquals(BulkLoadBucket actual, BulkLoadBucket expected) {
        assertThat(actual).usingRecursiveComparison().isEqualTo(expected);
    }
}
