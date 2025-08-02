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

package com.alibaba.fluss.utils.json;

import com.alibaba.fluss.lake.committer.BucketOffset;

/** Test for {@link BucketOffset}. */
public class BucketOffsetJsonSerdeTest extends JsonSerdeTestBase<BucketOffset> {

    BucketOffsetJsonSerdeTest() {
        super(BucketOffsetJsonSerde.INSTANCE);
    }

    @Override
    protected BucketOffset[] createObjects() {
        return new BucketOffset[] {
            new BucketOffset(10, 1, 1L, "eu-central$2023$12"), new BucketOffset(20, 2, null, null)
        };
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"partition_id\":1,\"bucket_id\":1,\"partition_name\":\"eu-central$2023$12\",\"log_offset\":10}",
            "{\"bucket_id\":2,\"log_offset\":20}"
        };
    }
}
