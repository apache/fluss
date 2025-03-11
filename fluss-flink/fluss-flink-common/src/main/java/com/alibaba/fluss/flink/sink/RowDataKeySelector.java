/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.sink;

import com.alibaba.fluss.bucketing.FlussBucketingFunction;
import com.alibaba.fluss.flink.row.FlinkAsFlussRow;
import com.alibaba.fluss.row.encode.KeyEncoder;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.table.data.RowData;

/** {@link KeySelector} to get bucket id. */
public class RowDataKeySelector implements KeySelector<RowData, Integer> {

    private final FlussBucketingFunction flussBucketingFunction;
    private final KeyEncoder bucketKeyEncoder;
    private final int numBucket;

    public RowDataKeySelector(KeyEncoder bucketKeyEncoder, int numBucket) {
        this.bucketKeyEncoder = bucketKeyEncoder;
        this.flussBucketingFunction = new FlussBucketingFunction();
        this.numBucket = numBucket;
    }

    @Override
    public Integer getKey(RowData rowData) throws Exception {
        FlinkAsFlussRow row = new FlinkAsFlussRow().replace(rowData);
        return flussBucketingFunction.bucketing(bucketKeyEncoder.encodeKey(row), numBucket);
    }
}
