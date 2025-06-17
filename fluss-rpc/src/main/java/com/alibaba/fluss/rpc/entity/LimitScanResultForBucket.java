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

package com.alibaba.fluss.rpc.entity;

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.record.DefaultValueRecordBatch;
import com.alibaba.fluss.record.LogRecords;
import com.alibaba.fluss.rpc.protocol.ApiError;

import javax.annotation.Nullable;

/** Result of {@link com.alibaba.fluss.rpc.messages.LimitScanRequest} for each table bucket. */
public class LimitScanResultForBucket extends ResultForBucket {

    @Nullable private final DefaultValueRecordBatch values;
    @Nullable private final LogRecords records;
    @Nullable private final Boolean isLogTable;

    public LimitScanResultForBucket(TableBucket tableBucket, DefaultValueRecordBatch values) {
        this(tableBucket, ApiError.NONE, values, null, false);
    }

    public LimitScanResultForBucket(TableBucket tableBucket, LogRecords records) {
        this(tableBucket, ApiError.NONE, null, records, true);
    }

    public LimitScanResultForBucket(TableBucket tableBucket, ApiError error) {
        this(tableBucket, error, null, null, null);
    }

    private LimitScanResultForBucket(
            TableBucket tableBucket,
            ApiError error,
            DefaultValueRecordBatch values,
            LogRecords records,
            Boolean isLogTable) {
        super(tableBucket, error);
        this.values = values;
        this.records = records;
        this.isLogTable = isLogTable;
    }

    @Nullable
    public DefaultValueRecordBatch getValues() {
        return values;
    }

    @Nullable
    public LogRecords getRecords() {
        return records;
    }

    @Nullable
    public Boolean isLogTable() {
        return isLogTable;
    }
}
