/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.fluss.rpc.entity;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.protocol.ApiError;

/** Result of {@link org.apache.fluss.rpc.messages.GetTableStatsResponse} for each table bucket. */
public class TableStatsResultForBucket extends ResultForBucket {

    private final long rowCount;

    public TableStatsResultForBucket(TableBucket tableBucket, long rowCount) {
        super(tableBucket);
        this.rowCount = rowCount;
    }

    public TableStatsResultForBucket(TableBucket tableBucket, ApiError error) {
        super(tableBucket, error);
        this.rowCount = -1;
    }

    /** Returns the row count of the table bucket. If the request is failed, it will return -1. */
    public long getRowCount() {
        return rowCount;
    }
}
