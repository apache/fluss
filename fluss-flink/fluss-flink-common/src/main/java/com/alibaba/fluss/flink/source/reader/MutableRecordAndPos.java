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

package com.alibaba.fluss.flink.source.reader;

import com.alibaba.fluss.client.table.scanner.ScanRecord;

/**
 * A mutable version of the {@link RecordAndPos}.
 *
 * <p>This mutable object is useful in cases where only once instance of a {@code RecordAndPos} is
 * needed at a time.
 */
public class MutableRecordAndPos extends RecordAndPos {

    public MutableRecordAndPos() {
        super(null, NO_READ_RECORDS_COUNT);
    }

    public void setRecord(ScanRecord scanRecord, long readRecordsCount) {
        this.scanRecord = scanRecord;
        this.readRecordsCount = readRecordsCount;
    }
}
