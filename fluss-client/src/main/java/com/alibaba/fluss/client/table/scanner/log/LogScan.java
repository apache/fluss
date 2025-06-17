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

package com.alibaba.fluss.client.table.scanner.log;

import com.alibaba.fluss.annotation.PublicEvolving;

import javax.annotation.Nullable;

/**
 * Used to describe the operation to scan log data by {@link LogScanner} to a table.
 *
 * @since 0.1
 */
@PublicEvolving
public class LogScan {

    /** The projected fields to do projection. No projection if is null. */
    @Nullable private final int[] projectedFields;

    public LogScan() {
        this(null);
    }

    private LogScan(@Nullable int[] projectedFields) {
        this.projectedFields = projectedFields;
    }

    /**
     * Returns a new instance of LogScan description with column projection.
     *
     * @param projectedFields the projection fields
     */
    public LogScan withProjectedFields(int[] projectedFields) {
        return new LogScan(projectedFields);
    }

    @Nullable
    public int[] getProjectedFields() {
        return projectedFields;
    }
}
