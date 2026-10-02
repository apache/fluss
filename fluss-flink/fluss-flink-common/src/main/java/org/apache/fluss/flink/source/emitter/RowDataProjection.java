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

package org.apache.fluss.flink.source.emitter;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.ProjectedRowData;

import javax.annotation.Nullable;

/** Output projection for {@link RowData} records emitted by {@link FlinkRecordEmitter}. */
public final class RowDataProjection implements FlinkRecordEmitter.OutputProjection<RowData> {
    private static final long serialVersionUID = 1L;

    private final int[] projectedFields;

    private RowDataProjection(int[] projectedFields) {
        this.projectedFields = projectedFields;
    }

    /** Creates a projection, or returns {@code null} when no projection is requested. */
    @Nullable
    public static RowDataProjection of(@Nullable int[] projectedFields) {
        return projectedFields == null ? null : new RowDataProjection(projectedFields);
    }

    @Override
    public RowData project(RowData record) {
        return ProjectedRowData.from(projectedFields).replaceRow(record);
    }
}
