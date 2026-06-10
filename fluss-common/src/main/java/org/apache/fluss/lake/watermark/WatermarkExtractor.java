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

package org.apache.fluss.lake.watermark;

import org.apache.fluss.lake.batch.RecordBatch;
import org.apache.fluss.row.InternalRow;

import javax.annotation.Nullable;

/**
 * Extracts watermark values from {@link InternalRow} for lake tiering.
 *
 * <p>The extracted value should be expressed in epoch milliseconds and should represent the
 * watermark derived from the row's event-time field and watermark strategy.
 */
public interface WatermarkExtractor {

    /**
     * Extracts the watermark for the given row.
     *
     * @param row the row to extract the watermark from
     * @return the watermark in epoch milliseconds, or {@code null} if the row does not provide a
     *     watermark value
     */
    @Nullable
    Long currentWatermark(InternalRow row);

    /**
     * Extracts the maximum watermark for the given record batch.
     *
     * @param recordBatch the record batch to extract the watermark from
     * @return the maximum watermark in epoch milliseconds, or {@code null} if the batch does not
     *     provide a watermark value
     */
    @Nullable
    Long currentWatermark(RecordBatch recordBatch);
}
