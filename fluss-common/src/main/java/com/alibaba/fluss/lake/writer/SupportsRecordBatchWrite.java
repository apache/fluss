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

package com.alibaba.fluss.lake.writer;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.lake.batch.RecordBatch;

import java.io.IOException;

/**
 * The SupportsRecordBatchWrite interface for writing batches of records. It provides a method to
 * write a batch of records to the underlying storage.
 *
 * @since 0.7
 */
@PublicEvolving
public interface SupportsRecordBatchWrite {

    /**
     * Writes a batch of records.
     *
     * @param recordBatch the batch of records to write
     * @throws IOException if an I/O error occurs
     */
    void write(RecordBatch recordBatch) throws IOException;
}
