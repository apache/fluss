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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.record;

import com.alibaba.fluss.row.decode.RowDecoder;

/**
 * A value record batch is a container for a batch of {@link ValueRecord}.
 *
 * @since 0.3
 */
public interface ValueRecordBatch {

    /** The "magic" values. */
    byte VALUE_BATCH_MAGIC_VALUE_V0 = 0;

    /** The current "magic" value. */
    byte CURRENT_VALUE_BATCH_MAGIC = VALUE_BATCH_MAGIC_VALUE_V0;

    /**
     * Get the size in bytes of this batch, including the size of the record and the batch overhead.
     *
     * @return The size in bytes of this batch
     */
    int sizeInBytes();

    /**
     * Get the record format version of this record batch (i.e its magic value).
     *
     * @return the magic byte
     */
    byte magic();

    /**
     * Get the count.
     *
     * @return The number of records in the batch.
     */
    int getRecordCount();

    /**
     * Get the iterable of {@link ValueRecord} in this batch.
     *
     * @param readContext The context to read records from the record batch
     * @return The iterable of {@link KvRecord} in this batch
     */
    Iterable<ValueRecord> records(ReadContext readContext);

    /** The read context of a {@link ValueRecordBatch} to read records. */
    interface ReadContext {

        /**
         * Gets the row decoder for the given schema to decode bytes read from {@link
         * ValueRecordBatch}.
         *
         * @param schemaId the schema of the kv records
         */
        RowDecoder getRowDecoder(int schemaId);
    }
}
