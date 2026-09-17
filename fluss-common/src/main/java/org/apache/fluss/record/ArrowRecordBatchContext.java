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

package org.apache.fluss.record;

import javax.annotation.Nullable;

/** Read context that resolves schemas and projection metadata for serialized Arrow batches. */
interface ArrowRecordBatchContext extends LogRecordBatch.ReadContext {
    /**
     * Creates an owned IPC batch from the payload and Fluss log metadata. Takes ownership of the
     * supplied arrays; the caller must not modify them after this method returns.
     */
    ArrowIpcBatch createArrowIpcBatch(
            byte[] recordBatch,
            long baseLogOffset,
            long timestamp,
            int schemaId,
            int recordCount,
            @Nullable byte[] changeTypes);
}
