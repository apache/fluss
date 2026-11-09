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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.memory.MemorySegment;

/** Internal context for loading Arrow record batches with unshaded Arrow resources. */
@Internal
interface ArrowRecordBatchContext extends LogRecordBatch.ReadContext {

    /** Creates a batch-scoped access wrapper for unshaded Arrow resources. */
    UnshadedArrowBatchAccess createUnshadedArrowBatchAccess(int schemaId);

    /** Internal wrapper that hides unshaded Arrow types from shared signatures. */
    @Internal
    interface UnshadedArrowBatchAccess extends AutoCloseable {

        /** Loads one Arrow batch from the given memory segment into the internal read root. */
        void loadArrowBatch(MemorySegment segment, int arrowOffset, int arrowLength);

        /**
         * Applies schema-evolution projection (if needed) internally and builds the final {@link
         * ArrowBatchData}, transferring ownership to the caller.
         */
        ArrowBatchData createArrowBatchData(long baseLogOffset, long timestamp, int schemaId);
    }
}
