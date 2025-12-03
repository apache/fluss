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

package org.apache.fluss.server.kv.rowmerger;

import org.apache.fluss.metadata.DeleteBehavior;
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.record.BinaryValue;

import javax.annotation.Nullable;

/**
 * A merger that deduplicates and keeps first row.
 *
 * @see MergeEngineType#FIRST_ROW
 */
public class FirstRowRowMerger implements RowMerger {

    private final DeleteBehavior deleteBehavior;

    public FirstRowRowMerger(@Nullable DeleteBehavior deleteBehavior) {
        if (deleteBehavior == DeleteBehavior.ALLOW) {
            throw new IllegalArgumentException(
                    "DELETE is not supported for the first_row merge engine.");
        }
        // for compatibility, default to IGNORE if not specified
        this.deleteBehavior = deleteBehavior != null ? deleteBehavior : DeleteBehavior.IGNORE;
    }

    @Nullable
    @Override
    public BinaryValue merge(BinaryValue oldValue, BinaryValue newValue) {
        // always retain the old row (first row)
        return oldValue;
    }

    @Nullable
    @Override
    public BinaryValue delete(BinaryValue oldRow) {
        throw new UnsupportedOperationException(
                "DELETE is not supported for the first_row merge engine.");
    }

    @Override
    public DeleteBehavior deleteBehavior() {
        return deleteBehavior;
    }

    @Override
    public RowMerger configureTargetColumns(
            @Nullable int[] targetColumns, short schemaId, Schema schema) {
        if (targetColumns == null) {
            return this;
        } else {
            throw new UnsupportedOperationException(
                    "Partial update is not supported for the first_row merge engine.");
        }
    }
}
