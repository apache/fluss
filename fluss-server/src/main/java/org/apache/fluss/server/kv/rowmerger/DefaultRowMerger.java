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
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.server.kv.partialupdate.PartialUpdater;
import org.apache.fluss.server.kv.partialupdate.PartialUpdaterCache;

import javax.annotation.Nullable;

/**
 * The default row merger of primary key table that always retains the latest row and supports
 * configure target merge columns to do partial update.
 */
public class DefaultRowMerger implements RowMerger {

    private final PartialUpdaterCache partialUpdaterCache;
    private final KvFormat kvFormat;
    private final DeleteBehavior deleteBehavior;

    public DefaultRowMerger(KvFormat kvFormat, @Nullable DeleteBehavior deleteBehavior) {
        this.kvFormat = kvFormat;
        // for compatibility, default to ALLOW if not specified
        this.deleteBehavior = deleteBehavior != null ? deleteBehavior : DeleteBehavior.ALLOW;
        // TODO: share cache in server level when PartialUpdater is thread-safe
        this.partialUpdaterCache = new PartialUpdaterCache();
    }

    @Nullable
    @Override
    public BinaryValue merge(BinaryValue oldValue, BinaryValue newValue) {
        // always retain the new row (latest row)
        return newValue;
    }

    @Nullable
    @Override
    public BinaryValue delete(BinaryValue oldRow) {
        // returns null to indicate the row is deleted
        return null;
    }

    @Override
    public DeleteBehavior deleteBehavior() {
        return deleteBehavior;
    }

    @Override
    public RowMerger configureTargetColumns(
            @Nullable int[] targetColumns, short latestShemaId, Schema latestSchema) {
        if (targetColumns == null) {
            return this;
        } else {
            // this also sanity checks the validity of the partial update
            PartialUpdater partialUpdater =
                    partialUpdaterCache.getOrCreatePartialUpdater(
                            kvFormat, latestShemaId, latestSchema, targetColumns);
            return new PartialUpdateRowMerger(partialUpdater, deleteBehavior);
        }
    }

    /** A merger that partially updates specified columns with the new row. */
    private static class PartialUpdateRowMerger implements RowMerger {

        private final PartialUpdater partialUpdater;
        private final DeleteBehavior deleteBehavior;

        public PartialUpdateRowMerger(
                PartialUpdater partialUpdater, DeleteBehavior deleteBehavior) {
            this.partialUpdater = partialUpdater;
            this.deleteBehavior = deleteBehavior;
        }

        @Override
        public RowMerger configureTargetColumns(
                int[] targetColumns, short schemaId, Schema schema) {
            throw new IllegalStateException(
                    "PartialUpdateRowMerger does not support reconfigure target merge columns.");
        }

        @Nullable
        @Override
        public BinaryValue merge(BinaryValue oldValue, BinaryValue newValue) {
            return partialUpdater.updateRow(oldValue, newValue);
        }

        @Nullable
        @Override
        public BinaryValue delete(BinaryValue oldRow) {
            return partialUpdater.deleteRow(oldRow);
        }

        @Override
        public DeleteBehavior deleteBehavior() {
            return deleteBehavior;
        }
    }
}
