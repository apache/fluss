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

package org.apache.fluss.client.write;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.rpc.messages.ApplyShreddingSchemaRequest;
import org.apache.fluss.rpc.messages.ApplyShreddingSchemaResponse;
import org.apache.fluss.rpc.messages.PbTablePath;
import org.apache.fluss.types.variant.ShreddingSchema;
import org.apache.fluss.types.variant.ShreddingSchemaInferrer;
import org.apache.fluss.types.variant.Variant;
import org.apache.fluss.types.variant.VariantStatisticsCollector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * Manages automatic Variant shredding inference for a single table on the client write path.
 *
 * <p>Each time a row is appended to a write batch ({@link #collectRow(InternalRow)}), this manager
 * extracts the Variant values from the row's Variant-typed columns and feeds them into per-column
 * {@link VariantStatisticsCollector}s. Once the minimum sample threshold is met and a non-empty
 * {@link ShreddingSchema} is inferred, an asynchronous RPC is dispatched to the Coordinator to
 * trigger server-side schema evolution.
 *
 * <p>Schema evolution is triggered <em>at most once</em> per manager instance. If the RPC fails
 * (e.g., transient network error), the {@link #schemaTriggered} flag is reset to allow a retry on
 * the next collected row.
 *
 * <p>Thread safety: {@link #collectRow} is called from the writer thread and is guarded by the
 * deque lock in {@link RecordAccumulator}. The {@link #schemaTriggered} flag is an {@link
 * AtomicBoolean} so it can safely be reset from the RPC callback thread.
 */
@Internal
public class VariantShreddingManager {

    private static final Logger LOG = LoggerFactory.getLogger(VariantShreddingManager.class);

    private final TablePath tablePath;

    /**
     * Column indices (into the row's schema) of all Variant-typed columns. Each index maps to the
     * corresponding {@link VariantStatisticsCollector} in {@link #collectors} at the same array
     * position.
     */
    private final int[] variantColumnIndices;

    /**
     * Names of the Variant columns, used to construct the column-name-based {@link
     * ShreddingSchema}.
     */
    private final String[] variantColumnNames;

    /** One statistics collector per Variant column. */
    private final VariantStatisticsCollector[] collectors;

    /** Inferrer, configured from the table's shredding options. */
    private final ShreddingSchemaInferrer inferrer;

    /**
     * Guards against duplicate schema evolution RPCs. Set to {@code true} when an RPC is in flight;
     * reset to {@code false} on RPC failure so the next {@link #collectRow} call can retry.
     */
    private final AtomicBoolean schemaTriggered = new AtomicBoolean(false);

    /**
     * Callback that sends the {@link ApplyShreddingSchemaRequest} to the Coordinator and returns a
     * future. Injected by {@link RecordAccumulator} so this class does not depend on a concrete RPC
     * client.
     */
    private final Function<
                    ApplyShreddingSchemaRequest, CompletableFuture<ApplyShreddingSchemaResponse>>
            rpcCaller;

    public VariantShreddingManager(
            TablePath tablePath,
            int[] variantColumnIndices,
            String[] variantColumnNames,
            ShreddingSchemaInferrer inferrer,
            Function<ApplyShreddingSchemaRequest, CompletableFuture<ApplyShreddingSchemaResponse>>
                    rpcCaller) {
        this.tablePath = tablePath;
        this.variantColumnIndices = variantColumnIndices;
        this.variantColumnNames = variantColumnNames;
        this.inferrer = inferrer;
        this.rpcCaller = rpcCaller;

        this.collectors = new VariantStatisticsCollector[variantColumnIndices.length];
        for (int i = 0; i < variantColumnIndices.length; i++) {
            this.collectors[i] = new VariantStatisticsCollector();
        }
    }

    /**
     * Collects statistics from one row that is about to be (or has just been) written.
     *
     * <p>This method extracts the Variant value at each variant-column index from {@code row} and
     * feeds it into the corresponding {@link VariantStatisticsCollector}. If the inferrer produces
     * a non-empty schema for any column, {@link #triggerSchemaEvolution} is called.
     *
     * @param row the row being written
     */
    public void collectRow(InternalRow row) {
        if (schemaTriggered.get()) {
            return;
        }

        for (int c = 0; c < variantColumnIndices.length; c++) {
            int colIdx = variantColumnIndices[c];
            Variant variant = row.isNullAt(colIdx) ? null : row.getVariant(colIdx);
            collectors[c].collect(variant);
        }

        maybeInferAndTrigger();
    }

    // --------------------------------------------------------------------------------------------
    // Internal helpers
    // --------------------------------------------------------------------------------------------

    private void maybeInferAndTrigger() {
        for (int c = 0; c < variantColumnIndices.length; c++) {
            VariantStatisticsCollector collector = collectors[c];
            long totalRecords = collector.getTotalRecords();

            // Skip inference until we have enough samples to be statistically meaningful.
            // This avoids creating empty ShreddingSchema objects on every row.
            if (totalRecords < inferrer.getMinSampleSize()) {
                continue;
            }

            ShreddingSchema schema =
                    inferrer.infer(variantColumnNames[c], collector.getStatistics(), totalRecords);
            if (!schema.getFields().isEmpty()) {
                triggerSchemaEvolution(schema);
                return;
            }
        }
    }

    private void triggerSchemaEvolution(ShreddingSchema schema) {
        if (!schemaTriggered.compareAndSet(false, true)) {
            return;
        }

        String schemaJson = schema.toJson();
        LOG.info(
                "Triggering Variant shredding schema evolution for table {}: {}",
                tablePath,
                schemaJson);

        ApplyShreddingSchemaRequest request =
                new ApplyShreddingSchemaRequest()
                        .setTablePath(
                                new PbTablePath()
                                        .setDatabaseName(tablePath.getDatabaseName())
                                        .setTableName(tablePath.getTableName()))
                        .setShreddingSchemaJson(schemaJson);

        rpcCaller
                .apply(request)
                .whenComplete(
                        (resp, ex) -> {
                            if (ex != null) {
                                LOG.warn(
                                        "Failed to apply Variant shredding schema for table {}, "
                                                + "will retry on next row: {}",
                                        tablePath,
                                        ex.getMessage());
                                schemaTriggered.set(false);
                            } else {
                                LOG.info(
                                        "Successfully applied Variant shredding schema for table {}",
                                        tablePath);
                            }
                        });
    }
}
