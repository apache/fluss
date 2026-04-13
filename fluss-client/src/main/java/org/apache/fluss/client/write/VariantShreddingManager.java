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
import org.apache.fluss.types.variant.ShreddingSchema;
import org.apache.fluss.types.variant.ShreddingSchemaInferrer;
import org.apache.fluss.types.variant.Variant;
import org.apache.fluss.types.variant.VariantStatisticsCollector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Manages automatic Variant shredding inference for a single table on the client write path.
 *
 * <p>Uses the Writer-independent-decision approach (Plan A): each Writer independently samples,
 * infers, and decides the typed_value layout for its own batches. No server-side coordination or
 * RPC is involved.
 *
 * <p>Each time a row is appended to a write batch ({@link #collectRow(InternalRow)}), this manager
 * extracts the Variant values from the row's Variant-typed columns and feeds them into per-column
 * {@link VariantStatisticsCollector}s. Once the minimum sample threshold is met and a non-empty
 * {@link ShreddingSchema} is inferred, the result is stored locally and made available via {@link
 * #getShreddingSchemas()}.
 *
 * <p>Inference is triggered <em>at most once</em> per manager instance. Once schemas are inferred,
 * subsequent calls to {@link #collectRow} are no-ops.
 *
 * <p>Thread safety: {@link #collectRow} is called from the writer thread and is guarded by the
 * deque lock in {@link RecordAccumulator}.
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

    /** Whether inference has already been completed. */
    private volatile boolean inferenceCompleted = false;

    /**
     * Locally inferred shredding schemas. Key is the variant column name. Empty until inference
     * completes with non-empty results.
     */
    private volatile Map<String, ShreddingSchema> inferredSchemas = Collections.emptyMap();

    public VariantShreddingManager(
            TablePath tablePath,
            int[] variantColumnIndices,
            String[] variantColumnNames,
            ShreddingSchemaInferrer inferrer) {
        this.tablePath = tablePath;
        this.variantColumnIndices = variantColumnIndices;
        this.variantColumnNames = variantColumnNames;
        this.inferrer = inferrer;

        this.collectors = new VariantStatisticsCollector[variantColumnIndices.length];
        for (int i = 0; i < variantColumnIndices.length; i++) {
            this.collectors[i] = new VariantStatisticsCollector();
        }
    }

    /**
     * Collects statistics from one row that is about to be (or has just been) written.
     *
     * <p>This method extracts the Variant value at each variant-column index from {@code row} and
     * feeds it into the corresponding {@link VariantStatisticsCollector}. Once enough samples are
     * collected and the inferrer produces a non-empty schema, the result is stored locally.
     *
     * @param row the row being written
     */
    public void collectRow(InternalRow row) {
        if (inferenceCompleted) {
            return;
        }

        for (int c = 0; c < variantColumnIndices.length; c++) {
            int colIdx = variantColumnIndices[c];
            Variant variant = row.isNullAt(colIdx) ? null : row.getVariant(colIdx);
            collectors[c].collect(variant);
        }

        maybeInfer();
    }

    /**
     * Returns the locally inferred shredding schemas. Returns an empty map if inference has not yet
     * completed or produced no results.
     *
     * @return unmodifiable map of variant column name to its ShreddingSchema
     */
    public Map<String, ShreddingSchema> getShreddingSchemas() {
        return inferredSchemas;
    }

    // --------------------------------------------------------------------------------------------
    // Internal helpers
    // --------------------------------------------------------------------------------------------

    private void maybeInfer() {
        Map<String, ShreddingSchema> schemas = new HashMap<>();

        for (int c = 0; c < variantColumnIndices.length; c++) {
            VariantStatisticsCollector collector = collectors[c];
            long totalRecords = collector.getTotalRecords();

            // Skip inference until we have enough samples to be statistically meaningful.
            if (totalRecords < inferrer.getMinSampleSize()) {
                return;
            }

            ShreddingSchema schema =
                    inferrer.infer(variantColumnNames[c], collector.getStatistics(), totalRecords);
            if (!schema.getFields().isEmpty()) {
                schemas.put(variantColumnNames[c], schema);
            }
        }

        if (!schemas.isEmpty()) {
            inferredSchemas = Collections.unmodifiableMap(schemas);
            inferenceCompleted = true;
            LOG.info(
                    "Inferred Variant shredding schemas for table {} (Writer-local): {}",
                    tablePath,
                    inferredSchemas);
        }
    }
}
