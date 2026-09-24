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

package org.apache.fluss.flink.source.lookup;

import org.apache.fluss.client.lookup.LookupType;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.adapter.RuntimeContextAdapter;
import org.apache.fluss.flink.row.FlinkAsFlussRow;
import org.apache.fluss.flink.source.lookup.LookupNormalizer.RemainingFilter;
import org.apache.fluss.flink.utils.FlinkConversions;
import org.apache.fluss.flink.utils.FlinkUtils;
import org.apache.fluss.flink.utils.FlussRowToFlinkRowConverter;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.decode.RowDecoder;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.utils.IOUtils;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.ProjectedRowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Converts Flink lookup keys and results for a continuously maintained local full cache.
 *
 * <p>{@link FullCacheLookupRuntime} owns bootstrap, changelog consumption and the cache lifecycle.
 * Opening the function starts bootstrap in the background; lookups wait for the snapshot to load.
 */
public class FlinkFullCacheLookupFunction extends LookupFunction {

    private static final long serialVersionUID = 1L;

    private final Configuration flussConfig;
    private final TablePath tablePath;
    private final RowType flinkRowType;
    private final LookupNormalizer lookupNormalizer;
    @Nullable private final int[] projection;
    private final int numBuckets;

    private transient FullCacheLookupRuntime cacheRuntime;
    private transient KeyEncoder lookupKeyEncoder;
    private transient RowDecoder rowDecoder;
    private transient FlinkAsFlussRow lookupRow;
    private transient FlussRowToFlinkRowConverter flussRowToFlinkRowConverter;
    private transient RowDataSerializer outputSerializer;
    @Nullable private transient ProjectedRowData projectedRow;

    /**
     * Creates a local full-cache lookup function.
     *
     * @param flussConfig the Fluss client configuration
     * @param tablePath the path of the lookup table
     * @param flinkRowType the row type of the lookup table as planned by Flink
     * @param lookupNormalizer the normalizer created from the lookup join keys
     * @param projection the projection pushed down by Flink, or {@code null} when no field is
     *     pruned
     * @param numBuckets the bucket count of the lookup table the plan was created for
     */
    public FlinkFullCacheLookupFunction(
            Configuration flussConfig,
            TablePath tablePath,
            RowType flinkRowType,
            LookupNormalizer lookupNormalizer,
            @Nullable int[] projection,
            int numBuckets) {
        this.flussConfig = flussConfig;
        this.tablePath = tablePath;
        this.flinkRowType = flinkRowType;
        this.lookupNormalizer = lookupNormalizer;
        this.projection = projection;
        this.numBuckets = numBuckets;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        cacheRuntime = new FullCacheLookupRuntime(flussConfig, tablePath, numBuckets);
        try {
            TableInfo tableInfo =
                    cacheRuntime.open(
                            RuntimeContextAdapter.getIndexOfThisSubtask(context),
                            RuntimeContextAdapter.getNumberOfParallelSubtasks(context),
                            context.getMetricGroup().addGroup("fluss").addGroup("fullCache"));
            org.apache.fluss.types.RowType flussRowType = tableInfo.getRowType();
            org.apache.fluss.types.RowType lookupKeyType =
                    flussRowType.project(lookupNormalizer.getLookupKeyIndexes());
            lookupKeyEncoder =
                    KeyEncoder.ofPrimaryKeyEncoder(
                            lookupKeyType,
                            lookupNormalizer.getLookupType() == LookupType.PREFIX_LOOKUP
                                    ? tableInfo.getBucketKeys()
                                    : tableInfo.getPhysicalPrimaryKeys(),
                            tableInfo.getTableConfig(),
                            tableInfo.isDefaultBucketKey());
            rowDecoder =
                    RowDecoder.create(
                            tableInfo.getTableConfig().getKvFormat(),
                            flussRowType.getChildren().toArray(new DataType[0]));
            lookupRow = new FlinkAsFlussRow();
            RowType outputType =
                    projection == null
                            ? flinkRowType
                            : FlinkUtils.projectRowType(flinkRowType, projection);
            flussRowToFlinkRowConverter =
                    new FlussRowToFlinkRowConverter(FlinkConversions.toFlussRowType(flinkRowType));
            outputSerializer = new RowDataSerializer(outputType);
            projectedRow = projection == null ? null : ProjectedRowData.from(projection);
        } catch (Throwable t) {
            close();
            throw t;
        }
    }

    @Override
    public Collection<RowData> lookup(RowData keyRow) {
        cacheRuntime.awaitReady();
        if (containsNull(keyRow)) {
            return Collections.emptyList();
        }
        try {
            RowData normalizedKey = lookupNormalizer.normalizeLookupKey(keyRow);
            byte[] lookupKey = lookupKeyEncoder.encodeKey(lookupRow.replace(normalizedKey));
            RemainingFilter remainingFilter = lookupNormalizer.createRemainingFilter(keyRow);
            LookupType lookupType = lookupNormalizer.getLookupType();
            if (lookupType != LookupType.LOOKUP && lookupType != LookupType.PREFIX_LOOKUP) {
                throw new IllegalStateException(
                        "Unsupported full-cache lookup type: " + lookupType);
            }
            List<byte[]> values =
                    cacheRuntime.lookup(lookupKey, lookupType == LookupType.PREFIX_LOOKUP);
            List<RowData> results = new ArrayList<>(values.size());
            for (byte[] value : values) {
                RowData outputRow = toOutputRow(value);
                if (remainingFilter == null || remainingFilter.isMatch(outputRow)) {
                    results.add(outputSerializer.copy(outputRow));
                }
            }
            return results;
        } catch (Exception e) {
            throw new RuntimeException("Execution of Fluss full-cache lookup failed.", e);
        }
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(cacheRuntime);
    }

    /** Decodes a cached value into the projected output row. */
    private RowData toOutputRow(byte[] value) {
        RowData fullRow = flussRowToFlinkRowConverter.toFlinkRowData(rowDecoder.decode(value));
        return projectedRow == null ? fullRow : projectedRow.replaceRow(fullRow);
    }

    private static boolean containsNull(RowData row) {
        for (int i = 0; i < row.getArity(); i++) {
            if (row.isNullAt(i)) {
                return true;
            }
        }
        return false;
    }
}
